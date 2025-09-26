import os, tempfile, subprocess, uuid, json, time, requests
from fastapi import FastAPI, Body, Header, HTTPException
from fastapi.responses import PlainTextResponse

# ---------- ENV ----------
CLD_NAME        = os.environ["CLD_NAME"]                 # ex: drfh6p8cn
CLD_PRESET      = os.environ["CLD_UNSIGNED_PRESET"]      # ex: brian_vid_unsigned
WORKER_TOKEN    = os.environ.get("WORKER_TOKEN", "")
MAX_DURATION_S  = int(os.environ.get("MAX_DURATION_SEC", "300"))  # e.g. 300 = 5 min
MAX_BYTES       = int(os.environ.get("MAX_BYTES", "0"))  # optional hard stop by size (0 = ignore)

# yt-dlp politeness/retry tuning to reduce 429s
YTDLP_COMMON = [
    "--no-progress",
    "--retries", "10",
    "--fragment-retries", "10",
    "--retry-sleep", "linear=1:10",
    "--sleep-requests", "1",
    "--concurrent-fragments", "1",
    "--socket-timeout", "20",
    "--user-agent", "Mozilla/5.0 (compatible; IDBVideoWorker/1.0)",
]

app = FastAPI()

# ---------- health/roots ----------
@app.get("/ping")
def ping():
    return {"ok": True}

@app.get("/health")
def health():
    return {"ok": True}

@app.get("/", response_class=PlainTextResponse)
@app.head("/", response_class=PlainTextResponse)
def root():
    return "ok"

# ---------- helpers ----------
def _run(cmd: list[str], check=True, capture=False, text=False, timeout=None):
    return subprocess.run(
        cmd, check=check,
        stdout=(subprocess.PIPE if capture else None),
        stderr=(subprocess.PIPE if capture else None),
        text=text, timeout=timeout
    )

def _preflight_info(url: str, retries: int = 1, backoff_base: float = 1.0) -> dict:
    """
    Use yt-dlp to fetch metadata only (no download). Returns {} on failure/timeout.
    """
    for attempt in range(1, retries + 1):
        try:
            cmd = ["yt-dlp", *YTDLP_COMMON, "-j", "--skip-download", url]
            p = _run(cmd, check=True, capture=True, text=True, timeout=25)
            for line in p.stdout.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    continue
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            pass
        time.sleep(backoff_base * attempt)
    return {}

def _ffprobe_duration(path: str) -> float | None:
    try:
        p = _run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "format=duration", "-of", "json", path],
            check=True, capture=True, text=True, timeout=30
        )
        j = json.loads(p.stdout)
        dur = float(j.get("format", {}).get("duration", "0") or 0)
        return dur if dur > 0 else None
    except Exception:
        return None

# ---- Response builders (new shape + array-of-one) ----
def _one_response(thread_id: str, source_url: str, public_id=None, secure_url=None, thumb_url=None, error_message: str | None = None):
    obj = {
        "thread_id": thread_id,
        "source_url": source_url,
        "public_id": public_id,
        "secure_url": secure_url,
        "thumb_url": thumb_url,
    }
    if error_message:
        obj["error"] = {"message": error_message}
    return [obj]

# ---------- main endpoint ----------
@app.post("/mux-upload")
def mux_upload(payload: dict = Body(...), x_token: str = Header(default="")):
    # Auth
    if WORKER_TOKEN and x_token != WORKER_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")

    # ---------- LOCK IN IDENTIFIERS (only this fix) ----------
    # Canonical ID for DB upsert must be exactly the provided thread_id (or legacy reddit_id).
    thread_id_raw = payload.get("thread_id")
    legacy_id_raw = payload.get("reddit_id")  # legacy back-compat

    if isinstance(thread_id_raw, str) and thread_id_raw.strip():
        thread_id = thread_id_raw.strip()
    elif isinstance(legacy_id_raw, str) and legacy_id_raw.strip():
        thread_id = legacy_id_raw.strip()
    else:
        # Do not fabricate a UUID for thread_id; fail fast so upstream upsert logic is safe.
        raise HTTPException(status_code=400, detail="thread_id ausente")

    # Source URL priority: video_url_clean -> video_url -> vredd_url (legacy)
    source_url = (payload.get("video_url_clean")
                  or payload.get("video_url")
                  or payload.get("vredd_url")
                  or "").strip()

    if not source_url.startswith("https://v.redd.it/"):
        raise HTTPException(status_code=400, detail="vredd_url inválida")

    # Wrap the whole flow defensively so nothing ever leaks a 500
    try:
        # ---------- preflight: quick check & skip if no media ----------
        info = _preflight_info(source_url)
        has_media = bool(info.get("duration") or info.get("formats") or info.get("url"))
        if not info or not has_media:
            return _one_response(thread_id, source_url, error_message="unavailable_or_removed_preflight")

        # length hint (if present) — we’ll trim after download if needed
        need_trim = False
        try:
            if "duration" in info and info["duration"]:
                dur = float(info["duration"])
                if MAX_DURATION_S > 0 and dur > MAX_DURATION_S:
                    need_trim = True
        except Exception:
            pass

        # ---------- download (720p mp4 like original) ----------
        # Use thread_id for temp files only (not exposed), safe to keep
        out_path       = os.path.join(tempfile.gettempdir(), f"{thread_id}.mp4")
        trimmed_path   = os.path.join(tempfile.gettempdir(), f"{thread_id}.cut.mp4")
        out_for_upload = out_path

        dl_cmd = [
            "yt-dlp", *YTDLP_COMMON,
            "-f", "bv*+ba/b",
            "--merge-output-format", "mp4",
            "-S", "res:720,ext:mp4",
            "-o", out_path,
            source_url,
        ]
        try:
            _run(dl_cmd, check=True)
        except subprocess.CalledProcessError as e:
            return _one_response(thread_id, source_url, error_message=f"yt-dlp_error: {e}")

        try:
            # verify duration post-download; mark for trim if needed
            post_dur = _ffprobe_duration(out_path)
            if post_dur is not None and MAX_DURATION_S > 0 and post_dur > MAX_DURATION_S:
                need_trim = True

            # Trim locally if needed (stream copy; cut point may align to keyframe)
            if need_trim and MAX_DURATION_S > 0:
                trim_cmd = [
                    "ffmpeg", "-y",
                    "-i", out_path,
                    "-t", str(MAX_DURATION_S),
                    "-c", "copy",
                    trimmed_path,
                ]
                try:
                    _run(trim_cmd, check=True, capture=False, text=False, timeout=None)
                except subprocess.CalledProcessError as e:
                    return _one_response(thread_id, source_url, error_message=f"ffmpeg_trim_error: {e}")
                out_for_upload = trimmed_path

            # optional size guard (on the file we’ll upload)
            if MAX_BYTES > 0:
                try:
                    file_bytes = os.path.getsize(out_for_upload)
                    if file_bytes > MAX_BYTES:
                        return _one_response(thread_id, source_url, error_message=f"skipped_large_file: {file_bytes}B > {MAX_BYTES}B")
                except Exception:
                    pass

            # ---------- unsigned upload to Cloudinary via REST ----------
            up_url = f"https://api.cloudinary.com/v1_1/{CLD_NAME}/video/upload"
            # short random public_id (no "reddit" anywhere)
            public_id_random = uuid.uuid4().hex[:10]  # short, hard to reverse to thread_id

            try:
                with open(out_for_upload, "rb") as f:
                    res = requests.post(
                        up_url,
                        data={"upload_preset": CLD_PRESET, "public_id": public_id_random},
                        files={"file": f},
                        timeout=(15, 600),
                    )
            except requests.exceptions.RequestException as e:
                return _one_response(thread_id, source_url, error_message=f"cloudinary_exception: {type(e).__name__}: {e}")

            if res.status_code >= 300:
                snippet = ""
                try:
                    snippet = res.text[:200]
                except Exception:
                    pass
                return _one_response(thread_id, source_url, error_message=f"cloudinary_error: {res.status_code} {snippet}")

            j = res.json()
            public_id  = j.get("public_id")  # should be public_id_random
            secure_url = j.get("secure_url")
            thumb_url  = (
                f"https://res.cloudinary.com/{CLD_NAME}/video/upload/so_2,w_640,h_360,c_fill,q_auto,f_auto/{public_id}.jpg"
                if public_id else None
            )

            return _one_response(thread_id, source_url, public_id=public_id, secure_url=secure_url, thumb_url=thumb_url)

        finally:
            # best effort cleanup for both files
            for pth in (out_path, trimmed_path):
                try:
                    if os.path.exists(pth):
                        os.remove(pth)
                except Exception:
                    pass

    except HTTPException:
        # preserve intentional HTTP errors (e.g., bad token, bad vredd)
        raise
    except Exception as e:
        # last-ditch guard: never emit a 500 up to n8n; return structured error
        # IMPORTANT: reuse the same thread_id and source_url we already locked in (no UUID fallback)
        return _one_response(thread_id, source_url, error_message=f"unexpected_exception: {type(e).__name__}: {e}")
