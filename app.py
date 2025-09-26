import os, tempfile, subprocess, uuid, json, time, requests
from fastapi import FastAPI, Body, Header, HTTPException
from fastapi.responses import PlainTextResponse

# ---------- ENV ----------
CLD_NAME        = os.environ["CLD_NAME"]
CLD_PRESET      = os.environ["CLD_UNSIGNED_PRESET"]
WORKER_TOKEN    = os.environ.get("WORKER_TOKEN", "")
MAX_DURATION_S  = int(os.environ.get("MAX_DURATION_SEC", "300"))
MAX_BYTES       = int(os.environ.get("MAX_BYTES", "0"))

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

def _run(cmd: list[str], check=True, capture=False, text=False, timeout=None):
    return subprocess.run(
        cmd, check=check,
        stdout=(subprocess.PIPE if capture else None),
        stderr=(subprocess.PIPE if capture else None),
        text=text, timeout=timeout
    )

def _preflight_info(url: str, retries: int = 1, backoff_base: float = 1.0) -> dict:
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

def _stable_response(thread_id: str, source_url: str, public_id=None, secure_url=None, thumb_url=None):
    # Keep reddit_id for backwards compatibility; thread_id is the canonical name going forward.
    return {
        "thread_id": thread_id,
        "reddit_id": thread_id,
        "source_url": source_url,
        "public_id": public_id,
        "secure_url": secure_url,
        "thumb_url":  thumb_url,
    }

def _error_payload(thread_id: str, source_url: str, message: str):
    base = _stable_response(thread_id, source_url, None, None, None)
    base["video_url_clean"] = source_url
    base["error"] = {"message": message}
    return base

@app.post("/mux-upload")
def mux_upload(payload: dict = Body(...), x_token: str = Header(default="")):
    if WORKER_TOKEN and x_token != WORKER_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")

    try:
        # Accept new names first, fall back to legacy ones
        tid   = (payload.get("thread_id") or payload.get("reddit_id") or str(uuid.uuid4())).strip()
        vredd = (payload.get("video_url_clean") or payload.get("vredd_url") or payload.get("video_url") or "").strip()

        if not vredd.startswith("https://v.redd.it/"):
            raise HTTPException(status_code=400, detail="vredd_url inválida")

        info = _preflight_info(vredd)
        has_media = bool(info.get("duration") or info.get("formats") or info.get("url"))
        if not info or not has_media:
            return _error_payload(tid, vredd, "unavailable_or_removed_preflight")

        need_trim = False
        try:
            if "duration" in info and info["duration"]:
                if MAX_DURATION_S > 0 and float(info["duration"]) > MAX_DURATION_S:
                    need_trim = True
        except Exception:
            pass

        out_path       = os.path.join(tempfile.gettempdir(), f"{tid}.mp4")
        trimmed_path   = os.path.join(tempfile.gettempdir(), f"{tid}.cut.mp4")
        out_for_upload = out_path

        dl_cmd = [
            "yt-dlp", *YTDLP_COMMON,
            "-f", "bv*+ba/b",
            "--merge-output-format", "mp4",
            "-S", "res:720,ext:mp4",
            "-o", out_path,
            vredd,
        ]
        try:
            _run(dl_cmd, check=True)
        except subprocess.CalledProcessError as e:
            return _error_payload(tid, vredd, f"yt-dlp_error: {e}")

        try:
            post_dur = _ffprobe_duration(out_path)
            if post_dur is not None and MAX_DURATION_S > 0 and post_dur > MAX_DURATION_S:
                need_trim = True

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
                    return _error_payload(tid, vredd, f"ffmpeg_trim_error: {e}")
                out_for_upload = trimmed_path

            if MAX_BYTES > 0:
                try:
                    file_bytes = os.path.getsize(out_for_upload)
                    if file_bytes > MAX_BYTES:
                        return _error_payload(tid, vredd, f"skipped_large_file: {file_bytes}B > {MAX_BYTES}B")
                except Exception:
                    pass

            # Use thread_id directly (no 'reddit/' prefix)
            up_url    = f"https://api.cloudinary.com/v1_1/{CLD_NAME}/video/upload"
            public_id = tid

            try:
                with open(out_for_upload, "rb") as f:
                    res = requests.post(
                        up_url,
                        data={"upload_preset": CLD_PRESET, "public_id": public_id},
                        files={"file": f},
                        timeout=(15, 600),
                    )
            except requests.exceptions.RequestException as e:
                return _error_payload(tid, vredd, f"cloudinary_exception: {type(e).__name__}: {e}")

            if res.status_code >= 300:
                snippet = ""
                try:
                    snippet = res.text[:200]
                except Exception:
                    pass
                return _error_payload(tid, vredd, f"cloudinary_error: {res.status_code} {snippet}")

            j = res.json()
            public_id  = j.get("public_id")
            secure_url = j.get("secure_url")
            thumb_url  = (
                f"https://res.cloudinary.com/{CLD_NAME}/video/upload/so_2,w_640,h_360,c_fill,q_auto,f_auto/{public_id}.jpg"
                if public_id else None
            )

            return _stable_response(tid, vredd, public_id=public_id, secure_url=secure_url, thumb_url=thumb_url)

        finally:
            for pth in (out_path, trimmed_path):
                try:
                    if os.path.exists(pth):
                        os.remove(pth)
                except Exception:
                    pass

    except HTTPException:
        raise
    except Exception as e:
        tid = (payload.get("thread_id") or payload.get("reddit_id") or str(uuid.uuid4())).strip()
        vredd = (payload.get("video_url_clean") or payload.get("vredd_url") or payload.get("video_url") or "").strip()
        return _error_payload(tid, vredd, f"unexpected_exception: {type(e).__name__}: {e}")
