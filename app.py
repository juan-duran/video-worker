import os, tempfile, subprocess, uuid, json, time, shlex, requests
from fastapi import FastAPI, Body, Header, HTTPException
from fastapi.responses import PlainTextResponse

# ---------- ENV ----------
CLD_NAME        = os.environ["CLD_NAME"]                 # ex: drfh6p8cn
CLD_PRESET      = os.environ["CLD_UNSIGNED_PRESET"]      # ex: brian_vid_unsigned
WORKER_TOKEN    = os.environ.get("WORKER_TOKEN", "")
MAX_DURATION_S  = int(os.environ.get("MAX_DURATION_SEC", "240"))  # hard stop (e.g. 240s = 4 min)
MAX_BYTES       = int(os.environ.get("MAX_BYTES", "0"))  # optional hard stop by size (0 = ignore)

# yt-dlp politeness/retry tuning to reduce 429s
YTDLP_COMMON = [
    "--no-progress",
    "--retries", "10",
    "--fragment-retries", "10",
    "--retry-sleep", "linear=1:10",
    "--sleep-requests", "1",
    "--concurrent-fragments", "1",
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

def _preflight_info(url: str, retries: int = 5, backoff_base: float = 1.0) -> dict:
    """
    Use yt-dlp to fetch metadata only (no download). Returns {} on failure.
    """
    for attempt in range(1, retries + 1):
        try:
            cmd = ["yt-dlp", *YTDLP_COMMON, "-j", "--skip-download", url]
            p = _run(cmd, check=True, capture=True, text=True, timeout=60)
            # yt-dlp may print multiple lines; take the first valid JSON line
            for line in p.stdout.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    continue
        except subprocess.CalledProcessError:
            pass
        # backoff
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

def _stable_response(reddit_id: str, source_url: str, public_id=None, secure_url=None, thumb_url=None):
    """
    EXACT output shape your nodes expect. Keys always present.
    """
    return {
        "reddit_id": reddit_id,
        "source_url": source_url,
        "public_id": public_id,
        "secure_url": secure_url,
        "thumb_url":  thumb_url,
    }

# ---------- main endpoint ----------
@app.post("/mux-upload")
def mux_upload(payload: dict = Body(...), x_token: str = Header(default="")):
    # Auth
    if WORKER_TOKEN and x_token != WORKER_TOKEN:
        raise HTTPException(status_code=401, detail="invalid token")

    vredd = (payload.get("vredd_url") or "").strip()
    rid   = (payload.get("reddit_id") or str(uuid.uuid4())).strip()
    if not vredd.startswith("https://v.redd.it/"):
        raise HTTPException(status_code=400, detail="vredd_url inválida")

    # ---------- preflight: skip long videos BEFORE download ----------
    info = _preflight_info(vredd)
    dur  = None
    try:
        # yt-dlp duration is seconds (float)
        if "duration" in info and info["duration"]:
            dur = float(info["duration"])
    except Exception:
        dur = None

    if dur is not None and MAX_DURATION_S > 0 and dur > MAX_DURATION_S:
        # Skip cleanly with stable shape (null urls)
        return _stable_response(rid, vredd, public_id=None, secure_url=None, thumb_url=None)

    # ---------- download (cap at 720p mp4 like your original) ----------
    out_path = os.path.join(tempfile.gettempdir(), f"{rid}.mp4")
    dl_cmd = [
        "yt-dlp",
        *YTDLP_COMMON,
        "-f", "bv*+ba/b",
        "--merge-output-format", "mp4",
        "-S", "res:720,ext:mp4",
        "-o", out_path,
        vredd,
    ]
    try:
        _run(dl_cmd, check=True)
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"yt-dlp/ffmpeg erro: {e}")

    # Optional: hard-stop by bytes if configured
    try:
        file_bytes = os.path.getsize(out_path)
        if MAX_BYTES > 0 and file_bytes > MAX_BYTES:
            try: os.remove(out_path)
            except Exception: pass
            return _stable_response(rid, vredd, public_id=None, secure_url=None, thumb_url=None)
    except Exception:
        pass

    # Double-check duration if preflight failed
    if dur is None:
        dur = _ffprobe_duration(out_path)
        if dur is not None and MAX_DURATION_S > 0 and dur > MAX_DURATION_S:
            try: os.remove(out_path)
            except Exception: pass
            return _stable_response(rid, vredd, public_id=None, secure_url=None, thumb_url=None)

    # ---------- unsigned upload to Cloudinary via REST (no SDK) ----------
    # NOTE: This keeps your original, working approach.
    up_url = f"https://api.cloudinary.com/v1_1/{CLD_NAME}/video/upload"
    public_id = f"reddit/{rid}"

    try:
        with open(out_path, "rb") as f:
            res = requests.post(
                up_url,
                data={"upload_preset": CLD_PRESET, "public_id": public_id},
                files={"file": f},
                timeout=600
            )
    finally:
        # best effort cleanup
        try: os.remove(out_path)
        except Exception: pass

    if res.status_code >= 300:
        # On upload error, still return stable shape with null urls
        # (so the flow can "continue on error" and not explode)
        return _stable_response(rid, vredd, public_id=None, secure_url=None, thumb_url=None)

    j = res.json()
    public_id  = j.get("public_id")
    secure_url = j.get("secure_url")
    thumb_url  = f"https://res.cloudinary.com/{CLD_NAME}/video/upload/so_2,w_640,h_360,c_fill,q_auto,f_auto/{public_id}.jpg" if public_id else None

    return _stable_response(rid, vredd, public_id=public_id, secure_url=secure_url, thumb_url=thumb_url)
