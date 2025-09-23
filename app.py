# app.py
import os, time, json, subprocess
from pathlib import Path
from threading import Lock
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Query
from pydantic import BaseModel

import cloudinary
import cloudinary.uploader as cu

WORKER_TOKEN = os.environ.get("WORKER_TOKEN")

# soft limits (tune these)
MAX_SECONDS = int(os.environ.get("MAX_SECONDS", "240"))       # 4 min
MAX_MEGABYTES = int(os.environ.get("MAX_MEGABYTES", "120"))  # 120 MB

# Cloudinary config
if os.environ.get("CLOUDINARY_URL"):
    cloudinary.config(cloudinary_url=os.environ["CLOUDINARY_URL"])
else:
    cloudinary.config(
        cloud_name=os.environ.get("CLOUDINARY_CLOUD_NAME"),
        api_key=os.environ.get("CLOUDINARY_API_KEY"),
        api_secret=os.environ.get("CLOUDINARY_API_SECRET"),
        secure=True,
    )

TMP_DIR = Path("/tmp"); TMP_DIR.mkdir(parents=True, exist_ok=True)
_lock = Lock()
_busy = False

app = FastAPI(title="video-worker-iidb")

class MuxUploadBody(BaseModel):
    reddit_id: str
    vredd_url: Optional[str] = None
    video_url: Optional[str] = None

def run_quiet(cmd: list[str], timeout: Optional[int] = None) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL,
                   stderr=subprocess.STDOUT, timeout=timeout)

def run_capture(cmd: list[str], timeout: int = 60) -> str:
    out = subprocess.run(cmd, check=True, stdout=subprocess.PIPE,
                         stderr=subprocess.DEVNULL, timeout=timeout)
    return out.stdout.decode("utf-8", "replace")

def preflight_probe(url: str) -> dict:
    # Ask yt-dlp for metadata only (no download)
    # -j prints one JSON object; we parse duration/filesize_approx if present
    meta_raw = run_capture(["yt-dlp", "-j", "--skip-download", url], timeout=45)
    data = json.loads(meta_raw)
    duration = int(float(data.get("duration") or 0))
    # filesize_approx is bytes if present; fall back to 0
    approx_bytes = int(float(data.get("filesize_approx") or 0))
    approx_mb = approx_bytes // (1024 * 1024)
    return {"duration": duration, "approx_mb": approx_mb}

def download_video(reddit_id: str, url: str) -> Path:
    dest = TMP_DIR / f"{reddit_id}.mp4"
    yt_cmd = [
        "yt-dlp",
        "-q", "--no-progress",
        "-N", "2",
        "--retries", "3",
        "--fragment-retries", "10",
        "--sleep-requests", "5",
        "-o", str(dest),
        "-f", "bv*+ba/b",
        "--merge-output-format", "mp4",
        url,
    ]
    backoff = [10, 20, 40]
    last = None
    for i in range(len(backoff)):
        try:
            run_quiet(yt_cmd, timeout=900)  # 15 min hard cap per download
            if dest.exists() and dest.stat().st_size > 0:
                return dest
            raise RuntimeError("download produced empty file")
        except Exception as e:
            last = e
            try: dest.unlink(missing_ok=True)
            except: pass
            time.sleep(backoff[i])
    raise last or RuntimeError("download failed")

def upload_to_cloudinary(reddit_id: str, path: Path) -> dict:
    public_id = f"reddit/{reddit_id}"
    res = cu.upload_large(
        str(path),
        resource_type="video",
        public_id=public_id,
        overwrite=True,
        chunk_size=20_000_000,
        eager=[{
            "format": "jpg", "resource_type": "video",
            "start_offset": "2", "width": 640, "height": 360,
            "crop": "fill", "quality": "auto", "fetch_format": "auto",
        }],
    )
    thumb = None
    if isinstance(res.get("eager"), list) and res["eager"]:
        thumb = res["eager"][0].get("secure_url")
    return {
        "status": "ok",
        "reddit_id": reddit_id,
        "public_id": public_id,
        "secure_url": res["secure_url"],
        "thumb_url": thumb,
    }

def tmp_usage_mb() -> int:
    try:
        return sum(p.stat().st_size for p in TMP_DIR.glob("*") if p.is_file()) // (1024*1024)
    except: return -1

@app.on_event("startup")
def cleanup_tmp_on_start():
    for glob in ("*.mp4","*.part","*.ytdl","*.m4a","*.webm"):
        for p in TMP_DIR.glob(glob):
            try: p.unlink()
            except: pass

@app.get("/")
def root(): return {"ok": True, "service": "video-worker-iidb"}

@app.get("/health")
def health():
    return {"ok": True, "busy": _busy, "tmp_mb": tmp_usage_mb(),
            "limits": {"max_seconds": MAX_SECONDS, "max_megabytes": MAX_MEGABYTES},
            "version": os.environ.get("GIT_COMMIT", "dev")}

@app.post("/mux-upload")
def mux_upload(
    body: MuxUploadBody,
    x_token: Optional[str] = Header(default=None, alias="X-Token"),
    allow_long: Optional[int] = Query(default=0)  # also allow ?allow_long=1
):
    global _busy
    if not WORKER_TOKEN:
        raise HTTPException(500, "server misconfigured: WORKER_TOKEN not set")
    if x_token != WORKER_TOKEN:
        raise HTTPException(401, "bad token")

    reddit_id = (body.reddit_id or "").strip()
    url = (body.vredd_url or body.video_url or "").strip()
    if not reddit_id or not url:
        raise HTTPException(400, "missing reddit_id or vredd_url/video_url")

    # serialize heavy work
    with _lock:
        _busy = True
        path = None
        try:
            # ---- NEW: preflight probe + skip logic ----
            meta = preflight_probe(url)
            duration = meta.get("duration", 0)
            approx_mb = meta.get("approx_mb", 0)
            if not allow_long and (
                (MAX_SECONDS and duration and duration > MAX_SECONDS) or
                (MAX_MEGABYTES and approx_mb and approx_mb > MAX_MEGABYTES)
            ):
                # Return 200 so the workflow can continue cleanly
                return {
                    "status": "skipped",
                    "reason": "duration_or_size_limit",
                    "duration_sec": duration,
                    "limit_sec": MAX_SECONDS,
                    "approx_mb": approx_mb,
                    "limit_mb": MAX_MEGABYTES,
                    "reddit_id": reddit_id,
                    "url": url,
                }

            # proceed normally
            path = download_video(reddit_id, url)
            return upload_to_cloudinary(reddit_id, path)

        finally:
            _busy = False
            if path:
                try: path.unlink(missing_ok=True)
                except: pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT","10000")))
