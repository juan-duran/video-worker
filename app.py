# app.py — crash-proof worker (no 500s)
import os, time, json, subprocess
from pathlib import Path
from threading import Lock
from typing import Optional, Dict, Any

from fastapi import FastAPI, Header, Query
from pydantic import BaseModel

WORKER_TOKEN = os.environ.get("WORKER_TOKEN") or ""  # if missing, we still return 200 with error payload
MAX_SECONDS = int(os.environ.get("MAX_SECONDS", "240"))        # 0 = ignore duration limit
MAX_MEGABYTES = int(os.environ.get("MAX_MEGABYTES", "120"))    # 0 = ignore size limit

TMP_DIR = Path("/tmp"); TMP_DIR.mkdir(parents=True, exist_ok=True)
app = FastAPI(title="video-worker-iidb")

_lock = Lock()
_busy = False

class MuxUploadBody(BaseModel):
    reddit_id: str
    vredd_url: Optional[str] = None
    video_url: Optional[str] = None

def _json_ok(data: Dict[str, Any]) -> Dict[str, Any]:
    return {"status": "ok", **data}

def _json_skip(reason: str, **extra) -> Dict[str, Any]:
    return {"status": "skipped", "reason": reason, **extra}

def _json_err(reason: str, **extra) -> Dict[str, Any]:
    return {"status": "error", "reason": reason, **extra}

def _tmp_usage_mb() -> int:
    try:
        return sum(p.stat().st_size for p in TMP_DIR.glob("*") if p.is_file()) // (1024*1024)
    except:
        return -1

def _cloud_env_present() -> bool:
    if os.environ.get("CLOUDINARY_URL"):
        return True
    need = ("CLOUDINARY_CLOUD_NAME", "CLOUDINARY_API_KEY", "CLOUDINARY_API_SECRET")
    return all(os.environ.get(k) for k in need)

def _run_q(cmd: list[str], timeout: Optional[int] = None) -> None:
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT, timeout=timeout)

def _run_out(cmd: list[str], timeout: int = 60) -> str:
    out = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
    return out.stdout.decode("utf-8", "replace")

def preflight_probe(url: str) -> Dict[str, Any]:
    # Try to get duration & approx size quickly; tolerate failures.
    try:
        meta_raw = _run_out(["yt-dlp", "-j", "--skip-download", url], timeout=45)
        data = json.loads(meta_raw)
        duration = int(float(data.get("duration") or 0))
        approx_bytes = int(float(data.get("filesize_approx") or 0))
        approx_mb = approx_bytes // (1024 * 1024) if approx_bytes else 0
        return {"duration": duration, "approx_mb": approx_mb}
    except Exception as e:
        return {"probe_error": str(e)[:300]}

def download_video(reddit_id: str, url: str) -> Path:
    dest = TMP_DIR / f"{reddit_id}.mp4"
    yt_cmd = [
        "yt-dlp", "-q", "--no-progress",
        "-N", "2", "--retries", "3", "--fragment-retries", "10",
        "--sleep-requests", "5",
        "-o", str(dest),
        "-f", "bv*+ba/b",
        "--merge-output-format", "mp4",
        url,
    ]
    backoff = [10, 20, 40]
    last_exc = None
    for delay in backoff:
        try:
            _run_q(yt_cmd, timeout=900)  # cap to 15m to avoid runaway
            if dest.exists() and dest.stat().st_size > 0:
                return dest
            raise RuntimeError("download produced empty file")
        except Exception as e:
            last_exc = e
            try: dest.unlink(missing_ok=True)
            except: pass
            time.sleep(delay)
    raise last_exc or RuntimeError("download failed")

def upload_to_cloudinary(reddit_id: str, path: Path) -> Dict[str, Any]:
    if not _cloud_env_present():
        return _json_skip("cloudinary_not_configured", reddit_id=reddit_id)

    # Import & configure only when actually needed, fully guarded
    try:
        import cloudinary
        import cloudinary.uploader as cu
        if os.environ.get("CLOUDINARY_URL"):
            cloudinary.config(cloudinary_url=os.environ["CLOUDINARY_URL"])
        else:
            cloudinary.config(
                cloud_name=os.environ.get("CLOUDINARY_CLOUD_NAME"),
                api_key=os.environ.get("CLOUDINARY_API_KEY"),
                api_secret=os.environ.get("CLOUDINARY_API_SECRET"),
                secure=True,
            )
    except Exception as e:
        return _json_skip("cloudinary_import_or_config_error", message=str(e)[:300], reddit_id=reddit_id)

    public_id = f"reddit/{reddit_id}"
    try:
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
        return _json_ok({
            "reddit_id": reddit_id,
            "public_id": public_id,
            "secure_url": res.get("secure_url"),
            "thumb_url": thumb,
            "bytes": res.get("bytes"),
            "width": res.get("width"),
            "height": res.get("height"),
        })
    except Exception as e:
        # Never 500—surface as error payload
        return _json_err("cloudinary_upload_failed", message=str(e)[:500], reddit_id=reddit_id)

@app.on_event("startup")
def cleanup_tmp_on_start():
    for glob in ("*.mp4","*.part","*.ytdl","*.m4a","*.webm"):
        for p in TMP_DIR.glob(glob):
            try: p.unlink()
            except: pass

@app.get("/")
def root():
    return {"ok": True, "service": "video-worker-iidb"}

@app.get("/health")
def health():
    return {
        "ok": True,
        "busy": _busy,
        "tmp_mb": _tmp_usage_mb(),
        "limits": {"max_seconds": MAX_SECONDS, "max_megabytes": MAX_MEGABYTES},
        "cloudinary_configured": _cloud_env_present(),
        "version": os.environ.get("GIT_COMMIT", "dev"),
    }

@app.post("/mux-upload")
def mux_upload(
    body: MuxUploadBody,
    x_token: Optional[str] = Header(default=None, alias="X-Token"),
    allow_long: Optional[int] = Query(default=0),
):
    # ALWAYS return 200 with a JSON status. No exceptions bubble out.
    global _busy
    reddit_id = (body.reddit_id or "").strip()
    url = (body.vredd_url or body.video_url or "").strip()

    if not reddit_id or not url:
        return _json_err("bad_request", message="missing reddit_id or vredd_url/video_url")

    if not WORKER_TOKEN:
        return _json_err("server_misconfigured", message="WORKER_TOKEN not set")
    if x_token != WORKER_TOKEN:
        return _json_err("unauthorized", message="bad token")

    with _lock:
        _busy = True
        path = None
        try:
            probe = preflight_probe(url)
            duration = int(probe.get("duration") or 0)
            approx_mb = int(probe.get("approx_mb") or 0)

            if not allow_long and (
                (MAX_SECONDS and duration and duration > MAX_SECONDS) or
                (MAX_MEGABYTES and approx_mb and approx_mb > MAX_MEGABYTES)
            ):
                return _json_skip(
                    "duration_or_size_limit",
                    reddit_id=reddit_id, url=url,
                    duration_sec=duration, limit_sec=MAX_SECONDS,
                    approx_mb=approx_mb, limit_mb=MAX_MEGABYTES,
                )

            if probe.get("probe_error"):
                # We can still try download, but mark the issue for observability
                probe_note = probe["probe_error"][:200]
            else:
                probe_note = None

            try:
                path = download_video(reddit_id, url)
            except Exception as e:
                return _json_err("download_failed", reddit_id=reddit_id, message=str(e)[:500], probe_error=probe_note)

            # Upload (optional; returns ok/skipped/error, never raises)
            result = upload_to_cloudinary(reddit_id, path)
            if probe_note and result.get("status") == "ok":
                result["note"] = {"probe_warning": probe_note}
            return result

        except Exception as e:
            return _json_err("unexpected", message=str(e)[:500], reddit_id=reddit_id)
        finally:
            _busy = False
            if path:
                try: path.unlink(missing_ok=True)
                except: pass

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.environ.get("PORT","10000")))
