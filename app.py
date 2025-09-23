# app.py — stable rollback (no cloud deps, no 500s)
import os, json, time, subprocess
from pathlib import Path
from typing import Optional, Dict, Any
from fastapi import FastAPI, Header, Query
from pydantic import BaseModel

WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "")
MAX_SECONDS = int(os.environ.get("MAX_SECONDS", "240"))       # 0 = ignore
MAX_MEGABYTES = int(os.environ.get("MAX_MEGABYTES", "120"))  # 0 = ignore
PORT = int(os.environ.get("PORT", "10000"))

TMP = Path("/tmp"); TMP.mkdir(parents=True, exist_ok=True)
app = FastAPI(title="video-worker")

class Req(BaseModel):
    reddit_id: str
    vredd_url: Optional[str] = None
    video_url: Optional[str] = None

def ok(d: Dict[str, Any]): return {"status": "ok", **d}
def skip(reason: str, **kw): return {"status": "skipped", "reason": reason, **kw}
def err(reason: str, **kw): return {"status": "error", "reason": reason, **kw}

def run_out(cmd: list[str], timeout: int) -> str:
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                       check=True, timeout=timeout)
    return p.stdout.decode("utf-8", "replace")

def probe(url: str) -> Dict[str, Any]:
    try:
        meta = json.loads(run_out(["yt-dlp","-j","--skip-download",url], timeout=45))
        dur = int(float(meta.get("duration") or 0))
        approx = int(float(meta.get("filesize_approx") or 0))
        return {"duration": dur, "approx_mb": approx//(1024*1024) if approx else 0}
    except Exception as e:
        return {"probe_error": str(e)[:300]}

def download(reddit_id: str, url: str) -> Path:
    dest = TMP / f"{reddit_id}.mp4"
    cmd = [
        "yt-dlp","-q","--no-progress",
        "-N","2","--retries","3","--fragment-retries","10","--sleep-requests","5",
        "-o", str(dest), "-f","bv*+ba/b","--merge-output-format","mp4", url
    ]
    last = None
    for delay in (5, 10, 20, 40):
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL,
                           stderr=subprocess.STDOUT, timeout=900)
            if dest.exists() and dest.stat().st_size > 0:
                return dest
            raise RuntimeError("empty file")
        except Exception as e:
            last = e
            try: dest.unlink(missing_ok=True)
            except: pass
            time.sleep(delay)
    raise last or RuntimeError("download failed")

@app.on_event("startup")
def clean_tmp():
    for g in ("*.mp4","*.part","*.webm","*.m4a","*.ytdl"):
        for p in TMP.glob(g):
            try: p.unlink()
            except: pass

@app.get("/")
def root(): return {"ok": True, "service": "video-worker"}
@app.get("/health")
def health():
    return {
        "ok": True,
        "limits": {"max_seconds": MAX_SECONDS, "max_megabytes": MAX_MEGABYTES},
        "version": os.environ.get("GIT_COMMIT","dev"),
    }

@app.post("/mux-upload")
def mux_upload(body: Req, x_token: Optional[str] = Header(default=None, alias="X-Token"),
               allow_long: int = Query(0)):
    try:
        if not (body.reddit_id and (body.vredd_url or body.video_url)):
            return err("bad_request", message="missing reddit_id or url")
        if not WORKER_TOKEN:
            return err("server_misconfigured", message="WORKER_TOKEN not set")
        if x_token != WORKER_TOKEN:
            return err("unauthorized", message="bad token")

        url = (body.vredd_url or body.video_url).strip()
        rid = body.reddit_id.strip()

        pr = probe(url)
        dur = int(pr.get("duration") or 0)
        approx_mb = int(pr.get("approx_mb") or 0)
        if not allow_long and (
            (MAX_SECONDS and dur and dur > MAX_SECONDS) or
            (MAX_MEGABYTES and approx_mb and approx_mb > MAX_MEGABYTES)
        ):
            return skip("duration_or_size_limit",
                        reddit_id=rid, url=url,
                        duration_sec=dur, limit_sec=MAX_SECONDS,
                        approx_mb=approx_mb, limit_mb=MAX_MEGABYTES)

        try:
            path = download(rid, url)
        except Exception as e:
            return err("download_failed", reddit_id=rid, message=str(e)[:500], probe_error=pr.get("probe_error"))

        size = path.stat().st_size
        mb = round(size/1024/1024, 2)
        # We’re not uploading anywhere in this rollback. Return metadata so your flow can continue.
        res = ok({
            "reddit_id": rid,
            "local_path": str(path),
            "bytes": size,
            "mb": mb,
            "duration_sec": dur or None,
            "approx_mb_probe": approx_mb or None
        })
        try: path.unlink(missing_ok=True)
        except: pass
        return res
    except Exception as e:
        return err("unexpected", message=str(e)[:500])

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=PORT)
