import os, subprocess, requests
from fastapi import FastAPI, HTTPException, Request

APP_TOKEN = os.environ.get("WORKER_TOKEN", "")
CLOUD = os.environ.get("CLOUDINARY_CLOUD_NAME", "drfh6p8cn")
PRESET_VID = os.environ.get("CLOUDINARY_UPLOAD_PRESET_VID", "brian_vid_unsigned")

app = FastAPI()

@app.get("/")
def root():
    return {"ok": True}

@app.get("/health")
def health():
    return {"ok": True}

def run_ytdlp(url: str, out_basename: str) -> str:
    out_mp4 = f"/tmp/{out_basename}.mp4"
    cmd = [
        "yt-dlp",
        "-v",
        "-f", "bv*+ba/b",
        "--merge-output-format", "mp4",
        "--no-call-home",
        "--no-progress",
        # be nice to v.redd.it to avoid 429s:
        "--sleep-requests", os.getenv("YTDLP_SLEEP_REQUESTS", "2"),
        "--retries", os.getenv("YTDLP_RETRIES", "10"),
        "--fragment-retries", os.getenv("YTDLP_FRAGMENT_RETRIES", "20"),
        "--retry-sleep", os.getenv("YTDLP_RETRY_SLEEP", "5"),
        "--add-header", "User-Agent: Mozilla/5.0",
        "--add-header", "Referer: https://www.reddit.com",
        "-o", out_mp4,
        url,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    if proc.returncode != 0:
        combined = f"{proc.stdout}\n{proc.stderr}"
        if "HTTP Error 429" in combined or "Too Many Requests" in combined:
            # tell caller to retry later instead of hard 500
            raise HTTPException(status_code=429, detail="Upstream 429 from v.redd.it", headers={"Retry-After": "60"})
        # surface last part of logs for debugging
        raise HTTPException(status_code=500, detail="yt-dlp failed", headers={"X-Worker-Error": combined[-2000:]})
    if not os.path.exists(out_mp4):
        raise HTTPException(status_code=500, detail="Downloaded file not found")
    return out_mp4

def upload_to_cloudinary(mp4_path: str, reddit_id: str):
    public_id = f"reddit/{reddit_id}"
    url = f"https://api.cloudinary.com/v1_1/{CLOUD}/video/upload"
    with open(mp4_path, "rb") as f:
        resp = requests.post(
            url,
            data={"upload_preset": PRESET_VID, "public_id": public_id, "resource_type": "video"},
            files={"file": f},
            timeout=120,
        )
    if resp.status_code >= 400:
        raise HTTPException(status_code=502, detail=f"Cloudinary upload failed: {resp.text[:500]}")
    data = resp.json()
    secure_url = data.get("secure_url")
    # thumbnail via transformation (same pattern you used)
    thumb_url = f"https://res.cloudinary.com/{CLOUD}/video/upload/so_2,w_640,h_360,c_fill,q_auto,f_auto/{public_id}.jpg"
    return public_id, secure_url, thumb_url

@app.post("/mux-upload")
def mux_upload(payload: dict, request: Request):
    token = request.headers.get("X-Token", "")
    if not APP_TOKEN or token != APP_TOKEN:
        raise HTTPException(status_code=401, detail="Bad token")

    reddit_id = (payload.get("reddit_id") or "").strip()
    vredd_url = (payload.get("vredd_url") or "").strip()
    if not reddit_id or not vredd_url:
        raise HTTPException(status_code=400, detail="Missing reddit_id or vredd_url")

    mp4_path = run_ytdlp(vredd_url, reddit_id)
    try:
        public_id, secure_url, thumb_url = upload_to_cloudinary(mp4_path, reddit_id)
    finally:
        try: os.remove(mp4_path)
        except: pass

    return {
        "reddit_id": reddit_id,
        "public_id": public_id,
        "secure_url": secure_url,
        "thumb_url": thumb_url,
    }
