import os, tempfile, subprocess, uuid
from fastapi import FastAPI, Body, Header, HTTPException
import cloudinary, cloudinary.uploader as cu

cloudinary.config(
  cloud_name=os.environ["CLD_NAME"],
  api_key=os.environ["CLD_KEY"],
  api_secret=os.environ["CLD_SECRET"],
)
WORKER_TOKEN = os.environ.get("WORKER_TOKEN","")
app = FastAPI()

@app.get("/ping")
def ping(): return {"ok": True}

@app.post("/mux-upload")
def mux_upload(payload: dict = Body(...), x_token: str = Header(default="")):
  if WORKER_TOKEN and x_token != WORKER_TOKEN:
    raise HTTPException(status_code=401, detail="invalid token")
  vredd = (payload.get("vredd_url") or "").strip()
  rid   = (payload.get("reddit_id") or str(uuid.uuid4())).strip()
  if not vredd.startswith("https://v.redd.it/"):
    raise HTTPException(status_code=400, detail="vredd_url inválida")

  out_path = os.path.join(tempfile.gettempdir(), f"{rid}.mp4")
  cmd = ["yt-dlp","-f","bv*+ba/b","--merge-output-format","mp4","-S","res:720,ext:mp4","-o",out_path, vredd]
  try: subprocess.check_call(cmd)
  except subprocess.CalledProcessError as e:
    raise HTTPException(status_code=500, detail=f"yt-dlp/ffmpeg erro: {e}")

  try:
    res = cu.upload_large(out_path, resource_type="video", public_id=f"reddit/{rid}", chunk_size=6_000_000)
  except Exception as e:
    raise HTTPException(status_code=500, detail=f"cloudinary erro: {e}")

  return {"reddit_id": rid, "secure_url": res.get("secure_url"),
          "bytes": res.get("bytes"), "width": res.get("width"), "height": res.get("height")}
