import os, tempfile, subprocess, uuid, requests
from fastapi import FastAPI, Body, Header, HTTPException

CLD_NAME = os.environ["CLD_NAME"]                # ex: drfh6p8cn
CLD_PRESET = os.environ["CLD_UNSIGNED_PRESET"]   # ex: brian_vid_unsigned
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
    # baixa melhor vídeo + áudio e mescla em MP4 (limite 720p p/ tamanho)
    cmd = ["yt-dlp","-f","bv*+ba/b","--merge-output-format","mp4",
           "-S","res:720,ext:mp4","-o",out_path, vredd]
    try:
        subprocess.check_call(cmd)
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail=f"yt-dlp/ffmpeg erro: {e}")

    # upload UNSIGNED para Cloudinary
    up_url = f"https://api.cloudinary.com/v1_1/{CLD_NAME}/video/upload"
    with open(out_path, "rb") as f:
        res = requests.post(up_url,
            data={"upload_preset": CLD_PRESET, "public_id": f"reddit/{rid}"},
            files={"file": f}, timeout=120)
    if res.status_code >= 300:
        raise HTTPException(status_code=500, detail=f"cloudinary: {res.text}")

    j = res.json()
    public_id  = j.get("public_id")
    secure_url = j.get("secure_url")
    # thumbnail (frame no segundo 2, 640x360)
    thumb_url = f"https://res.cloudinary.com/{CLD_NAME}/video/upload/so_2,w_640,h_360,c_fill,q_auto,f_auto/{public_id}.jpg"
    return {"reddit_id": rid, "public_id": public_id, "secure_url": secure_url, "thumb_url": thumb_url}
