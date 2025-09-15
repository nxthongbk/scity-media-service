from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
import datetime
import logging
import sys
import boto3
from botocore.exceptions import ClientError
import os
import py_eureka_client.eureka_client as eureka_client

# --- Logging ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

EUREKA_SERVER = "http://discovery:8761/eureka"
APP_NAME = "media-service"
APP_PORT = 9002
HOSTNAME = "media-service"

app = FastAPI()

# --- MinIO config (dùng env cho bảo mật) ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "itp@2025")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "securevision")

# MinIO client
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

def generate_stream(obj_body, chunk_size=8192):
    """Stream object từ MinIO thành từng chunk nhỏ"""
    for chunk in obj_body.iter_chunks(chunk_size=chunk_size):
        if chunk:
            yield chunk

# --- Eureka ---
@app.on_event("startup")
async def startup_event():
    try:
        await eureka_client.init_async(
            eureka_server=EUREKA_SERVER,
            app_name=APP_NAME,
            instance_port=APP_PORT,
            instance_host=HOSTNAME,
            instance_id=f"{HOSTNAME}:{APP_PORT}",
            data_center_name="MyOwn"
        )
        logger.info("Registered with Eureka successfully")
    except Exception as e:
        logger.error(f"Eureka registration failed: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    try:
        await eureka_client.stop_async()
        logger.info("Deregistered from Eureka")
    except Exception as e:
        logger.error(f"Eureka deregistration failed: {e}")

# --- API ---
@app.get("/get_video")
async def get_video(datetime: str, camera: str = "cam01"):
    logger.info(f"Received request with datetime={datetime}, camera={camera}")

    try:
        # input dạng YYYY-MM-DD_HH:MM:SS
        input_time = datetime.datetime.strptime(datetime, "%Y-%m-%d_%H:%M:%S")
    except Exception:
        logger.error(f"Invalid datetime format: {datetime}")
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid datetime format. Use YYYY-MM-DD_HH:MM:SS"},
        )

    try:
        # ==============================
        # Tạo prefix: camera/YYYY/MM/DD/
        # ==============================
        prefix = f"{camera}/{input_time.strftime('%Y/%m/%d/')}"
        logger.info(f"Listing objects in prefix={prefix}")

        objects = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
        if "Contents" not in objects:
            return JSONResponse(
                status_code=404,
                content={"error": f"No files found for {prefix}"},
            )

        file_list = [obj["Key"] for obj in objects["Contents"]]

        return {"files": file_list}

    except ClientError as e:
        logger.error(f"Error accessing MinIO: {e}")
        return JSONResponse(status_code=500, content={"error": "MinIO access error"})
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return JSONResponse(status_code=500, content={"error": "Server error"})

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI video API service...")
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)
