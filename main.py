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

# --- MinIO config ---
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
    """
    Lấy video gần nhất với datetime yêu cầu.
    Input datetime: YYYY-MM-DD_HH:MM:SS
    """
    logger.info(f"Received request with datetime={datetime}, camera={camera}")

    try:
        input_time = datetime.datetime.strptime(datetime, "%Y-%m-%d_%H:%M:%S")
    except Exception:
        logger.error(f"Invalid datetime format: {datetime}")
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid datetime format. Use YYYY-MM-DD_HH:MM:SS"},
        )

    try:
        # ==============================
        # Tạo prefix: camera/YYYY-MM-DD/
        # ==============================
        date_str = input_time.strftime("%Y-%m-%d")
        prefix = f"{camera}/{date_str}/"
        logger.info(f"Listing objects in prefix={prefix}")

        objects = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
        if "Contents" not in objects:
            return JSONResponse(
                status_code=404,
                content={"error": f"No files found for {prefix}"},
            )

        file_list = sorted([obj["Key"] for obj in objects["Contents"]])

        # Chuẩn hóa target file name: HHMMSS.mp4
        target_name = input_time.strftime("%H%M%S") + ".mp4"

        # Tìm file chính xác hoặc gần nhất
        selected_file = None
        if target_name in [os.path.basename(f) for f in file_list]:
            selected_file = [f for f in file_list if f.endswith(target_name)][0]
        else:
            # fallback: chọn file gần nhất nhỏ hơn hoặc bằng target
            sorted_files = sorted(file_list)
            for f in reversed(sorted_files):
                fname = os.path.basename(f)
                if fname <= target_name:
                    selected_file = f
                    break

        if not selected_file:
            return JSONResponse(
                status_code=404,
                content={"error": f"No matching video found for {datetime}"},
            )

        logger.info(f"Selected file: {selected_file}")

        obj = s3_client.get_object(Bucket=MINIO_BUCKET, Key=selected_file)
        return StreamingResponse(
            generate_stream(obj["Body"]),
            media_type="video/mp4"
        )

    except ClientError as e:
        logger.error(f"Error accessing MinIO: {e}")
        return JSONResponse(status_code=500, content={"error": "MinIO access error"})
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return JSONResponse(status_code=500, content={"error": "Server error"})


@app.get("/list_videos")
async def list_videos(camera: str, date: str):
    """
    Liệt kê toàn bộ file video trong 1 ngày của camera
    date: YYYY-MM-DD
    """
    prefix = f"{camera}/{date}/"
    logger.info(f"Listing all videos in {prefix}")
    try:
        objects = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
        if "Contents" not in objects:
            return {"files": []}
        return {"files": sorted([obj["Key"] for obj in objects["Contents"]])}
    except Exception as e:
        logger.error(f"Error: {e}")
        return JSONResponse(status_code=500, content={"error": "Server error"})


if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI video API service...")
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)
