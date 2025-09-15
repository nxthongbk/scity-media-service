from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
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

s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)


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
async def get_video(
    alarm_time: str = Query(..., description="Format: YYYY-MM-DD_HH:MM:SS"),
    camera: str = Query(..., description="Camera name (folder prefix)")
):
    logger.info(f"Received request with alarm_time={alarm_time}, camera={camera}")

    # Parse datetime
    try:
        input_time = datetime.datetime.strptime(alarm_time, "%Y-%m-%d_%H:%M:%S")
    except Exception:
        return JSONResponse(
            status_code=400,
            content={"error": "Invalid datetime format. Use YYYY-MM-DD_HH:MM:SS"},
        )

    try:
        # Prefix: camera/YYYY-MM-DD/
        prefix = f"{camera}/{input_time.strftime('%Y-%m-%d/')}"
        logger.info(f"Listing objects in prefix={prefix}")

        objects = s3_client.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
        if "Contents" not in objects:
            return JSONResponse(
                status_code=404,
                content={"error": f"No files found for {prefix}"},
            )

        file_list = [obj["Key"] for obj in objects["Contents"] if obj["Key"].endswith(".mp4")]
        logger.info(f"Found {len(file_list)} files under {prefix}")

        closest_file = None
        closest_delta = None

        for f in file_list:
            fname = os.path.basename(f)  # ví dụ: 141530.mp4
            ts_str = fname.replace(".mp4", "")
            try:
                file_dt = datetime.datetime.strptime(
                    input_time.strftime("%Y-%m-%d_") + ts_str,
                    "%Y-%m-%d_%H%M%S"
                )
            except ValueError:
                continue

            if file_dt <= input_time:
                delta = (input_time - file_dt).total_seconds()
                if closest_delta is None or delta < closest_delta:
                    closest_delta = delta
                    closest_file = f

        if closest_file:
            return {"file": closest_file}
        else:
            return JSONResponse(
                status_code=404,
                content={"error": "No earlier file found"}
            )

    except ClientError:
        return JSONResponse(status_code=500, content={"error": "MinIO access error"})
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return JSONResponse(status_code=500, content={"error": "Server error"})



if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI video API service...")
    uvicorn.run(app, host="0.0.0.0", port=APP_PORT)
