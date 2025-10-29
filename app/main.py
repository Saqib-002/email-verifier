from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn
import uuid
import time
import requests
import subprocess
from google.cloud import storage
from .schemas import UploadResponse, JobStatus
from .cache import set_job_status, get_job_status, set_total_chunks,redis_client
from .merger import merge_outputs
import os

app = FastAPI(title="Email Verifier API", version="1.0")

BUCKET_NAME = os.getenv("GCS_BUCKET", "email-verifier-bucket")
SPLITTER_URL = os.getenv("SPLITTER_URL")  # e.g., Cloud Function URL

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)

def scale_mig_up(job_id: str, desired_count: int = 100):
    try:
        subprocess.run([
            "gcloud", "compute", "instance-groups", "managed", "resize",
            "verifier-mig", "--size", str(desired_count),
            "--zone", "us-central1-a", "--quiet"
        ], check=True)
        print(f"Scaled MIG to {desired_count} for job {job_id}")
    except Exception as e:
        print(f"Scale-up failed: {e}")
def monitor_splitter_completion(job_id: str, num_chunks: int):
    bucket = storage.Client().bucket(BUCKET_NAME)
    while True:
        chunks = list(bucket.list_blobs(prefix=f"{job_id}/input_chunk_"))
        if len(chunks) == num_chunks:
            scale_mig_up(job_id, min(num_chunks, 100))
            break
        time.sleep(5)
def trigger_splitter(job_id: str, input_object: str, num_chunks: int):
    payload = {
        "num_chunks": num_chunks,
        "input_object": input_object,
        "bucket_name": BUCKET_NAME,
        "job_id": job_id
    }
    try:
        requests.post(SPLITTER_URL, json=payload, timeout=30)
    except:
        pass  # Fail silently; monitor via GCS

def monitor_and_merge(job_id: str, total_chunks: int):
    time.sleep(60)  # Let splitter run
    set_total_chunks(job_id, total_chunks)
    set_job_status(job_id, {
    **get_job_status(job_id),
        "status": "verifying",
        "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "total_chunks": total_chunks
    })
    
    # Poll until all chunks processed
    while True:
        processed = 0
        for i in range(total_chunks):
            blob = bucket.blob(f"output_chunk_{i}.csv")
            if blob.exists():
                processed += 1
        if processed == total_chunks:
            merge_outputs(BUCKET_NAME, job_id, total_chunks)
            break
        time.sleep(10)

@app.post("/upload", response_model=UploadResponse)
async def upload_file(file: UploadFile = File(...), chunks: int = 50, background: BackgroundTasks = None):
    if not file.filename.endswith('.csv'):
        raise HTTPException(400, "Only CSV files allowed")
    
    job_id = str(uuid.uuid4())[:8]
    input_object = f"full_input_{job_id}.csv"
    
    # Save to GCS
    contents = await file.read()
    blob = bucket.blob(input_object)
    blob.upload_from_string(contents, content_type="text/csv")
    
    # Init job
    status = {
        "status": "splitting",
        "uploaded_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "input_file": f"gs://{BUCKET_NAME}/{input_object}",
        "started_at": None,
        "completed_at": None,
        "total_chunks": None,
        "processed_chunks": None,
        "stats": None,
        "final_file": None
    }
    set_job_status(job_id, status)
    
    # Trigger splitter
    background.add_task(trigger_splitter, job_id, input_object, chunks)
    background.add_task(monitor_splitter_completion, job_id, chunks)
    background.add_task(monitor_and_merge, job_id, chunks)
    
    return {"job_id": job_id, "message": "Upload successful", "input_file": status["input_file"]}

@app.get("/status/{job_id}", response_model=JobStatus)
def get_status(job_id: str):
    data = get_job_status(job_id)
    if not data:
        raise HTTPException(404, "Job not found")

    # Enrich with live GCS stats
    total = int(redis_client.get(f"job:{job_id}:total") or 0)
    processed = 0
    for i in range(total):
        if bucket.blob(f"output_chunk_{i}.csv").exists():
            processed += 1

    # Build response with defaults
    response = {
        "job_id": job_id,
        "status": data.get("status", "unknown"),
        "uploaded_at": data.get("uploaded_at", ""),
        "started_at": data.get("started_at"),
        "completed_at": data.get("completed_at"),
        "total_chunks": total,
        "processed_chunks": processed,
        "progress": f"{processed}/{total}" if total else "0/0",
        "stats": data.get("stats"),
        "final_file": data.get("final_file")
    }

    return response

@app.get("/jobs")
def list_jobs():
    keys = redis_client.keys("job:*")
    jobs = []
    for key in keys:
        job_id = key.split(":")[1]
        status = get_job_status(job_id)
        if status:
            jobs.append(status)
    return sorted(jobs, key=lambda x: x.get("uploaded_at", ""), reverse=True)[:50]