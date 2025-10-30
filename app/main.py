from fastapi import FastAPI, File, UploadFile, HTTPException, BackgroundTasks
import uuid
import time
import requests
from google.cloud import storage
from .schemas import UploadResponse, JobStatus
from .cache import set_job_status, get_job_status, set_total_chunks, redis_client, get_job_stats
from .merger import merge_outputs, scale_mig_down
from fastapi.responses import RedirectResponse
import datetime
import os
from google.oauth2 import service_account
from googleapiclient import discovery

app = FastAPI(title="Email Verifier API", version="1.0")

BUCKET_NAME = os.getenv("GCS_BUCKET", "email-verifier-bucket")
SPLITTER_URL = os.getenv("SPLITTER_URL")  # e.g., Cloud Function URL

client = storage.Client()
bucket = client.bucket(BUCKET_NAME)
compute = None

def get_compute_client():
    global compute
    if compute is None:
        credentials = service_account.Credentials.from_service_account_file(
            os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        )
        compute = discovery.build('compute', 'v1', credentials=credentials)
    return compute
def scale_mig_up(job_id: str, desired_count: int = 100):
    try:
        client = get_compute_client()
        client.instanceGroupManagers().resize(
            project='email-verifier-475805',
            zone='us-central1-a',
            instanceGroupManager='verifier-mig',
            size=desired_count
        ).execute()
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
    current_status = get_job_status(job_id)
    if not current_status:
        current_status = {}
    current_status.update({
            "status": "verifying",
            "started_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "total_chunks": total_chunks
        })
    set_job_status(job_id, current_status)
    
    # Poll until all chunks processed
    while True:
        processed = 0
        for i in range(total_chunks):
            blob = bucket.blob(f"{job_id}/output_chunk_{i}.csv")
            if blob.exists():
                processed += 1
        status_data = get_job_status(job_id)
        if status_data:
            if status_data.get("status") not in ["done", "failed"]:
                status_data['processed_chunks'] = processed
                set_job_status(job_id, status_data)
        if processed == total_chunks:
            print(f"All {total_chunks} chunks processed for {job_id}. Merging...")
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

    # --- START: MODIFIED (REQ 4) ---
    total = data.get("total_chunks") or 0
    processed = data.get("processed_chunks") or 0 # Get from stored status
    
    # If job is not done, poll GCS for a more up-to-date 'processed' count
    if data.get("status") not in ["done", "failed", "merging"] and total > 0:
        processed_gcs = 0
        for i in range(total):
            if bucket.blob(f"{job_id}/output_chunk_{i}.csv").exists():
                processed_gcs += 1
        processed = processed_gcs # Use live GCS count

    # Get aggregated stats from Redis
    live_stats = get_job_stats(job_id)
    
    # If job is 'done', stats should already be in 'data'. If not, use live_stats.
    stats_to_show = data.get("stats") or live_stats
    # --- END: MODIFIED ---

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
        "stats": stats_to_show, # <-- MODIFIED
        "final_file": data.get("final_file")
    }

    return response

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
@app.get("/results")
def list_results():
    """Lists all successfully merged final_result.csv files."""
    prefixes = set()
    # List all "directories" (job_id prefixes)
    for blob in bucket.list_blobs(delimiter='/'):
        if blob.prefix:
            prefixes.add(blob.prefix)
    
    final_files = []
    for prefix in prefixes:
        job_id = prefix.strip('/')
        final_blob_name = f"{job_id}/final_result.csv"
        final_blob = bucket.blob(final_blob_name)
        
        if final_blob.exists():
            try:
                final_blob.reload() # Get metadata (size, created time)
                final_files.append({
                    "job_id": job_id,
                    "file": f"gs://{BUCKET_NAME}/{final_blob.name}",
                    "size_mb": round(final_blob.size / (1024*1024), 2) if final_blob.size else 0,
                    "created": final_blob.time_created
                })
            except Exception as e:
                print(f"Error checking blob {final_blob_name}: {e}")

    return sorted(final_files, key=lambda x: x['created'], reverse=True)


@app.get("/download/{job_id}")
def download_result(job_id: str):
    """Generates a temporary signed URL to download a final_result.csv."""
    blob_name = f"{job_id}/final_result.csv"
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        raise HTTPException(404, "Final result file not found for this job ID.")
    
    try:
        # Create a signed URL valid for 15 minutes
        url = blob.generate_signed_url(
            version="v4",
            expiration=datetime.timedelta(minutes=15),
            method="GET",
        )
        # Redirect the user directly to the download
        return RedirectResponse(url=url)
    except Exception as e:
        print(f"Error generating signed URL for {job_id}: {e}")
        raise HTTPException(500, "Could not generate download link.")