from google.cloud import storage
import csv
import io
import time
import json
from .cache import redis_client

def merge_outputs(bucket_name: str, job_id: str, total_chunks: int) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    output_path = f"/tmp/merged_{job_id}.csv"
    with open(output_path, 'w', newline='', encoding='utf-8') as outfile:
        writer = None
        for i in range(total_chunks):
            blob = bucket.blob(f"output_chunk_{i}.csv")
            if not blob.exists():
                time.sleep(2)
                i -= 1  # retry
                continue
            data = blob.download_as_string().decode('utf-8')
            reader = csv.DictReader(io.StringIO(data))
            if writer is None:
                writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
                writer.writeheader()
            for row in reader:
                writer.writerow(row)
    
    # Upload merged
    merged_blob = bucket.blob(f"final_result_{job_id}.csv")
    merged_blob.upload_from_filename(output_path)
    
    # Update status
    status = {
        "status": "done",
        "completed_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "final_file": f"gs://{bucket_name}/final_result_{job_id}.csv"
    }
    redis_client.setex(f"job:{job_id}", 86400 * 30, json.dumps(status))
    
    return merged_blob.public_url