from pydantic import BaseModel
from typing import Optional, List

class UploadResponse(BaseModel):
    job_id: str
    message: str
    input_file: str

class JobStatus(BaseModel):
    job_id: str
    status: str  # pending, splitting, verifying, merging, done, failed
    uploaded_at: str
    started_at: Optional[str]
    completed_at: Optional[str]
    total_chunks: Optional[int]
    processed_chunks: Optional[int]
    stats: Optional[dict]