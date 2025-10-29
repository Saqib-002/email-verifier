from pydantic import BaseModel
from typing import Dict, Optional, List

class UploadResponse(BaseModel):
    job_id: str
    message: str
    input_file: str

class JobStatus(BaseModel):
    job_id: str
    status: str  # pending, splitting, verifying, merging, done, failed
    uploaded_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    total_chunks: Optional[int] = None
    processed_chunks: Optional[int] = None
    stats: Optional[Dict] = None
    progress: Optional[str] = None
    final_file: Optional[str] = None

    class Config:
        extra = "ignore"