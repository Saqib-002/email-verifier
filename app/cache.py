import redis
import json
import os

redis_client = redis.Redis(
    host=os.getenv("REDIS_HOST", "redis"),
    port=6379,
    db=0,
    decode_responses=True
)

def set_job_status(job_id: str, data: dict):
    redis_client.setex(f"job:{job_id}", 86400 * 7, json.dumps(data))  # 7 days

def get_job_status(job_id: str):
    data = redis_client.get(f"job:{job_id}")
    return json.loads(data) if data else None

def incr_chunk_processed(job_id: str):
    return redis_client.incr(f"job:{job_id}:processed")

def set_total_chunks(job_id: str, count: int):
    redis_client.set(f"job:{job_id}:total", count, ex=86400 * 7)
def get_job_stats(job_id: str):
    """Get aggregated stats hash from Redis."""
    stats_key = f"job:{job_id}:stats"
    stats_raw = redis_client.hgetall(stats_key)
    if not stats_raw:
        return None
    return {k: int(v) for k, v in stats_raw.items()}