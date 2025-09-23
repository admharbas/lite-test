import os, uuid, json, datetime as dt, requests
from functools import wraps
from pathlib import Path

def _post_event(evt: dict):
    url = os.getenv("OPENLINEAGE_URL", "http://localhost:8000/api/v1/lineage")
    headers = {"Content-Type": "application/json"}
    # If/when you add auth: headers["Authorization"] = f"Bearer {os.getenv('OPENLINEAGE_API_KEY')}"
    requests.post(url, headers=headers, data=json.dumps(evt), timeout=15)

def _mk_event(event_type, run_id, job_name, project, inputs, outputs):
    ns = project  # simple: namespace == project (e.g., "retail")
    to_ds = lambda paths: [{"namespace": ns, "name": p} for p in paths]
    return {
        "eventType": event_type,
        "eventTime": dt.datetime.utcnow().isoformat() + "Z",
        "run": {"runId": run_id},
        "job": {"namespace": ns, "name": job_name},
        "inputs": to_ds(inputs),
        "outputs": to_ds(outputs),
        "producer": "https://openlineage.io/python",
        "facets": {"source": {"mode": "ci" if os.getenv("CI") == "true" else "local"}},
    }

def with_lineage(job_name: str, inputs: list[str], outputs: list[str]):
    """Emit START/COMPLETE/FAIL when EMIT_LINEAGE=1."""
    def deco(fn):
        @wraps(fn)
        def run(*args, **kwargs):
            emit = os.getenv("EMIT_LINEAGE") == "1"
            project = os.getenv("PROJECT", "retail")
            run_id = str(uuid.uuid4())
            if emit: _post_event(_mk_event("START", run_id, job_name, project, inputs, outputs))
            try:
                return fn(*args, **kwargs)
            except Exception:
                if emit: _post_event(_mk_event("FAIL", run_id, job_name, project, inputs, outputs))
                raise
            finally:
                if emit: _post_event(_mk_event("COMPLETE", run_id, job_name, project, inputs, outputs))
        return run
    return deco

def file_uri(p: str | Path) -> str:
    return Path(p).resolve().as_uri()