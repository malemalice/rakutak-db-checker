from fastapi import FastAPI, HTTPException
from typing import Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

app = FastAPI(title="Database Validator Health Check")

# Store the last execution status
last_execution = {
    "timestamp": None,
    "status": "not_started",
    "details": {}
}

@app.get("/health")
async def health_check() -> Dict[str, Any]:
    """
    Health check endpoint that returns the current status of the validator.
    """
    try:
        return {
            "status": "healthy",
            "last_execution": last_execution,
            "timestamp": datetime.utcnow().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail="Health check failed")

def update_execution_status(status: str, details: Dict[str, Any] = None) -> None:
    """
    Update the last execution status.
    
    Args:
        status (str): Execution status (e.g., "running", "completed", "failed")
        details (Dict[str, Any], optional): Additional execution details
    """
    global last_execution
    last_execution = {
        "timestamp": datetime.utcnow().isoformat(),
        "status": status,
        "details": details or {}
    }
    logger.info(f"Updated execution status: {status}")

def start_server(host: str, port: int) -> None:
    """
    Start the FastAPI server.
    
    Args:
        host (str): Host to bind the server to
        port (int): Port to bind the server to
    """
    import uvicorn
    logger.info(f"Starting health check server on {host}:{port}")
    uvicorn.run(app, host=host, port=port) 