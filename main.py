# import asyncio
# import uuid
# from enum import Enum
# from typing import List, Dict
# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# import heapq
# import time
# from contextlib import asynccontextmanager
# import logging

# # Set up logging
# logging.basicConfig(level=logging.DEBUG)
# logger = logging.getLogger(__name__)

# app = FastAPI()

# # Enum for priority levels
# class Priority(str, Enum):
#     HIGH = "HIGH"
#     MEDIUM = "MEDIUM"
#     LOW = "LOW"

# # Pydantic model for ingestion request
# class IngestionRequest(BaseModel):
#     ids: List[int]
#     priority: Priority = Priority.MEDIUM

# # Data structures
# ingestion_jobs: Dict[str, dict] = {}
# batch_statuses: Dict[str, dict] = {}
# priority_queue = []
# lock = asyncio.Lock()

# # Priority value mapping
# priority_values = {Priority.HIGH: 0, Priority.MEDIUM: 1, Priority.LOW: 2}

# # Simulate external API call
# async def simulate_external_api(id: int) -> dict:
#     logger.debug(f"Processing ID {id}")
#     await asyncio.sleep(1)
#     return {"id": id, "data": "processed"}

# # Process a batch of IDs
# async def process_batch(batch_id: str, ids: List[int]):
#     logger.info(f"Starting batch: batch_id={batch_id}, ids={ids}")
#     async with lock:
#         batch_statuses[batch_id]["status"] = "triggered"
#         update_ingestion_status(batch_statuses[batch_id]["ingestion_id"])
    
#     for id in ids:
#         await simulate_external_api(id)
    
#     async with lock:
#         batch_statuses[batch_id]["status"] = "completed"
#         update_ingestion_status(batch_statuses[batch_id]["ingestion_id"])
    
#     logger.debug("Waiting 5s rate limit")
#     await asyncio.sleep(5)

# # Update ingestion status
# def update_ingestion_status(ingestion_id: str):
#     batches = [b for b in batch_statuses.values() if b["ingestion_id"] == ingestion_id]
#     statuses = [b["status"] for b in batches]
    
#     if all(s == "yet_to_start" for s in statuses):
#         ingestion_jobs[ingestion_id]["status"] = "yet_to_start"
#     elif all(s == "completed" for s in statuses):
#         ingestion_jobs[ingestion_id]["status"] = "completed"
#     else:
#         ingestion_jobs[ingestion_id]["status"] = "triggered"

# # Background task to process batches
# async def process_queue():
#     logger.info("Starting process_queue task")
#     while True:
#         try:
#             async with lock:
#                 if not priority_queue:
#                     logger.debug("Queue empty, sleeping 0.1s")
#                     await asyncio.sleep(0.1)
#                     continue
#                 priority_value, created_time, batch_id, ids = heapq.heappop(priority_queue)
#                 logger.info(f"Popped: priority={priority_value}, batch_id={batch_id}, ids={ids}, time={created_time}")
            
#             await process_batch(batch_id, ids)
#         except Exception as e:
#             logger.error(f"Error in process_queue: {e}")
#             await asyncio.sleep(1)

# # Lifespan handler
# @asynccontextmanager
# async def lifespan(app: FastAPI):
#     logger.info("Starting API")
#     task = asyncio.create_task(process_queue())
#     yield
#     logger.info("Shutting down API")
#     task.cancel()
#     try:
#         await task
#     except asyncio.CancelledError:
#         logger.info("Process queue task cancelled")

# app = FastAPI(title="Data Ingestion API", lifespan=lifespan)

# # Ingestion endpoint
# @app.post("/ingest")
# async def ingest_data(request: IngestionRequest):
#     if not request.ids or any(id < 1 or id > 10**9 + 7 for id in request.ids):
#         logger.error("Invalid IDs provided")
#         raise HTTPException(status_code=400, detail="Invalid IDs")
    
#     ingestion_id = str(uuid.uuid4())
#     created_time = time.time()
#     batches = [request.ids[i:i+3] for i in range(0, len(request.ids), 3)]
    
#     logger.info(f"New ingestion: id={ingestion_id}, priority={request.priority}, batches={len(batches)}")
    
#     async with lock:
#         ingestion_jobs[ingestion_id] = {
#             "status": "yet_to_start",
#             "created_time": created_time,
#             "priority": request.priority
#         }
#         for ids in batches:
#             batch_id = str(uuid.uuid4())
#             batch_statuses[batch_id] = {
#                 "ingestion_id": ingestion_id,
#                 "ids": ids,
#                 "status": "yet_to_start"
#             }
#             heapq.heappush(
#                 priority_queue,
#                 (priority_values[request.priority], created_time, batch_id, ids)
#             )
#             logger.debug(f"Pushed: priority={priority_values[request.priority]}, batch_id={batch_id}, ids={ids}")
    
#     return {"ingestion_id": ingestion_id}

# # Status endpoint
# @app.get("/status/{ingestion_id}")
# async def get_status(ingestion_id: str):
#     if ingestion_id not in ingestion_jobs:
#         logger.error(f"Ingestion ID not found: {ingestion_id}")
#         raise HTTPException(status_code=404, detail="Ingestion ID not found")
    
#     batches = [
#         {
#             "batch_id": batch_id,
#             "ids": batch["ids"],
#             "status": batch["status"]
#         }
#         for batch_id, batch in batch_statuses.items()
#         if batch["ingestion_id"] == ingestion_id
#     ]
    
#     logger.debug(f"Status for {ingestion_id}: status={ingestion_jobs[ingestion_id]['status']}, batches={len(batches)}")
    
#     return {
#         "ingestion_id": ingestion_id,
#         "status": ingestion_jobs[ingestion_id]["status"],
#         "batches": batches
#     }

import asyncio
import uuid
from enum import Enum
from typing import List, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import heapq
import time
from contextlib import asynccontextmanager
import logging

# Set up logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = FastAPI()

# Enum for priority levels
class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

# Pydantic model for ingestion request
class IngestionRequest(BaseModel):
    ids: List[int]
    priority: Priority = Priority.MEDIUM

# Data structures
ingestion_jobs: Dict[str, dict] = {}
batch_statuses: Dict[str, dict] = {}
priority_queue = []
lock = asyncio.Lock()

# Priority value mapping
priority_values = {Priority.HIGH: 0, Priority.MEDIUM: 1, Priority.LOW: 2}

# Simulate external API call
async def simulate_external_api(id: int) -> dict:
    logger.debug(f"Processing ID {id}")
    await asyncio.sleep(1)
    return {"id": id, "data": "processed"}

# Process a batch of IDs
async def process_batch(batch_id: str, ids: List[int]):
    logger.info(f"Starting batch: batch_id={batch_id}, ids={ids}")
    async with lock:
        batch_statuses[batch_id]["status"] = "triggered"
        update_ingestion_status(batch_statuses[batch_id]["ingestion_id"])
    
    for id in ids:
        await simulate_external_api(id)
    
    async with lock:
        batch_statuses[batch_id]["status"] = "completed"
        update_ingestion_status(batch_statuses[batch_id]["ingestion_id"])
    
    logger.debug("Waiting 5s rate limit")
    await asyncio.sleep(5)

# Update ingestion status
def update_ingestion_status(ingestion_id: str):
    batches = [b for b in batch_statuses.values() if b["ingestion_id"] == ingestion_id]
    statuses = [b["status"] for b in batches]
    
    if all(s == "yet_to_start" for s in statuses):
        ingestion_jobs[ingestion_id]["status"] = "yet_to_start"
    elif all(s == "completed" for s in statuses):
        ingestion_jobs[ingestion_id]["status"] = "completed"
    else:
        ingestion_jobs[ingestion_id]["status"] = "triggered"

# Background task to process batches
async def process_queue():
    logger.info("Starting process_queue task")
    while True:
        try:
            async with lock:
                if not priority_queue:
                    logger.debug("Queue empty, sleeping 0.1s")
                    await asyncio.sleep(0.1)
                    continue
                priority_value, created_time, batch_id, ids = heapq.heappop(priority_queue)
                logger.info(f"Popped: priority={priority_value}, batch_id={batch_id}, ids={ids}, time={created_time}")
            
            await process_batch(batch_id, ids)
        except Exception as e:
            logger.error(f"Error in process_queue: {e}")
            await asyncio.sleep(1)

# Lifespan handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting API")
    task = asyncio.create_task(process_queue())
    yield
    logger.info("Shutting down API")
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        logger.info("Process queue task cancelled")

app = FastAPI(title="Data Ingestion API", lifespan=lifespan)

# Ingestion endpoint
@app.post("/ingest")
async def ingest_data(request: IngestionRequest):
    if not request.ids or any(id < 1 or id > 10**9 + 7 for id in request.ids):
        logger.error("Invalid IDs provided")
        raise HTTPException(status_code=400, detail="Invalid IDs")
    
    ingestion_id = str(uuid.uuid4())
    created_time = time.time()
    batches = [request.ids[i:i+3] for i in range(0, len(request.ids), 3)]
    
    logger.info(f"New ingestion: id={ingestion_id}, priority={request.priority}, batches={len(batches)}")
    
    async with lock:
        ingestion_jobs[ingestion_id] = {
            "status": "yet_to_start",
            "created_time": created_time,
            "priority": request.priority
        }
        for ids in batches:
            batch_id = str(uuid.uuid4())
            batch_statuses[batch_id] = {
                "ingestion_id": ingestion_id,
                "ids": ids,
                "status": "yet_to_start"
            }
            heapq.heappush(
                priority_queue,
                (priority_values[request.priority], created_time, batch_id, ids)
            )
            logger.debug(f"Pushed: priority={priority_values[request.priority]}, batch_id={batch_id}, ids={ids}")
    
    return {"ingestion_id": ingestion_id}

# Status endpoint
@app.get("/status/{ingestion_id}")
async def get_status(ingestion_id: str):
    if ingestion_id not in ingestion_jobs:
        logger.error(f"Ingestion ID not found: {ingestion_id}")
        raise HTTPException(status_code=404, detail="Ingestion ID not found")
    
    batches = [
        {
            "batch_id": batch_id,
            "ids": batch["ids"],
            "status": batch["status"]
        }
        for batch_id, batch in batch_statuses.items()
        if batch["ingestion_id"] == ingestion_id
    ]
    
    logger.debug(f"Status for {ingestion_id}: status={ingestion_jobs[ingestion_id]['status']}, batches={len(batches)}")
    
    return {
        "ingestion_id": ingestion_id,
        "status": ingestion_jobs[ingestion_id]["status"],
        "batches": batches
    }
    