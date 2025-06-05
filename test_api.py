import pytest
import asyncio
import aiohttp
import time
from fastapi.testclient import TestClient
from main import app, Priority

client = TestClient(app)

@pytest.mark.asyncio
async def test_ingestion_and_status():
    # Test case 1: Single ingestion request
    payload = {"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"}
    response = client.post("/ingest", json=payload)
    assert response.status_code == 200
    ingestion_id = response.json()["ingestion_id"]
    
    # Wait for processing to start
    await asyncio.sleep(1)
    
    # Check status
    response = client.get(f"/status/{ingestion_id}")
    assert response.status_code == 200
    status_data = response.json()
    assert status_data["ingestion_id"] == ingestion_id
    assert status_data["status"] in ["yet_to_start", "triggered", "completed"]
    assert len(status_data["batches"]) == 2
    assert status_data["batches"][0]["ids"] == [1, 2, 3]
    assert status_data["batches"][1]["ids"] == [4, 5]

@pytest.mark.asyncio
async def test_priority_and_rate_limit():
    # Test case 2: Priority and rate limit
    t0 = time.time()
    
    # Request 1: Medium priority
    payload1 = {"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"}
    response1 = client.post("/ingest", json=payload1)
    ingestion_id1 = response1.json()["ingestion_id"]
    
    # Wait 4 seconds
    await asyncio.sleep(4)
    
    # Request 2: High priority
    payload2 = {"ids": [6, 7, 8, 9], "priority": "HIGH"}
    response2 = client.post("/ingest", json=payload2)
    ingestion_id2 = response2.json()["ingestion_id"]
    
    # Wait for processing
    await asyncio.sleep(15)
    
    # Check status for request 1
    response1 = client.get(f"/status/{ingestion_id1}")
    status_data1 = response1.json()
    
    # Check status for request 2
    response2 = client.get(f"/status/{ingestion_id2}")
    status_data2 = response2.json()
    
    # Verify processing order (HIGH priority processed first)
    assert status_data2["batches"][0]["status"] == "completed"  # [6, 7, 8]
    assert status_data2["batches"][1]["status"] == "completed"  # [9]
    assert status_data1["batches"][0]["status"] == "completed"  # [1, 2, 3]
    assert status_data1["batches"][1]["status"] in ["completed", "triggered"]  # [4, 5]

@pytest.mark.asyncio
async def test_invalid_input():
    # Test case 3: Invalid IDs
    payload = {"ids": [0, 2, 3], "priority": "MEDIUM"}
    response = client.post("/ingest", json=payload)
    assert response.status_code == 400
    
    payload = {"ids": [1, 2, 10**9 + 8], "priority": "MEDIUM"}
    response = client.post("/ingest", json=payload)
    assert response.status_code == 400
    
    # Test case 4: Invalid ingestion ID
    response = client.get("/status/invalid_id")
    assert response.status_code == 404
