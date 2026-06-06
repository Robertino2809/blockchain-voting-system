import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import httpx
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI
from pydantic import BaseModel

from shared.config import settings
from shared.logging_config import setup_logging

logger = setup_logging("monitoring")


# ── Modeli ───────────────────────────────────────────────────
class ServiceHealth(BaseModel):
    service: str
    url: str
    status: str
    response_time_ms: float
    details: dict = {}
    checked_at: str = ""


class SystemHealth(BaseModel):
    overall: str
    services: list[ServiceHealth]
    checked_at: str
    total: int
    online: int
    offline: int


# ── Poznati servisi ──────────────────────────────────────────
SERVICES = [
    {"service": "voting_service",    "url": "http://localhost:8001"},
    {"service": "result_aggregator", "url": "http://localhost:8002", "health_path": "/health"},
    {"service": "peer_discovery",    "url": "http://localhost:8003", "health_path": "/health"},
]

_health_history: list[SystemHealth] = []


# ── Health check ─────────────────────────────────────────────
async def check_service(client: httpx.AsyncClient, service: str, url: str) -> ServiceHealth:
    start = asyncio.get_event_loop().time()
    try:
        resp = await client.get(f"{url}/status", timeout=settings.http_timeout)
        elapsed = (asyncio.get_event_loop().time() - start) * 1000

        if resp.status_code == 200:
            return ServiceHealth(
                service=service,
                url=url,
                status="online",
                response_time_ms=round(elapsed, 2),
                details=resp.json(),
                checked_at=datetime.now(timezone.utc).isoformat(),
            )
        else:
            return ServiceHealth(
                service=service,
                url=url,
                status="degraded",
                response_time_ms=round(elapsed, 2),
                checked_at=datetime.now(timezone.utc).isoformat(),
            )
    except Exception as e:
        elapsed = (asyncio.get_event_loop().time() - start) * 1000
        return ServiceHealth(
            service=service,
            url=url,
            status="offline",
            response_time_ms=round(elapsed, 2),
            details={"error": str(e)},
            checked_at=datetime.now(timezone.utc).isoformat(),
        )


async def check_all_services() -> SystemHealth:
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(*[
            check_service(client, s["service"], s["url"])
            for s in SERVICES
        ])

    online = sum(1 for r in results if r.status == "online")
    offline = sum(1 for r in results if r.status == "offline")
    overall = "healthy" if offline == 0 else ("degraded" if online > 0 else "down")

    health = SystemHealth(
        overall=overall,
        services=list(results),
        checked_at=datetime.now(timezone.utc).isoformat(),
        total=len(results),
        online=online,
        offline=offline,
    )

    # Čuvaj zadnjih 20 provjera
    _health_history.append(health)
    if len(_health_history) > 20:
        _health_history.pop(0)

    return health


# ── Monitoring loop ──────────────────────────────────────────
async def monitoring_loop():
    """Svakih 10 sekundi provjeri health svih servisa."""
    while True:
        await asyncio.sleep(10)
        health = await check_all_services()
        logger.info(
            f"Health check | overall={health.overall} | "
            f"online={health.online}/{health.total}"
        )
        for s in health.services:
            if s.status != "online":
                logger.warning(f"Servis {s.service} je {s.status}!")


# ── Lifespan ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(monitoring_loop())
    logger.info("Monitoring Service pokrenut")
    yield
    task.cancel()
    logger.info("Monitoring Service ugašen")


# ── FastAPI app ───────────────────────────────────────────────
app = FastAPI(
    title="Monitoring Service",
    description="Health monitoring svih mikroservisa u sustavu",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Endpointi ────────────────────────────────────────────────
@app.get("/health", response_model=SystemHealth)
async def get_health():
    """Trenutni health status svih servisa."""
    return await check_all_services()


@app.get("/health/history")
async def get_health_history():
    """Zadnjih 20 health checkova."""
    return {
        "history": _health_history,
        "count": len(_health_history),
    }


@app.get("/health/{service_name}")
async def get_service_health(service_name: str):
    """Health status jednog servisa."""
    service = next((s for s in SERVICES if s["service"] == service_name), None)
    if not service:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail=f"Servis '{service_name}' nije poznat.")

    async with httpx.AsyncClient() as client:
        return await check_service(client, service["service"], service["url"])


@app.get("/status")
async def status():
    return {
        "service": "monitoring",
        "status": "online",
        "watching": [s["service"] for s in SERVICES],
    }