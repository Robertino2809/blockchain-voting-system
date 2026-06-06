import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import httpx
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from shared.config import settings
from shared.logging_config import setup_logging

logger = setup_logging("api_gateway")

# ── Servisne adrese ──────────────────────────────────────────
VOTING_SERVICE    = "http://localhost:8001"
RESULT_AGGREGATOR = "http://localhost:8002"
PEER_DISCOVERY    = "http://localhost:8003"
MONITORING        = "http://localhost:8004"


# ── Pydantic modeli ──────────────────────────────────────────
class VoteRequest(BaseModel):
    voter_id: str
    candidate: str


# ── HTTP klijent ─────────────────────────────────────────────
async def forward(method: str, url: str, **kwargs) -> JSONResponse:
    """Proslijedi zahtjev na mikroservis i vrati odgovor."""
    try:
        async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
            resp = await client.request(method, url, **kwargs)
            return JSONResponse(
                content=resp.json(),
                status_code=resp.status_code,
            )
    except httpx.ConnectError:
        logger.error(f"Servis nedostupan: {url}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Servis nedostupan: {url}",
        )
    except Exception as e:
        logger.error(f"Greška pri prosljeđivanju na {url}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Interna greška gateway-a.",
        )


# ── Lifespan ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"API Gateway pokrenut na portu {settings.api_gateway_port}")
    yield
    logger.info("API Gateway ugašen")


# ── FastAPI app ───────────────────────────────────────────────
app = FastAPI(
    title="API Gateway",
    description="Ulazna točka distribuiranog blockchain voting sustava",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Glasanje ─────────────────────────────────────────────────
@app.post("/votes", status_code=201)
async def cast_vote(request: VoteRequest):
    """Proslijedi glas na Voting Service."""
    logger.info(f"Gateway prima glas: {request.voter_id} → {request.candidate}")
    return await forward("POST", f"{VOTING_SERVICE}/votes", json=request.model_dump())


@app.post("/mine")
async def mine_block():
    """Pokreni rudarenje novog bloka."""
    return await forward("POST", f"{VOTING_SERVICE}/mine")


@app.get("/votes/pending")
async def get_pending():
    """Pending glasovi s Voting Servicea."""
    return await forward("GET", f"{VOTING_SERVICE}/votes/pending")


# ── Rezultati ─────────────────────────────────────────────────
@app.get("/results")
async def get_results():
    """Agregirani rezultati s Result Aggregatora."""
    return await forward("GET", f"{RESULT_AGGREGATOR}/results")


@app.get("/results/cached")
async def get_cached_results():
    """Keširani rezultati."""
    return await forward("GET", f"{RESULT_AGGREGATOR}/results/cached")


# ── Blockchain ────────────────────────────────────────────────
@app.get("/chain")
async def get_chain():
    """Cijeli blockchain s Voting Servicea."""
    return await forward("GET", f"{VOTING_SERVICE}/chain") if hasattr(forward, "chain") \
        else await forward("GET", f"{VOTING_SERVICE}/votes/results")


# ── Mreža čvorova ─────────────────────────────────────────────
@app.get("/nodes")
async def get_nodes():
    """Lista svih čvorova iz Peer Discovery."""
    return await forward("GET", f"{PEER_DISCOVERY}/nodes")


@app.get("/nodes/leader")
async def get_leader():
    """Trenutni Raft leader."""
    return await forward("GET", f"{PEER_DISCOVERY}/nodes/leader/current")


# ── Monitoring ────────────────────────────────────────────────
@app.get("/health")
async def get_health():
    """Health status cijelog sustava."""
    return await forward("GET", f"{MONITORING}/health")


# ── Status gatewaya ───────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service": "api_gateway",
        "status": "online",
        "version": "1.0.0",
        "endpoints": {
            "votes":    "POST /votes",
            "mine":     "POST /mine",
            "results":  "GET  /results",
            "nodes":    "GET  /nodes",
            "leader":   "GET  /nodes/leader",
            "health":   "GET  /health",
            "docs":     "/docs",
        }
    }