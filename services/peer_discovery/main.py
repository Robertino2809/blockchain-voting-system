import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import httpx
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from shared.config import settings
from shared.logging_config import setup_logging

logger = setup_logging("peer_discovery")


# ── Registry čvorova ─────────────────────────────────────────
class NodeInfo(BaseModel):
    node_id: str
    node_url: str
    registered_at: str = ""
    last_seen: str = ""
    status: str = "unknown"
    raft_state: str = "unknown"
    chain_length: int = 0


_registry: dict[str, NodeInfo] = {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Health check loop ────────────────────────────────────────
async def health_check_loop():
    """
    Svakih 5 sekundi pinga sve registrirane čvorove.
    Ažurira status i detektira padove — heartbeat mehanizam.
    """
    while True:
        await asyncio.sleep(5)
        async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
            for node_id, info in list(_registry.items()):
                try:
                    resp = await client.get(f"{info.node_url}/status")
                    if resp.status_code == 200:
                        data = resp.json()
                        _registry[node_id] = NodeInfo(
                            node_id=node_id,
                            node_url=info.node_url,
                            registered_at=info.registered_at,
                            last_seen=now_iso(),
                            status="online",
                            raft_state=data.get("raft_state", "unknown"),
                            chain_length=data.get("chain_length", 0),
                        )
                        logger.info(f"Heartbeat OK: {node_id} | {data.get('raft_state')} | chain={data.get('chain_length')}")
                    else:
                        _registry[node_id].status = "degraded"
                        logger.warning(f"Čvor degradiran: {node_id}")
                except Exception:
                    _registry[node_id].status = "offline"
                    logger.warning(f"Čvor offline: {node_id}")


# ── Lifespan ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(health_check_loop())
    logger.info("Peer Discovery Service pokrenut")
    yield
    task.cancel()
    logger.info("Peer Discovery Service ugašen")


# ── FastAPI app ───────────────────────────────────────────────
app = FastAPI(
    title="Peer Discovery Service",
    description="Registry i health monitoring svih čvorova u mreži",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Endpointi ────────────────────────────────────────────────
@app.post("/nodes", status_code=201)
async def register_node(info: NodeInfo):
    """Registriraj novi čvor u mrežu."""
    info.registered_at = now_iso()
    info.last_seen = now_iso()
    info.status = "online"
    _registry[info.node_id] = info
    logger.info(f"Čvor registriran: {info.node_id} @ {info.node_url}")
    return {"message": "Čvor registriran.", "node": info}


@app.get("/nodes")
async def list_nodes():
    """Vrati sve registrirane čvorove."""
    return {
        "nodes": list(_registry.values()),
        "count": len(_registry),
        "online": sum(1 for n in _registry.values() if n.status == "online"),
        "offline": sum(1 for n in _registry.values() if n.status == "offline"),
    }


@app.get("/nodes/{node_id}")
async def get_node(node_id: str):
    """Dohvati informacije o specifičnom čvoru."""
    if node_id not in _registry:
        raise HTTPException(status_code=404, detail=f"Čvor '{node_id}' nije pronađen.")
    return _registry[node_id]


@app.delete("/nodes/{node_id}")
async def deregister_node(node_id: str):
    """Odjavi čvor iz mreže."""
    if node_id not in _registry:
        raise HTTPException(status_code=404, detail=f"Čvor '{node_id}' nije pronađen.")
    del _registry[node_id]
    logger.info(f"Čvor odjavljen: {node_id}")
    return {"message": f"Čvor '{node_id}' odjavljen."}


@app.get("/nodes/leader/current")
async def get_current_leader():
    """Vrati trenutnog Raft leadera."""
    leaders = [n for n in _registry.values() if n.raft_state == "leader" and n.status == "online"]
    if not leaders:
        raise HTTPException(status_code=404, detail="Nema aktivnog leadera.")
    return {"leader": leaders[0]}


@app.get("/health")
async def health():
    return {
        "status": "online",
        "service": "peer_discovery",
        "registered_nodes": len(_registry),
    }


@app.get("/status")
async def status():
    return {
        "service": "peer_discovery",
        "status": "online",
        "registered_nodes": len(_registry),
    }