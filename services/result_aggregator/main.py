import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from shared.config import settings
from shared.logging_config import setup_logging

logger = setup_logging("result_aggregator")

# ── In-memory cache rezultata ────────────────────────────────
_cache: dict = {}
_known_nodes: list[str] = []


def get_known_nodes() -> list[str]:
    return _known_nodes


# ── Pydantic modeli ──────────────────────────────────────────
class NodeRegistration(BaseModel):
    node_url: str


class AggregatedResults(BaseModel):
    results: dict
    total_votes: int
    blocks_mined: int
    sources: list[str]
    consensus: bool


# ── Agregacija rezultata ─────────────────────────────────────
async def fetch_results_from_nodes() -> AggregatedResults:
    """
    Dohvati rezultate sa svih poznatih čvorova.
    CQRS read model — čita iz više izvora i agregira.
    """
    all_results: list[dict] = []
    sources: list[str] = []
    dead_nodes: list[str] = []

    async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
        for node_url in _known_nodes:
            try:
                resp = await client.get(f"{node_url}/votes/results")
                if resp.status_code == 200:
                    all_results.append(resp.json())
                    sources.append(node_url)
                    logger.info(f"Rezultati dohvaćeni s: {node_url}")
            except Exception:
                logger.warning(f"Čvor nedostupan: {node_url}")
                dead_nodes.append(node_url)

    # Ukloni mrtve čvorove
    for node in dead_nodes:
        _known_nodes.remove(node)

    if not all_results:
        return AggregatedResults(
            results={},
            total_votes=0,
            blocks_mined=0,
            sources=[],
            consensus=False,
        )

    # Agregacija — uzmi rezultate s najviše glasova (najduži lanac)
    best = max(all_results, key=lambda r: r.get("total_votes", 0))

    # Provjeri konsenzus — slažu li se svi čvorovi
    consensus = all(
        r.get("total_votes") == best.get("total_votes")
        for r in all_results
    )

    # Ažuriraj cache
    _cache.update(best)
    _cache["sources"] = sources
    _cache["consensus"] = consensus

    return AggregatedResults(
        results=best.get("results", {}),
        total_votes=best.get("total_votes", 0),
        blocks_mined=best.get("blocks_mined", 0),
        sources=sources,
        consensus=consensus,
    )


# ── Lifespan ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Učitaj seed čvorove iz konfiguracije
    for peer in settings.seed_peers:
        if peer not in _known_nodes:
            _known_nodes.append(peer)
    logger.info(f"Result Aggregator pokrenut | poznati čvorovi: {_known_nodes}")
    yield
    logger.info("Result Aggregator ugašen")


# ── FastAPI app ───────────────────────────────────────────────
app = FastAPI(
    title="Result Aggregator",
    description="CQRS read model — agregira rezultate glasanja s blockchain čvorova",
    version="1.0.0",
    lifespan=lifespan,
)


# ── Endpointi ────────────────────────────────────────────────
@app.get("/results", response_model=AggregatedResults)
async def get_results():
    """Agregirani rezultati svih čvorova."""
    return await fetch_results_from_nodes()


@app.get("/results/cached")
async def get_cached_results():
    """Vrati zadnje keširane rezultate bez kontaktiranja čvorova."""
    if not _cache:
        raise HTTPException(status_code=404, detail="Cache je prazan, pozovi /results prvo.")
    return _cache


@app.post("/nodes", status_code=201)
async def register_node(registration: NodeRegistration):
    """Registriraj blockchain čvor kao izvor podataka."""
    url = registration.node_url.rstrip("/")
    if url not in _known_nodes:
        _known_nodes.append(url)
        logger.info(f"Čvor registriran: {url}")
        return {"message": "Čvor registriran.", "nodes": _known_nodes}
    return {"message": "Čvor već poznat.", "nodes": _known_nodes}


@app.delete("/nodes/{node_url:path}")
async def remove_node(node_url: str):
    """Ukloni čvor iz izvora."""
    if node_url in _known_nodes:
        _known_nodes.remove(node_url)
        return {"message": f"Čvor uklonjen: {node_url}", "nodes": _known_nodes}
    raise HTTPException(status_code=404, detail="Čvor nije pronađen.")


@app.get("/nodes")
async def list_nodes():
    """Lista poznatih blockchain čvorova."""
    return {"nodes": _known_nodes, "count": len(_known_nodes)}


@app.get("/health")
async def health():
    return {
        "status": "online",
        "service": "result_aggregator",
        "known_nodes": len(_known_nodes),
        "cache_populated": bool(_cache),
    }


@app.get("/status")
async def status():
    return {
        "service": "result_aggregator",
        "status": "online",
        "known_nodes": len(_known_nodes),
    }