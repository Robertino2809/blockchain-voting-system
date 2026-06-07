import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
import httpx
import asyncio
from pydantic import BaseModel, Field

from blockchain.node.blockchain import Blockchain
from blockchain.storage.store import BlockchainStore
from blockchain.consensus.raft import RaftNode, RaftState
from blockchain.consensus.raft_server import router as raft_router, RaftRunner, set_raft_instance
from shared.config import settings
from shared.logging_config import setup_logging

logger = setup_logging("voting_service")

# ── Globalno stanje čvora ────────────────────────────────────
blockchain = Blockchain()
store = BlockchainStore(blockchain)
raft = RaftNode(
    node_id=settings.node_id,
    peers=settings.seed_peers,
)
raft_runner = RaftRunner(raft)
set_raft_instance(raft)


# ── Raft callbacks ───────────────────────────────────────────
def on_become_leader(node_id: str, term: int):
    logger.info(f"Ovaj čvor je novi LEADER | term={term}")


def on_commit(entry):
    """Kad Raft committa entry — dodaj glas u blockchain."""
    vote = entry.data
    added = blockchain.add_pending_vote(vote)
    if added:
        logger.info(f"Glas committan u blockchain: {vote['voter_id']} → {vote['candidate']}")


raft.on_become_leader(on_become_leader)
raft.on_commit(on_commit)


# ── Blockchain sync ──────────────────────────────────────────
async def sync_blockchain_from_peers():
    """Pri startu — dohvati blockchain od peera s najduljim lancem."""
    if not raft.peers:
        return

    best_chain = None
    best_length = len(blockchain.chain)

    async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
        for peer in raft.peers:
            try:
                resp = await client.get(f"{peer}/blocks/chain")
                data = resp.json()
                if data["length"] > best_length:
                    best_length = data["length"]
                    best_chain = data["chain"]
                    logger.info(f"Bolji lanac pronađen na {peer} — duljina {best_length}")
            except Exception:
                logger.warning(f"Sync nije uspio od {peer}")

    if best_chain:
        from blockchain.node.block import Block
        blockchain.chain = [Block.from_dict(b) for b in best_chain]
        blockchain.pending_votes = []
        store.save()
        logger.info(f"Blockchain sinkroniziran — {len(blockchain.chain)} blokova")


# ── Pydantic modeli ──────────────────────────────────────────
class VoteRequest(BaseModel):
    voter_id: str = Field(..., min_length=1)
    candidate: str = Field(..., min_length=1)


class VoteResponse(BaseModel):
    success: bool
    message: str
    vote: dict | None = None


class MineResponse(BaseModel):
    success: bool
    message: str
    block: dict | None = None


# ── Lifespan ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    loaded = store.load()
    if loaded:
        logger.info(f"Blockchain učitan s diska — {len(blockchain.chain)} blokova")
    else:
        logger.info("Svježi blockchain — kreiran genesis blok")

    node_num = int(settings.node_id.split("-")[-1]) if settings.node_id[-1].isdigit() else 1
    await asyncio.sleep(node_num * 2.0)
    raft_runner.start()
    logger.info(f"Voting Service pokrenut | node_id={settings.node_id} | port={settings.node_port}")
    await asyncio.sleep(2)
    await sync_blockchain_from_peers()

    yield

    # Shutdown
    raft_runner.stop()
    store.save()
    logger.info("Voting Service ugašen — blockchain spremljen")


# ── FastAPI app ───────────────────────────────────────────────
app = FastAPI(
    title="Voting Service",
    description="Mikroservis za validaciju i obradu glasova",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(raft_router, tags=["Raft Interni"])


# ── Endpointi ────────────────────────────────────────────────
@app.post("/votes", response_model=VoteResponse, status_code=status.HTTP_201_CREATED)
async def cast_vote(request: VoteRequest):
    if raft.state != RaftState.LEADER:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "error": "Ovaj čvor nije leader.",
                "leader": raft.voted_for,
                "hint": "Pošalji zahtjev na leader čvor.",
            }
        )

    # Provjeri i blockchain I pending pool
    if blockchain.has_voted(request.voter_id):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Glasač '{request.voter_id}' je već glasao."
        )

    for v in blockchain.pending_votes:
        if v["voter_id"] == request.voter_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Glasač '{request.voter_id}' već ima pending glas."
            )

    vote = {"voter_id": request.voter_id, "candidate": request.candidate}

    entry = raft.append_entry(vote)
    if not entry:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Greška pri dodavanju u Raft log."
        )

    raft.commit_entry(entry.index)
    blockchain.add_pending_vote(vote)

    logger.info(f"Glas prihvaćen: {request.voter_id} → {request.candidate}")
    return VoteResponse(
        success=True,
        message="Glas uspješno dodan.",
        vote=vote,
    )


@app.post("/mine", response_model=MineResponse, status_code=status.HTTP_201_CREATED)
async def mine_block():
    """Rudari novi blok — samo leader, pa replicira na followere."""
    if raft.state != RaftState.LEADER:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Samo leader može rudariti blokove."
        )

    block = blockchain.mine_block(term=raft.current_term)
    if block is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Nema pending glasova za rudarenje."
        )

    store.save()

    # Repliciraj blok na sve followere
    block_data = block.to_dict()
    async with httpx.AsyncClient(timeout=settings.http_timeout) as client:
        for peer in raft.peers:
            try:
                resp = await client.post(
                    f"{peer}/blocks/sync",
                    json={"block": block_data}
                )
                logger.info(f"Blok #{block.index} repliciran na {peer} — status {resp.status_code}")
            except Exception as e:
                logger.warning(f"Replikacija bloka na {peer} neuspješna: {e}")

    return MineResponse(
        success=True,
        message=f"Blok #{block.index} iskopan i repliciran.",
        block=block_data,
    )


@app.get("/votes/pending")
async def get_pending():
    return {"pending_votes": blockchain.pending_votes, "count": len(blockchain.pending_votes)}


@app.get("/votes/results")
async def get_results():
    results = blockchain.get_results()
    total = sum(results.values())
    return {
        "results": {
            c: {"votes": v, "percentage": round(v / total * 100, 2) if total else 0}
            for c, v in sorted(results.items(), key=lambda x: -x[1])
        },
        "total_votes": total,
        "blocks_mined": len(blockchain.chain) - 1,
    }


class BlockSyncRequest(BaseModel):
    block: dict


@app.post("/blocks/sync", status_code=status.HTTP_200_OK)
async def sync_block(request: BlockSyncRequest):
    """Follower prima replicirani blok od leadera."""
    success = blockchain.append_block(request.block)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Blok odbijen — neispravan hash ili previous_hash."
        )
    store.save()
    logger.info(f"Blok #{request.block['index']} sinkroniziran od leadera")
    return {"success": True, "chain_length": len(blockchain.chain)}


@app.get("/status")
async def get_status():
    return {
        "node_id": settings.node_id,
        "service": "voting_service",
        "raft_state": raft.state.value,
        "raft_term": raft.current_term,
        "chain_length": len(blockchain.chain),
        "pending_votes": len(blockchain.pending_votes),
        "is_valid": blockchain.is_valid(),
        "peers": raft.peers,
    }


@app.get("/blocks/chain")
async def get_chain():
    """Vrati cijeli blockchain — za sync novih/recovering čvorova."""
    return {
        "chain": blockchain.to_dict(),
        "length": len(blockchain.chain),
        "node_id": settings.node_id,
    }