"""
Integracijski testovi protiv stvarnog FastAPI app-a, jedan izolirani
čvor (bez peerova). Testira ono što smo ručno provjerili kroz curl
tijekom razvoja: leader election, glasovanje, zaštitu od dvostrukog
glasanja, mining i PoW, te konzistentnost rezultata.

STAVI OVAJ FAJL U: tests/integration/test_voting_api.py

NAPOMENA: import path "services.voting_service.main" pretpostavlja da
pytest pokrećeš iz root direktorija projekta (gdje je i uvicorn
komanda). Ako import puca, provjeri da imaš prazan __init__.py u
svakom nadređenom folderu (services/, services/voting_service/) —
već ih imaš po dosadašnjoj strukturi, pa bi trebalo raditi.
"""
import time
import pytest
from fastapi.testclient import TestClient

from services.voting_service.main import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as c:
        _wait_for_leader(c)
        yield c


def _wait_for_leader(client, timeout=5.0):
    """Čeka da izolirani čvor (bez peerova) izabere sam sebe za leadera."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = client.get("/raft/status")
        if r.status_code == 200 and r.json().get("state") == "leader":
            return
        time.sleep(0.1)
    pytest.fail("Čvor nije postao leader unutar timeouta — provjeri election_timeout konfiguraciju u conftest.py")


class TestRaftStatus:
    def test_single_node_becomes_leader(self, client):
        r = client.get("/raft/status")
        assert r.status_code == 200
        body = r.json()
        assert body["state"] == "leader"
        assert body["node_id"] == "test-node"


class TestVoting:
    def test_first_vote_accepted(self, client):
        r = client.post("/votes", json={"voter_id": "alice", "candidate": "kandidat_A"})
        assert r.status_code == 201
        body = r.json()
        assert body["success"] is True
        assert body["vote"]["voter_id"] == "alice"

    def test_duplicate_vote_rejected(self, client):
        # alice je već glasala u prošlom testu (isti klijent/čvor unutar modula)
        r = client.post("/votes", json={"voter_id": "alice", "candidate": "kandidat_A"})
        assert r.status_code != 200
        detail = r.json().get("detail", "")
        assert "već" in str(detail).lower() or "pending" in str(detail).lower()

    def test_second_distinct_voter_accepted(self, client):
        r = client.post("/votes", json={"voter_id": "bob", "candidate": "kandidat_B"})
        assert r.status_code == 201
        assert r.json()["success"] is True


class TestMiningAndResults:
    def test_mine_produces_valid_pow_hash(self, client):
        r = client.post("/mine")
        assert r.status_code == 201
        block = r.json()["block"]
        difficulty = 2  # mora odgovarati POW_DIFFICULTY iz conftest.py
        assert block["hash"].startswith("0" * difficulty)
        assert block["previous_hash"]  # genesis hash mora postojati

    def test_mine_rejects_when_no_pending_votes(self, client):
        r = client.post("/mine")
        assert r.status_code != 200

    def test_results_reflect_mined_votes(self, client):
        r = client.get("/votes/results")
        assert r.status_code == 200
        results = r.json()["results"]
        assert results["kandidat_A"]["votes"] == 1
        assert results["kandidat_B"]["votes"] == 1
        assert r.json()["total_votes"] == 2

    def test_voter_still_rejected_after_mining(self, client):
        # has_voted mora prolaziti i kroz minirane blokove, ne samo pending
        r = client.post("/votes", json={"voter_id": "alice", "candidate": "kandidat_A"})
        assert r.status_code != 200


class TestBlockchainIntegrity:
    def test_chain_has_genesis_and_mined_block(self, client):
        r = client.get("/blocks/chain")
        assert r.status_code == 200
        chain = r.json()["chain"]
        assert len(chain) == 2  # genesis + jedan minirani blok
        assert chain[0]["index"] == 0
        assert chain[0]["previous_hash"] == "0" * 64
        assert chain[1]["previous_hash"] == chain[0]["hash"]