"""
Postavlja izoliranu test konfiguraciju PRIJE nego se aplikacija uveze,
jer se env varijable čitaju jednom, pri importu shared.config.

- Zaseban NODE_ID i DATA_DIR, da testovi ne diraju ./data/node-1.json
  (podatke koje koristiš za stvarno pokretanje/demo).
- Prazan SEED_PEERS, da čvor bude sam u klasteru i odmah izabere
  sebe za leadera (majority od 1 čvora = 1 glas).

STAVI OVAJ FAJL U: tests/conftest.py
(mora biti direktno u tests/, ne u tests/unit/ ili tests/integration/,
da se pokrene prije bilo kojeg importa aplikacije)
"""
import os
import shutil
import pytest

os.environ["NODE_ID"] = "test-node"
os.environ["NODE_PORT"] = "9999"
os.environ["SEED_PEERS"] = ""
os.environ["DATA_DIR"] = "./data/test"
os.environ["POW_DIFFICULTY"] = "2"  # niža težina = brži testovi
os.environ["RAFT_ELECTION_TIMEOUT_MIN"] = "0.2"
os.environ["RAFT_ELECTION_TIMEOUT_MAX"] = "0.4"
os.environ["RAFT_HEARTBEAT_INTERVAL"] = "0.1"


@pytest.fixture(scope="session", autouse=True)
def _cleanup_test_data():
    """Obriše testne podatke prije i poslije cijelog test runa."""
    test_dir = os.environ["DATA_DIR"]
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    yield
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)