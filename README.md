# Decentralizirani blockchain sustav za glasovanje

Sustav implementira decentralizirani mehanizam glasovanja temeljen na blockchain tehnologiji. Čvorovi postižu konsenzus putem **Raft algoritma** implementiranog od nule u Pythonu, bez korištenja vanjskih Raft biblioteka.

---

## Arhitektura

Sustav se sastoji od mreže ravnopravnih čvorova (peer-to-peer). Svaki čvor istovremeno:

- sudjeluje u Raft konsenzusu (leader election, log replication)
- održava vlastitu kopiju blockchaina
- prima i obrađuje zahtjeve za glasovanje
- replicira podatke na ostale čvorove

```
node-1 (port 8001) ──┐
                      ├── Raft konsenzus + blockchain replikacija
node-2 (port 8002) ──┤
                      │
node-3 (port 8003) ──┘
```

### Struktura projekta

```
voting-blockchain/
├── blockchain/
│   ├── consensus/
│   │   ├── raft.py          # Raft algoritam (RaftNode, stanje, logika)
│   │   └── raft_server.py   # HTTP endpointi i RaftRunner petlja
│   ├── node/
│   │   ├── block.py         # Struktura bloka, PoW, hash
│   │   └── blockchain.py    # Lanac blokova, validacija, rezultati
│   └── storage/
│       └── store.py         # Perzistencija na disk
├── services/
│   └── voting_service/
│       └── main.py          # FastAPI aplikacija, endpointi
├── shared/
│   ├── config.py            # Konfiguracija putem env varijabli
│   └── logging_config.py    # Strukturirani logging
├── tests/                   # Unit i integracijski testovi
├── docker-compose.yml       # Docker konfiguracija (3 čvora)
└── requirements.txt
```

---

## Raft konsenzus algoritam

Implementacija pokriva sljedeće aspekte Raft algoritma (prema originalnom radu Ongaro & Ousterhout, 2014):

**Leader election**
- Svaki čvor počinje kao follower s randomiziranim election timeoutom
- Ako ne primi heartbeat u zadanom vremenu, postaje kandidat i traži glasove
- Kandidat koji prikupi glasove većine (n/2 + 1) postaje leader
- Leader šalje periodične heartbeatove kako bi spriječio nove izbore

**Log replication**
- Samo leader prima zahtjeve za glasovanje
- Leader dodaje glas u Raft log i replicira ga na followere
- Entry se committa tek kad ga potvrdi većina čvorova
- Provjera konzistencije loga (prevLogIndex, prevLogTerm) prema §5.3

**Fault tolerance**
- Sustav nastavlja s radom sve dok je dostupna većina čvorova (2 od 3)
- Nakon pada leadera, preostali čvorovi biraju novog leadera (~3-5s)
- Čvor koji se vrati u mrežu automatski sinkronizira blockchain od peera s najduljim lancem

---

## Blockchain

Svaki minirani blok sadrži:
- skup glasova (transakcija)
- timestamp
- kriptografski hash prethodnog bloka
- Raft term u kojemu je kreiran
- nonce (Proof of Work)

**Proof of Work** zahtijeva da hash bloka počinje s određenim brojem nula (konfigurabilna težina, default 3).

**Genesis blok** ima fiksni timestamp (`0.0`) kako bi hash bio identičan na svim čvorovima — preduvjet za ispravnu replikaciju.

---

## Pokretanje

### Preduvjeti

```bash
python3 --version  # Python 3.13+
```

### Instalacija

```bash
git clone https://github.com/Robertino2809/blockchain-voting-system
cd voting-blockchain
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Pokretanje 3-čvornog klastera

Otvori **3 terminala** i pokreni po jedan čvor u svakom:

```bash
# Terminal 1 — node-1
NODE_ID=node-1 NODE_PORT=8001 SEED_PEERS="http://localhost:8002,http://localhost:8003" \
uvicorn services.voting_service.main:app --port 8001

# Terminal 2 — node-2
NODE_ID=node-2 NODE_PORT=8002 SEED_PEERS="http://localhost:8001,http://localhost:8003" \
uvicorn services.voting_service.main:app --port 8002

# Terminal 3 — node-3
NODE_ID=node-3 NODE_PORT=8003 SEED_PEERS="http://localhost:8001,http://localhost:8002" \
uvicorn services.voting_service.main:app --port 8003
```

Pričekaj ~5 sekundi da se izabere leader.

---

## Demo

### 1. Provjera statusa klastera

```bash
curl -s http://localhost:8001/raft/status | python3 -m json.tool
curl -s http://localhost:8002/raft/status | python3 -m json.tool
curl -s http://localhost:8003/raft/status | python3 -m json.tool
```

Jedan čvor mora biti `"state": "leader"`, ostali `"state": "follower"`.

### 2. Glasovanje

Zahtjevi za glasovanje šalju se **samo na leader čvor** (pretpostavljamo da je leader na portu 8001):

```bash
curl -s -X POST http://localhost:8001/votes \
  -H "Content-Type: application/json" \
  -d '{"voter_id": "alice", "candidate": "kandidat-A"}' | python3 -m json.tool

curl -s -X POST http://localhost:8001/votes \
  -H "Content-Type: application/json" \
  -d '{"voter_id": "bob", "candidate": "kandidat-B"}' | python3 -m json.tool
```

### 3. Rudarenje bloka

```bash
curl -s -X POST http://localhost:8001/mine | python3 -m json.tool
```

### 4. Provjera rezultata na svim čvorovima

```bash
curl -s http://localhost:8001/votes/results | python3 -m json.tool
curl -s http://localhost:8002/votes/results | python3 -m json.tool
curl -s http://localhost:8003/votes/results | python3 -m json.tool
```

Sva tri čvora moraju prikazati iste rezultate.

### 5. Test tolerancije na greške

```bash
# Ugasi leader čvor (Ctrl+C u terminalu node-1)
# Pričekaj re-election (~5s)
sleep 5

# Provjeri novog leadera
curl -s http://localhost:8002/raft/status | python3 -m json.tool

# Pošalji glas kroz novog leadera
curl -s -X POST http://localhost:8002/votes \
  -H "Content-Type: application/json" \
  -d '{"voter_id": "charlie", "candidate": "kandidat-A"}' | python3 -m json.tool

# Pokreni node-1 ponovo i pričekaj node recovery sync
# Nakon ~10s provjeri da je node-1 sinkroniziran
curl -s http://localhost:8001/votes/results | python3 -m json.tool
```

---

## Testovi

```bash
pytest tests/ -v
```

31 test — unit testovi za blockchain i Raft logiku, integracijski testovi za API endpointe.

---

## Konfiguracija

Sve konfiguracije se postavljaju putem environment varijabli:

| Varijabla | Default | Opis |
|-----------|---------|------|
| `NODE_ID` | `node-1` | Jedinstveni identifikator čvora |
| `NODE_PORT` | `8000` | Port na kojemu čvor sluša |
| `SEED_PEERS` | `""` | Lista peerova (comma-separated URLs) |
| `POW_DIFFICULTY` | `3` | Težina Proof of Work (broj vodećih nula) |
| `RAFT_ELECTION_TIMEOUT_MIN` | `1.5` | Minimalni election timeout (sekunde) |
| `RAFT_ELECTION_TIMEOUT_MAX` | `3.0` | Maksimalni election timeout (sekunde) |
| `RAFT_HEARTBEAT_INTERVAL` | `0.5` | Interval heartbeata (sekunde) |

---

## API endpointi

| Metoda | Endpoint | Opis |
|--------|----------|------|
| `POST` | `/votes` | Pošalji glas (samo leader) |
| `POST` | `/mine` | Rudari novi blok (samo leader) |
| `GET` | `/votes/results` | Rezultati glasovanja |
| `GET` | `/votes/pending` | Glasovi u čekanju |
| `GET` | `/status` | Status čvora |
| `GET` | `/blocks/chain` | Cijeli blockchain |
| `POST` | `/blocks/sync` | Sinkronizacija bloka (interni) |
| `POST` | `/raft/heartbeat` | Raft heartbeat (interni) |
| `POST` | `/raft/vote` | Raft vote request (interni) |
| `POST` | `/raft/append` | Raft log append (interni) |
| `GET` | `/raft/status` | Status Raft čvora |

---

## Tehnologije

- **Python 3.13**
- **FastAPI** — HTTP framework
- **httpx** — async HTTP klijent za međučvornu komunikaciju
- **Pydantic** — validacija podataka
- **uvicorn** — ASGI server
- **pytest** — testiranje