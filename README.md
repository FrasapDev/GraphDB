# Modelli a confronto: relazionale, a grafo e distribuito su un caso reale

Progetto per l'esame di **Architettura dei Dati**. Analizza la rete sociale
di Twitch (168 k nodi, 6.8 M archi, dataset SNAP) confrontando come sistemi
con architetture radicalmente diverse rispondono allo stesso insieme di query
di analisi di grafi.

Il lavoro si divide in due parti complementari:

**Parte centralizzata** — PostgreSQL (relazionale) vs Neo4j (grafo nativo).
Stesse 6 metriche di rete (grado, clustering, betweenness, PageRank,
assortativita', caricamento), misurate con warmup e 2 run su un singolo host
Docker. L'obiettivo e' capire *perche'* le strutture dati e i piani di query
dei due motori producono tempi e complessita' diversi sulle stesse operazioni.

**Parte distribuita** — Cassandra (AP, RF=3) vs CockroachDB (CP, RF=3, Raft).
Stesso grafo su cluster a **3 nodi reali** (macchine Azure distinte, IP privati
su VNet). Oltre alle metriche di rete si dimostrano sperimentalmente le
garanzie del teorema CAP: Cassandra permette di scegliere il livello di
consistenza (ONE/QUORUM/ALL) per ogni query e rimane disponibile anche con
nodi down; CockroachDB garantisce sempre SERIALIZABLE via Raft ma va in
timeout quando i nodi vivi non raggiungono il quorum 2/3.

## Struttura

```
twitch-benchmark/
├── docker/
│   ├── docker-compose.yml             # i tre DB in container (tarato per 16 GB)
│   └── docker-compose.distributed.yml # cluster a 3 nodi Cassandra + 3 nodi CockroachDB
├── db/
│   ├── postgres_schema.sql        # DDL relazionale + indici giustificati
│   ├── neo4j_schema.cypher        # vincoli/indici, modello a grafo
│   ├── cassandra_cluster_schema.cql  # keyspace RF=3 per il cluster a 3 nodi
│   └── cockroachdb_schema.sql     # schema relazionale per il cluster CockroachDB
├── pipeline/
│   ├── data.py                    # download SNAP + normalizzazione
│   ├── loaders.py                 # load nei DB + misura tempi (metrica 6)
│   ├── metrics.py                 # le 6 metriche x 3 DB + Big-O + architettura
│   ├── report.py                  # genera results.md
│   ├── run_benchmark.py           # orchestratore (warmup + 2 misure)
│   └── distributed/               # ambito distribuito: Cassandra (AP) vs CockroachDB (CP)
│       ├── cassandra_cluster.py       # loader + metriche, CL configurabile
│       ├── cockroach.py               # loader + metriche, EXPLAIN local/full
│       ├── consistency_demo.py        # CL Cassandra + conflitti/isolamento
│       ├── fault_tolerance_demo.py    # kill di un nodo, RF=3
│       ├── run_distributed_benchmark.py  # orchestratore
│       └── report_distributed.py      # genera results_distributed.md
├── docs/                          # materiale espositivo per l'esame
│   ├── 01_modellazione.md
│   ├── 02_metriche_complessita.md
│   ├── 05_esplicabilita.md
│   ├── 06_limitazioni.md
│   └── 07_distribuito.md          # Cassandra (AP) vs CockroachDB (CP), cluster a 3 nodi
└── requirements.txt
```

## Prerequisiti

- Docker + Docker Compose
- Python 3.10+
- ~16 GB RAM (con 8 GB consiglio di usare `--sample 20000`)

## Esecuzione (copia-incolla)

```bash
# 1. dipendenze Python
pip install -r requirements.txt

# 2. avvia i database
docker compose -f docker/docker-compose.yml up -d

# 3. attendi che i DB siano pronti
docker compose -f docker/docker-compose.yml ps
#   stato 'healthy' su entrambi prima di proseguire

# 4. esegui il benchmark
#    -- dataset intero (serve RAM e pazienza):
python pipeline/run_benchmark.py --data ./data

#    -- consigliato la prima volta, sottocampione da 20k nodi:
python pipeline/run_benchmark.py --data ./data --sample 20000

#    -- solo riferimento NetworkX (nessun DB, verifica i numeri):
python pipeline/run_benchmark.py --data ./data --reference-only

# 5. risultati
cat results.md      # tabelle comparative
cat results.json    # dati grezzi
```

## Opzioni utili

| Flag | Effetto |
|---|---|
| `--sample N` | usa solo i primi N nodi (riduce RAM e tempi) |
| `--only postgres,neo4j` | salta uno o piu' DB |
| `--skip-load` | i DB sono gia' popolati, esegui solo le metriche |
| `--reference-only` | calcola solo i valori di riferimento con NetworkX |

## Note importanti

- **Betweenness esatta su PostgreSQL**: viene calcolata la distanza BFS media
  via CTE ricorsiva come *proxy di costo*; la Brandes completa in SQL puro sul
  grafo intero non e' praticabile — e' un risultato architetturale, non un
  limite dell'implementazione (vedi docs).
- **Verifica di correttezza**: tutte le metriche sono validate contro
  NetworkX sul campione (grado, clustering, PageRank, betweenness,
  assortativita' coincidono).

## Connessioni (default)

| DB | host:porta | credenziali |
|---|---|---|
| PostgreSQL | localhost:5432 | bench / bench, db `twitch` |
| Neo4j | bolt://localhost:7687 | neo4j / benchpass |

## Parte distribuita: Cassandra (AP) vs CockroachDB (CP)

Cluster a 3 nodi per sistema, testato su **5 VM Azure** (VNet 10.0.1.0/24).
Spiegazione completa, modello dati e interpretazione in
[`docs/07_distribuito.md`](docs/07_distribuito.md).

### Topologia VM

| VM | IP privato | Ruolo | Container |
|---|---|---|---|
| VM1 | manager | pipeline Python + SSH orchestration | — |
| VM2 | 10.0.1.6 | seed Cassandra + CockroachDB node 1 | `tbd-cass-1`, `tbd-crdb-1` |
| VM3 | 10.0.1.5 | Cassandra node 2 + CockroachDB node 2 | `tbd-cass-2`, `tbd-crdb-2` |
| VM4 | 10.0.1.8 | Cassandra node 3 + CockroachDB node 3 | `tbd-cass-3`, `tbd-crdb-3` |
| Worker1 | 10.0.1.4 | PostgreSQL + Neo4j (benchmark centralizzato) | `tb-postgres`, `tb-neo4j` |

### Avvio cluster (una tantum)

```bash
# VM2, VM3, VM4 — ciascuna con il proprio compose file:
docker compose -f docker/docker-compose.vm2.yml up -d   # su VM2
docker compose -f docker/docker-compose.vm3.yml up -d   # su VM3
docker compose -f docker/docker-compose.vm4.yml up -d   # su VM4

# Verifica gossip Cassandra (attendi tutti UN = Up Normal):
docker exec tbd-cass-1 nodetool status

# Inizializza CockroachDB UNA SOLA VOLTA da VM2:
docker exec tbd-crdb-1 cockroach init --insecure --host=10.0.1.6:26257
```

### Esecuzione da VM1

```bash
# Variabili d'ambiente per multi-VM (aggiungile a run_distributed.sh):
export CASS_CONTACT_POINTS=10.0.1.6,10.0.1.5,10.0.1.8
export CRDB_HOST=10.0.1.6
export CASS_LOAD_CL=ONE          # piu' veloce per il dataset completo
export POSTGRES_HOST=10.0.1.4    # per la consistency demo

# Benchmark principale (dataset completo):
./run_distributed.sh --sample 0

# Demo consistenza (CL Cassandra; conflitti CockroachDB vs PostgreSQL):
python3 pipeline/distributed/consistency_demo.py

# Demo fault tolerance (kill SSH remoto dei container):
export CONTAINER_HOST_MAP=tbd-cass-1:10.0.1.6,tbd-cass-2:10.0.1.5,tbd-cass-3:10.0.1.8,tbd-crdb-1:10.0.1.6,tbd-crdb-2:10.0.1.5,tbd-crdb-3:10.0.1.8
python3 pipeline/distributed/fault_tolerance_demo.py

# Report unico con §benchmark + §consistenza + §fault tolerance:
python3 -c "
import json
from pipeline.distributed.report_distributed import build_report
with open('results_distributed.json') as f:
    build_report(json.load(f))
"
cat results_distributed.md
```
