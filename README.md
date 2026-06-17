# Benchmark: Neo4j vs PostgreSQL vs Cassandra — Twitch Gamers Network

Confronto delle performance di tre database (grafo, relazionale, wide-column
distribuito) sul calcolo di 6 metriche di rete, con spiegazione architetturale
dei risultati. Progetto per l'esame di Architettura dei Dati.

## Struttura

```
twitch-benchmark/
├── docker/
│   ├── docker-compose.yml             # i tre DB in container (tarato per 16 GB)
│   └── docker-compose.distributed.yml # cluster a 3 nodi Cassandra + 3 nodi CockroachDB
├── db/
│   ├── postgres_schema.sql        # DDL relazionale + indici giustificati
│   ├── neo4j_schema.cypher        # vincoli/indici, modello a grafo
│   ├── cassandra_schema.cql       # keyspace + tabelle denormalizzate (1 nodo)
│   ├── cassandra_cluster_schema.cql  # keyspace RF=3 per il cluster a 3 nodi
│   └── cockroachdb_schema.sql     # schema relazionale per il cluster CockroachDB
├── pipeline/
│   ├── data.py                    # download SNAP + normalizzazione
│   ├── loaders.py                 # load nei 3 DB + misura tempi (metrica 6)
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
- ~16 GB RAM (con 8 GB usare `--sample 20000`)

## Esecuzione (copia-incolla)

```bash
# 1. dipendenze Python
pip install -r requirements.txt

# 2. avvia i tre database
docker compose -f docker/docker-compose.yml up -d

# 3. attendi che Cassandra sia pronto (1-2 min al primo avvio)
docker compose -f docker/docker-compose.yml ps
#   stato 'healthy' su tutti e tre prima di proseguire

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

- **Cassandra a 1 nodo** (non 3): su un singolo host 3 nodi competono per la
  stessa RAM e non misurano nulla di reale. Vedi `docs/06_limitazioni.md` per
  cosa cambierebbe su un cluster vero (replication factor, token ring).
- **Betweenness su Cassandra**: limitata a 500 nodi (vincolo del progetto),
  calcolata interamente lato client perche' Cassandra non esegue BFS.
- **Betweenness esatta su PostgreSQL**: il file calcola le distanze BFS via
  CTE ricorsiva come *proxy di costo*; la Brandes completa in SQL puro sul
  grafo intero non e' praticabile, ed e' un risultato architetturale, non un
  limite dell'implementazione (vedi docs).
- **Verifica di correttezza**: tutte le metriche sono validate contro
  NetworkX sul campione (grado, clustering, PageRank, betweenness,
  assortativita' coincidono).

## Connessioni (default)

| DB | host:porta | credenziali |
|---|---|---|
| PostgreSQL | localhost:5432 | bench / bench, db `twitch` |
| Neo4j | bolt://localhost:7687 | neo4j / benchpass |
| Cassandra | localhost:9042 | (nessuna auth) |

## Ambito distribuito: Cassandra (AP) vs CockroachDB (CP)

Un secondo benchmark, separato, confronta due sistemi distribuiti con
logiche **opposte** (consistenza tunabile vs consistenza forte via Raft) su
cluster a **3 nodi reali** ciascuno, sempre su Docker Compose. Spiegazione
completa, modello dati e interpretazione dei risultati in
[`docs/07_distribuito.md`](docs/07_distribuito.md).

```bash
# 0. ferma il benchmark centralizzato (stessa RAM da 16GB)
docker compose -f docker/docker-compose.yml down

# 1. avvia il cluster distribuito (3 nodi Cassandra RF=3 + 3 nodi CockroachDB RF=3)
docker compose -f docker/docker-compose.distributed.yml up -d
docker compose -f docker/docker-compose.distributed.yml ps   # attendi 'healthy'

# 2. benchmark: load + metriche + tabella di confronto a 4 colonne
python pipeline/distributed/run_distributed_benchmark.py --data ./data --sample 5000

# 3. demo consistenza (CL Cassandra; conflitti CockroachDB vs PostgreSQL)
docker compose -f docker/docker-compose.yml up -d postgres
python pipeline/distributed/consistency_demo.py

# 4. demo fault tolerance (kill di un nodo via docker stop/start)
python pipeline/distributed/fault_tolerance_demo.py

# 5. rigenera il report con tutte le sezioni
python pipeline/distributed/run_distributed_benchmark.py --data ./data --sample 5000 --skip-load

# 6. risultati
cat results_distributed.md

# 7. al termine
docker compose -f docker/docker-compose.distributed.yml down
```
