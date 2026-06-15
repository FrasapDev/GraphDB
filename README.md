# Benchmark: Neo4j vs PostgreSQL vs Cassandra — Twitch Gamers Network

Confronto delle performance di tre database (grafo, relazionale, wide-column
distribuito) sul calcolo di 6 metriche di rete, con spiegazione architetturale
dei risultati. Progetto per l'esame di Architettura dei Dati.

## Struttura

```
twitch-benchmark/
├── docker/docker-compose.yml      # i tre DB in container (tarato per 16 GB)
├── db/
│   ├── postgres_schema.sql        # DDL relazionale + indici giustificati
│   ├── neo4j_schema.cypher        # vincoli/indici, modello a grafo
│   └── cassandra_schema.cql       # keyspace + tabelle denormalizzate
├── pipeline/
│   ├── data.py                    # download SNAP + normalizzazione
│   ├── loaders.py                 # load nei 3 DB + misura tempi (metrica 6)
│   ├── metrics.py                 # le 6 metriche x 3 DB + Big-O + architettura
│   ├── report.py                  # genera results.md
│   └── run_benchmark.py           # orchestratore (warmup + 2 misure)
├── docs/                          # materiale espositivo per l'esame
│   ├── 01_modellazione.md
│   ├── 02_metriche_complessita.md
│   ├── 05_esplicabilita.md
│   └── 06_limitazioni.md
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
