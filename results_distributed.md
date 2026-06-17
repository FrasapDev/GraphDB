# Benchmark Distribuito: Cassandra (AP) vs CockroachDB (CP)

**Dataset:** Twitch Gamers Network — 5000 nodi, 3 nodi RF=3 per sistema.

| | Cassandra cluster | CockroachDB |
|---|---|---|
| Categoria CAP | AP (disponibilita' prioritaria) | CP (consistenza forte) |
| Consenso | Gossip + hinted handoff | Raft (leader per range) |
| Consistenza | Tunabile per query (ONE/QUORUM/ALL) | Sempre SERIALIZABLE |
| Modello dati | Wide-column, denormalizzato | Relazionale (SQL) |

## 1. Caricamento

| | Cassandra cluster | CockroachDB |
|---|---|---|
| Tempo load (s) | 14.80s CL=QUORUM | 5.11s |
| Righe nodi | 5000 | 5000 |
| Righe archi | 11250 | 11250 |

## 2. Grado medio

Cassandra: misurato a CL=QUORUM. CockroachDB: query distribuita (`distribution: full`) con consistenza forte.

| | Cassandra (CL=QUORUM) | CockroachDB |
|---|---|---|
| Grado medio | 3.934 | 3.934 |
| Dev. std | 7.069 | 7.069 |
| Grado max | 170 | 170 |

## 3. Tempi di esecuzione

| Metrica | Cassandra cluster | CockroachDB |
|---|---|---|
| Grado medio (global) | 0.163s | 0.006s |
| PageRank (10 it.) | 0.119s | n/d |
| Assortativita' | 0.151s | n/d |

## 4. Cassandra — locale vs globale per Consistency Level

**Locale** = lettura di 1 partizione (`source_id = X`), **globale** = full token-range scan su tutti i nodi.

| CL | locale (ms) | globale (s) | note |
|---|---|---|---|
| ONE | 5.17ms | 0.102s | 1 replica risponde, le altre 2 in background |
| QUORUM | 7.43ms | 0.163s | 2/3 repliche rispondono — compromesso tipico |
| ALL | 8.59ms | 0.158s | tutte e 3 le repliche rispondono — max latenza |

## 5. CockroachDB — distribuzione del piano (EXPLAIN)

| Query | EXPLAIN distribution |
|---|---|
| `WHERE source_id = X` (punto singolo) | `distribution: full` |
| `GROUP BY source_id` (scan globale) | `distribution: full` |

> **Nota:** su un campione piccolo l'intera tabella `follows` sta in un solo Raft range, quindi CockroachDB usa DistSQL (`full`) per entrambe le query. La distinzione `local` vs `full` diventa visibile su dataset piu' grandi, dove la tabella e' distribuita su piu' range/nodi reali.

## 6. Demo consistenza

### Cassandra — latenza scrittura/lettura per CL

| CL | scrittura mediana (ms) | lettura mediana (ms) |
|---|---|---|
| ONE | 15.53ms | 15.33ms |
| QUORUM | 9.20ms | 15.45ms |
| ALL | 15.74ms | 15.82ms |

### CockroachDB — conflitti concorrenti (read-modify-write)

Due thread con delta +10/-10 su `accounts.id=1`. Saldo iniziale = saldo atteso = 1000.

| Sistema | isolamento | retry (40001) | saldo finale | consistente |
|---|---|---|---|---|
| CockroachDB (sempre SERIALIZABLE) | default | 20 | 1000 | True |
| PostgreSQL SERIALIZABLE | SERIALIZABLE | 20 | 1000 | True |
| PostgreSQL READ COMMITTED | READ COMMITTED | 0 | 1200 | False |

## 7. Fault tolerance — nodo down (RF=3)

### Cassandra — effetto del CL con nodi mancanti

| Nodi attivi | CL=ONE | CL=QUORUM | CL=ALL |
|---|---|---|---|
| 3/3 | OK | OK | OK |
| 2/3 (1 spento) | OK | OK | FAIL (Unavailable: Error from server: code=1000 [Unavailable exception] message="Cannot achieve consistency level ALL" info={'consistency': 'ALL', 'required_replicas': 3, 'alive_replicas': 2}) |
| 1/3 (2 spenti) | OK | FAIL (Unavailable: Error from server: code=1000 [Unavailable exception] message="Cannot achieve consistency level QUORUM" info={'consistency': 'QUORUM', 'required_replicas': 2, 'alive_replicas': 1}) | FAIL (NoHostAvailable: ('Unable to complete the operation against any hosts', {<Host: 127.0.0.1:9042 datacenter1>: Unavailable('Error from server: code=1000 [Unavailable exception] message="Cannot achieve consistency level ALL" info={\'consistency\': \'ALL\', \'required_replicas\': 3, \'alive_replicas\': 1}')})) |

### CockroachDB — Raft sotto quorum

| Nodi attivi | SELECT count(*) FROM users |
|---|---|
| 3/3 | OK (5000 righe, 0.00s) |
| 2/3 — quorum Raft ok, ri-elezione | OK (5000 righe, 0.00s) |
| 1/3 — NESSUN range ha quorum | FAIL (QueryCanceled: query execution canceled due to statement timeout
) |

---
*Generato da `report_distributed.py` — vedi `docs/07_distribuito.md` per l'interpretazione.*
