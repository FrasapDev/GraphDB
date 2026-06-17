# 7. Ambito distribuito — Cassandra (AP) vs CockroachDB (CP)

## 7.1 Cosa significa "ambito distribuito" per questo progetto

Il benchmark centralizzato (PostgreSQL, Neo4j, Cassandra a 1 nodo) misura
**come** tre modelli di dati (relazionale, a grafo, wide-column) eseguono le
stesse 6 metriche su un'unica macchina. Non mostra invece **cosa cambia
quando i dati sono distribuiti su piu' nodi**: replica, instradamento delle
query, e soprattutto le scelte di progettazione che un sistema distribuito
*deve* fare quando una rete puo' rallentare o un nodo puo' guastarsi — il
celebre trade-off del [teorema CAP](https://it.wikipedia.org/wiki/Teorema_CAP).

Per mostrarlo servono **almeno due nodi che si parlano**, non uno solo. Questa
sezione introduce due sistemi che risolvono lo stesso problema (un grafo
"chi segue chi" su Twitch) con **logiche distributive opposte**:

| | Cassandra | CockroachDB |
|---|---|---|
| Categoria CAP | **AP** (disponibilita' e tolleranza di partizione, consistenza tunabile) | **CP** (consistenza forte, disponibilita' sacrificata sotto partizione) |
| Modello dati | wide-column, denormalizzato per query | relazionale (SQL), normalizzato |
| Consenso | gossip + read repair / hinted handoff, nessun leader | Raft per ogni range (un leader per range) |
| Consistenza | **tunabile per query** (ONE/QUORUM/ALL) | sempre **SERIALIZABLE** (non negoziabile) |
| Sharding | token ring (hash della partition key) | range contigui (~512MiB), ri-bilanciati automaticamente |
| Cosa succede se un nodo cade | dipende dal CL scelto: si puo' continuare a scrivere/leggere anche con 1 nodo su 3 | i range con leader sul nodo caduto ri-eleggono (con 2/3 nodi); con 1/3 nessun range ha piu' quorum |

Il resto del documento spiega **come** ciascuno di questi punti e' stato
implementato e **come leggere** i numeri prodotti da
`pipeline/distributed/run_distributed_benchmark.py`.

---

## 7.2 Architettura del cluster

Tutto corre in Docker su **un'unica macchina con 16GB di RAM**
(`docker/docker-compose.distributed.yml`), come cluster a 3 nodi *reali* per
ciascun sistema (non simulati): 3 processi Cassandra distinti che si parlano
via gossip, e 3 processi CockroachDB distinti che si parlano via Raft/RPC.

Budget di RAM indicativo:
- Cassandra: 3 nodi x 512MB heap ≈ 1.5-2GB (piu' overhead JVM, totale ≈ 3.5GB)
- CockroachDB: 3 nodi x 256MB cache/SQL mem ≈ 2GB

Questo e' **deliberatamente l'opposto** della scelta fatta nel benchmark
centralizzato, dove Cassandra girava **a 1 nodo** perche' l'obiettivo era
confrontare i *modelli di dati* a parita' di hardware. Qui l'obiettivo e'
osservare i **meccanismi distribuiti** (replica, consenso, CL), quindi 3 nodi
sono il minimo indispensabile (e il massimo che 16GB permettono).

> Lo stack distribuito e quello centralizzato condividono la RAM della stessa
> macchina: **fermare l'uno prima di avviare l'altro**
> (`docker compose -f docker/docker-compose.yml down` /
> `docker compose -f docker/docker-compose.distributed.yml down`).

### Cassandra — connessione dall'host

Il client (`cassandra-driver`) scopre i peer via **gossip**: si connette al
nodo di contatto (`cass-1`, porta host 9042), legge `system.peers` e trova gli
IP *interni* degli altri due nodi sulla rete Docker (`172.28.1.12`,
`172.28.1.13`). Da fuori Docker questi IP non sono raggiungibili — solo le
porte pubblicate (9043, 9044) lo sono. Per questo
`pipeline/distributed/cassandra_cluster.py` definisce un
`DockerEndPointFactory` che traduce gli IP statici del bridge `distnet` nelle
porte host corrispondenti, cosi' il driver puo' aprire connessioni dirette a
tutti e 3 i nodi e fare *token-aware routing* anche dall'host.

### CockroachDB — connessione dall'host

Non serve nulla di analogo: il client SQL si connette a **un solo nodo**
(porta 26257/26258/26259), che agisce da *gateway* e instrada internamente le
query verso i nodi che possiedono i range coinvolti, in modo trasparente.

---

## 7.3 Modello dati

### Cassandra (`db/cassandra_cluster_schema.cql`) — keyspace `twitch_dist`, RF=3

```sql
CREATE KEYSPACE twitch_dist
  WITH replication = {'class':'SimpleStrategy','replication_factor':3};

CREATE TABLE channels_by_id (
  id INT PRIMARY KEY, views INT, mature BOOLEAN, life_time INT,
  dead_account BOOLEAN, language TEXT, affiliate BOOLEAN
);

CREATE TABLE follows_by_source (
  source_id INT, target_id INT,
  PRIMARY KEY (source_id, target_id)
) WITH CLUSTERING ORDER BY (target_id ASC);

CREATE TABLE channels_by_language (
  language TEXT, id INT, PRIMARY KEY (language, id)
);
```

`follows_by_source` e' la tabella chiave: **denormalizza** la relazione in modo
che "chi segue X" sia **una singola partizione** (`source_id` = partition
key). Questo e' il pattern Cassandra canonico — *"modella le query, non i
dati"* — e rende la query del grado locale di un nodo una lettura a **una
partizione**, indipendentemente da quanto e' grande il cluster.

### CockroachDB (`db/cockroachdb_schema.sql`) — relazionale, wire-compatible Postgres

```sql
CREATE TABLE users (
  id INT8 PRIMARY KEY, views INT8, mature BOOL, life_time INT8,
  dead_account BOOL, language STRING, affiliate BOOL
);
CREATE INDEX idx_users_language ON users (language);

CREATE TABLE follows (
  source_id INT8, target_id INT8,
  PRIMARY KEY (source_id, target_id)
);
CREATE INDEX follows_src_hash ON follows (source_id) USING HASH WITH BUCKET_COUNT = 8;
```

Stesso modello relazionale `users`/`follows` del benchmark centralizzato:
nessuna denormalizzazione "manuale", perche' in un sistema CP la
distribuzione e' **trasparente** — e' CockroachDB che divide `follows` in
range e li replica via Raft, non lo schema.

L'indice `follows_src_hash` (*hash-sharded index*) e' un artefatto didattico:
su una chiave **sequenziale** come `source_id`, gli inserimenti finiscono
tutti negli ultimi range (hotspot di scrittura su pochi nodi). L'hash-sharding
distribuisce le scritture su `BUCKET_COUNT` bucket pseudo-casuali, al prezzo
di letture range-scan leggermente piu' costose. Sul campione di questo
progetto l'effetto **non e' misurabile** (pochi MB di dati, un solo range);
e' incluso per discuterne il principio nella relazione, non per i numeri.
Il loader (`CockroachLoader`) crea questo indice in modo *best-effort*: se la
sintassi non e' supportata dalla versione in uso, lo statement viene saltato
senza interrompere il caricamento.

---

## 7.4 Consistenza: tunabile (Cassandra) vs sempre-forte (CockroachDB)

### Cassandra — Consistency Level per query

Con RF=3, ogni query specifica **quante repliche** devono confermare prima
che il coordinatore risponda al client:

| CL | repliche attese | implicazione |
|---|---|---|
| `ONE` | 1 | piu' veloce, possibile leggere un dato "vecchio" se la replica contattata non e' aggiornata |
| `QUORUM` | 2 (= ⌊3/2⌋+1) | compromesso tipico: se W e R sono entrambi QUORUM, le letture vedono sempre l'ultima scrittura (consistenza "forte" per quella chiave) |
| `ALL` | 3 | massima sicurezza, **minima disponibilita'**: basta 1 nodo down per bloccare la query |

`pipeline/distributed/consistency_demo.py::cassandra_cl_latency` scrive e
legge N righe a ciascun CL e misura la latenza mediana: ci si aspetta
`ONE < QUORUM < ALL` sia in lettura che in scrittura, perche' il coordinatore
attende un numero crescente di ACK di rete.

`pipeline/distributed/run_distributed_benchmark.py` applica la stessa idea
alle query del benchmark: il **grado locale** (1 partizione) e il **grado
globale** (full token-range scan) vengono misurati a CL=ONE/QUORUM/ALL — la
sezione 4 di `results_distributed.md` mostra che il costo del CL si
**moltiplica** sulla query globale (si applica a ogni pagina della scan)
mentre resta quasi costante su quella locale (1 sola richiesta).

### CockroachDB — sempre SERIALIZABLE

> **Correzione concettuale rispetto alla richiesta iniziale "SNAPSHOT vs
> SERIALIZABLE su CockroachDB":** da CockroachDB v20.1, **tutti** i livelli di
> isolamento richiesti via `SET TRANSACTION ISOLATION LEVEL ...` vengono
> mappati su **SERIALIZABLE**. Non esiste un livello piu' debole da scegliere:
> e' la consistenza forte (la "C" di CP) ad essere il default non
> negoziabile, ottenuta con controllo di concorrenza ottimistico sopra Raft.

Il confronto interessante non e' quindi "due livelli su CockroachDB", ma **tre
sistemi/configurazioni a confronto sulla stessa race condition**
(`pipeline/distributed/consistency_demo.py::conflict_demo`): due transazioni
concorrenti fanno *read-modify-write* su `accounts.id=1` con delta opposti
(+10/-10), N volte ciascuna. Saldo iniziale 1000; se tutto va bene il saldo
finale torna a 1000.

| Sistema/isolamento | Cosa succede in caso di conflitto |
|---|---|
| **PostgreSQL READ COMMITTED** (default) | **Lost update silenzioso**: nessun errore, ma il saldo finale puo' finire != 1000 — la seconda transazione sovrascrive l'effetto della prima senza saperlo. |
| **PostgreSQL SERIALIZABLE** | La race viene **rilevata**: una delle due transazioni fallisce con `SQLSTATE 40001` ("serialization failure"), il client la ritenta. Saldo finale sempre 1000. |
| **CockroachDB** (sempre SERIALIZABLE) | Stesso `40001`/retry di Postgres SERIALIZABLE, ma **come comportamento di default**, non come opzione. Saldo finale sempre 1000. |

I valori effettivi (numero di retry, saldo finale, "consistente" si/no) sono
nella sezione 6 di `results_distributed.md` (da `results_consistency.json`).

---

## 7.5 Fault tolerance: cosa succede quando un nodo cade

`pipeline/distributed/fault_tolerance_demo.py` spegne (`docker stop`) nodi del
cluster uno alla volta e ripete un'operazione, con RF=3 in entrambi i casi:

### Cassandra — RF=3 su 3 nodi: ogni nodo ha *tutte* le partizioni

| Nodi attivi | CL=ONE | CL=QUORUM | CL=ALL |
|---|---|---|---|
| 3/3 | OK | OK | OK |
| 2/3 | OK | OK | **FALLISCE** |
| 1/3 | OK | **FALLISCE** | **FALLISCE** |

Con RF=3 = numero di nodi, **anche l'unico nodo superstite ha una replica di
ogni partizione**: CL=ONE funziona persino a 1/3. CL=QUORUM (2/3) richiede
almeno 2 nodi; CL=ALL richiede tutti e 3. Questo e' il trade-off **AP** in
azione: si sceglie, query per query, quanta disponibilita' sacrificare per
quanta consistenza — il sistema non impone una risposta unica.

### CockroachDB — RF=3, Raft per range

| Nodi attivi | `SELECT count(*) FROM users` |
|---|---|
| 3/3 | OK |
| 2/3 | OK (con un picco di latenza per la ri-elezione del leader Raft dei range che lo avevano sul nodo caduto) |
| 1/3 | **timeout/non disponibile**: nessun range ha piu' il quorum (2/3) richiesto da Raft |

Questo e' il trade-off **CP**: con 2/3 nodi il quorum Raft (maggioranza) e'
ancora raggiungibile e il cluster resta **completamente consistente e
disponibile**; con 1/3 nodi *nessuna* operazione che richieda quorum puo'
procedere — il sistema preferisce **non rispondere** piuttosto che rispondere
con dati potenzialmente non aggiornati. E' il prezzo della "C" sotto
partizione (teorema CAP).

I risultati effettivi sono nella sezione 7 di `results_distributed.md` (da
`results_fault_tolerance.json`).

---

## 7.6 Tabella di confronto a 4 colonne

`pipeline/distributed/report_distributed.py` produce in `results_distributed.md`
una tabella di confronto **PostgreSQL / Neo4j / Cassandra (cluster, RF=3) /
CockroachDB**, unendo:
- `results.json` (benchmark centralizzato — campione grande/dataset intero)
- `results_distributed.json` (questo benchmark — campione piccolo, 6 container)

> **Attenzione ai campioni**: i due file derivano da run con `--sample`
> diversi (il cluster distribuito a 6 container non sostiene il dataset da
> 168k nodi / 6.8M archi su 16GB di RAM). Il confronto a 4 colonne va quindi
> letto sui **meccanismi** e sugli **ordini di grandezza relativi** (es. "il
> grado globale a CL=ALL costa Nx il grado a CL=ONE"), non sui valori assoluti
> tra colonne con campioni diversi.

Le sezioni del report sono:
1. Tempi di caricamento
2. Grado medio (4 colonne)
3. Tempi di esecuzione per metrica (grado, PageRank, assortativita', clustering)
4. Cassandra: query locale vs globale per Consistency Level
5. CockroachDB: `EXPLAIN ... distribution: local|full` come evidenza diretta
   della differenza locale/globale
6. Demo consistenza (`results_consistency.json`)
7. Demo fault tolerance (`results_fault_tolerance.json`)

---

## 7.7 Esecuzione — passo per passo

> **Poca RAM disponibile?** I 6 container (3 Cassandra + 3 CockroachDB)
> insieme possono mettere sotto pressione una macchina con meno di 16GB
> liberi, allungando di molto i tempi di avvio/healthcheck. In tal caso si
> possono eseguire i due cluster **in sequenza**:
> ```bash
> # solo Cassandra
> docker compose -f docker/docker-compose.distributed.yml up -d cass-1 cass-2 cass-3
> docker compose -f docker/docker-compose.distributed.yml ps   # attendi 'healthy'
> python pipeline/distributed/run_distributed_benchmark.py --data ./data --sample 5000 --only cassandra
>
> # libera RAM, poi solo CockroachDB
> docker compose -f docker/docker-compose.distributed.yml stop cass-1 cass-2 cass-3
> docker compose -f docker/docker-compose.distributed.yml up -d crdb-1 crdb-2 crdb-3 crdb-init
> python pipeline/distributed/run_distributed_benchmark.py --data ./data --sample 5000 --only cockroach
> ```
> `run_distributed_benchmark.py` rileva un `results_distributed.json`
> esistente e **unisce** i risultati delle due run (non sovrascrive) — usa
> pero' sempre lo **stesso `--sample`** in entrambe, altrimenti il campione di
> nodi cambia e i due lati del confronto non sono coerenti.

```bash
# 0. ferma il benchmark centralizzato (stessa RAM)
docker compose -f docker/docker-compose.yml down

# 1. avvia il cluster distribuito (3 nodi Cassandra + 3 nodi CockroachDB)
docker compose -f docker/docker-compose.distributed.yml up -d

# 2. attendi che tutti i nodi siano 'healthy' (1-2 min)
docker compose -f docker/docker-compose.distributed.yml ps

# 3. benchmark principale: load + metriche + tabella 4 colonne
python pipeline/distributed/run_distributed_benchmark.py --data ./data --sample 5000

# 4. demo consistenza (CL Cassandra, conflitti CockroachDB/Postgres)
#    richiede anche il Postgres del benchmark centralizzato in esecuzione
#    per il confronto READ COMMITTED vs SERIALIZABLE
docker compose -f docker/docker-compose.yml up -d postgres
python pipeline/distributed/consistency_demo.py

# 5. demo fault tolerance (ferma/riavvia nodi via docker stop/start)
python pipeline/distributed/fault_tolerance_demo.py

# 6. rigenera il report con tutte le sezioni (consistenza + fault tolerance)
python pipeline/distributed/run_distributed_benchmark.py --data ./data --sample 5000 --skip-load

# 7. risultati
cat results_distributed.md
cat results_distributed.json

# 8. al termine, ferma il cluster distribuito
docker compose -f docker/docker-compose.distributed.yml down
```

### Interpretazione rapida dei risultati

- **Grado locale vs globale (Cassandra)**: il tempo di `degree_local` deve
  restare ~costante al variare del CL (1 partizione); `degree_global` deve
  crescere visibilmente da ONE a ALL (si applica a ogni pagina della scan).
- **`EXPLAIN distribution` (CockroachDB)**: `local` per la query su 1
  `source_id`, `full` per la `GROUP BY` su tutta `follows` — e' la prova che
  CockroachDB **sa** quando una query e' confinabile a un range/nodo.
- **Demo consistenza**: `postgres_read_committed.consistent == False` (o
  comunque `retries == 0` con saldo talvolta != 1000) mostra il lost update
  silenzioso; `postgres_serializable` e `cockroach_conflict` devono avere
  `consistent == True` con `retries > 0`.
- **Demo fault tolerance**: la tabella Cassandra deve mostrare "scalini" netti
  (ONE sempre OK, QUORUM fallisce a 1/3, ALL fallisce gia' a 2/3); CockroachDB
  deve passare da OK a timeout esattamente tra 2/3 e 1/3 nodi.

---

## 7.8 Conclusioni e trade-off

Lo stesso problema (grafo "chi segue chi") ammette due soluzioni distribuite
con filosofie opposte:

- **Cassandra (AP)** da' al *chiamante* il controllo del trade-off
  consistenza/disponibilita' (il CL), query per query. E' la scelta naturale
  quando la disponibilita' di scrittura non si discute (es. ingestion di
  eventi) e si puo' tollerare una consistenza "eventuale" su letture non
  critiche.
- **CockroachDB (CP)** impone consistenza forte come default e **rifiuta** di
  rispondere quando non puo' garantirla (sotto-quorum). E' la scelta naturale
  per dati dove un valore "vecchio o sbagliato" (es. un saldo) e' peggio di
  un errore esplicito — al prezzo di una finestra di indisponibilita' durante
  i guasti e di retry lato applicazione per i conflitti seriali.

Nessuno dei due e' "migliore": sono risposte diverse alla stessa domanda che
il teorema CAP rende inevitabile in presenza di una partizione di rete. La
scelta tra i due — o tra l'uno e un sistema centralizzato come PostgreSQL — e'
quindi una scelta di **requisiti applicativi**, non di performance pura: i
numeri di `results_distributed.md` la rendono concreta, ma il criterio di
scelta e' "quale errore posso permettermi: un dato vecchio, o nessun dato?".
