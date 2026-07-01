# 6. Limitazioni del benchmark

Ogni benchmark misura un sottoinsieme della realta'. Documentare cosa **non**
misura e' altrettanto importante quanto documentare i risultati. Questa
sezione elenca le limitazioni note e le scelte consapevoli fatte nel progetto.

---

## 6.1 Hardware: singola macchina, risorse condivise

Tutti i database girano come container Docker sullo **stesso host** (laptop
con 16GB di RAM). Questo significa:

- I due DB (benchmark centralizzato) o i 6 nodi (benchmark distribuito)
  **competono per CPU, RAM e disco**: i tempi sono influenzati dall'attivita'
  degli altri container. In produzione ogni DB avrebbe risorse dedicate.
- Il **disco** e' condiviso: le scritture di PostgreSQL possono rallentare le
  letture di Neo4j. In produzione si userebbero SSD dedicati.
- La **rete** tra i nodi del cluster distribuito e' la loopback Docker
  (latenza ~microsecondi): le differenze di latenza tra CL=ONE e CL=ALL su
  Cassandra, e tra query locale e distribuita su CockroachDB, sono
  **attenuate** rispetto a un cluster su rete reale.

> **Mitigazione**: i tempi sono espressi come **mediana di 2 misure dopo 1
> warmup**, non come singola misurazione. Il confronto e' sui meccanismi e
> sugli ordini di grandezza, non sui valori assoluti.

---

## 6.3 Betweenness su PostgreSQL: proxy, non valore esatto

La betweenness centrality di Brandes richiede, per ogni sorgente campionata,
una BFS completa con conteggio dei cammini minimi e propagazione a ritroso
delle dipendenze. In SQL:

- La BFS si simula con `WITH RECURSIVE`, che e' **quadraticamente piu' lenta**
  di una BFS nativa (ri-deduplica il fronte ad ogni livello via anti-join)
- La propagazione a ritroso (backtracking delle dipendenze) non ha un
  corrispettivo naturale in SQL

Per questo PostgreSQL restituisce le **distanze medie** da un campione di
sorgenti come **proxy di costo**, non la betweenness vera. Il risultato
mostra il **costo architetturale** di fare percorsi in SQL, non un valore
confrontabile con Neo4j. E' un risultato onesto e documentato — non un
"fallimento".

---

## 6.4 Campionamento vs dataset completo

Il flag `--sample N` limita il grafo ai primi N nodi (ordinati per id) e agli
archi tra di essi. Questo introduce distorsioni:

- Il sottografo e' **piu' sparso** del grafo originale (molti archi vengono
  esclusi perche' un estremo cade fuori dal campione): il grado medio del
  campione (es. ~4 con 5000 nodi) e' molto inferiore a quello del dataset
  intero (~81)
- Le metriche globali (clustering, PageRank, assortativita') sono calcolate
  sul **sottografo**, non sull'intero — i valori non sono confrontabili tra
  run con `--sample` diversi
- Il campionamento e' **deterministico** (stessi N nodi con lo stesso seed)
  per riproducibilita', ma **non e' casuale** — i "primi N per id" possono
  non essere rappresentativi

> **Mitigazione**: il campionamento e' usato solo quando le risorse non
> permettono il dataset completo (es. il cluster distribuito a 6 container).
> Il benchmark centralizzato usa il dataset intero.

---

## 6.5 Configurazione dei database: non ottimizzata

I tre DB usano le configurazioni **di default** di Docker (con eccezioni
minime per il dimensionamento della RAM):

- **PostgreSQL**: `shared_buffers`, `work_mem`, `effective_cache_size` ai
  valori di default (~128MB shared_buffers). Un DBA ottimizzerebbe per il
  workload specifico
- **Neo4j**: heap e pagecache ai valori di default del container. Un tuning
  specifico (allocare piu' pagecache) migliorerebbe le operazioni on-disk

Questo e' una scelta deliberata: confrontare le configurazioni **out of the
box** e' piu' rappresentativo di un benchmark accademico. Un benchmark
industriale richiederebbe tuning specifico per ciascun DB.

---

## 6.6 Workload: solo OLAP, nessun OLTP

Le 6 metriche sono tutte **operazioni analitiche** (full scan, aggregazione
globale, join, iterazioni). Non misurano:

- **Latenza di lookup singolo** — "dammi gli attributi del nodo X": qui
  Cassandra eccellerebbe (O(1) partizione) e Neo4j sarebbe competitivo
  (index lookup + property read)
- **Throughput di scrittura concorrente** — inserimenti/aggiornamenti
  paralleli: Cassandra e' progettata per eccellere qui
- **Query transazionali OLTP** — read-modify-write con isolamento: il
  benchmark distribuito (`consistency_demo.py`) copre parzialmente questo
  aspetto con la demo dei conflitti

---

## 6.7 Neo4j GDS: licenza e disponibilita'

La libreria Graph Data Science (GDS) e' disponibile nella **Community
Edition** per uso non commerciale. In produzione richiede la licenza
Enterprise. I risultati di questo benchmark (clustering, betweenness,
PageRank nativi via GDS) **non sono riproducibili** senza GDS — con il solo
Cypher, le stesse metriche richiederebbero implementazioni manuali
significativamente piu' lente.

---

## 6.8 Cluster distribuito: rete locale, non partizioni reali

Il cluster distribuito (3 nodi Cassandra + 3 nodi CockroachDB) gira su
**Docker su un singolo host**. Questo significa:

- Le **partizioni di rete** (il "P" del teorema CAP) non sono simulabili:
  tutti i nodi si parlano via loopback, non c'e' mai una vera interruzione
  di rete. Il `docker stop` simula un **crash di nodo**, non una partizione
- Il **tempo di ri-elezione Raft** su CockroachDB e il **gossip** di
  Cassandra sono piu' veloci del reale (latenza di rete ~0)
- Il dataset per il cluster distribuito e' **campionato** (5000 nodi di
  default) per restare dentro le risorse di un laptop

> **Mitigazione per il futuro**: con VM dedicate su rete reale (vedi
> `docs/07_distribuito.md`, sezione hardware), questi effetti diventano
> misurabili e le differenze tra AP e CP piu' nette.
