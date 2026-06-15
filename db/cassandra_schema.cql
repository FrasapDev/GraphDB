// ============================================================================
// Cassandra — Schema per Twitch Gamers Network
// ============================================================================
// Cassandra NON e' un database a grafo ne' relazionale: e' uno storage
// engine wide-column distribuito, ottimizzato per scritture e per letture su
// PARTIZIONE NOTA. Non esistono join. La regola di modellazione e' rovesciata
// rispetto a SQL: si parte DALLE QUERY e si crea UNA TABELLA PER PATTERN DI
// ACCESSO, denormalizzando i dati (stessi dati scritti piu' volte).
//
// Pattern di accesso serviti in questo benchmark:
//   (A) "dammi gli attributi del canale K"           -> channels_by_id
//   (B) "dammi tutti i vicini (follow) del canale K" -> follows_by_source
//
// Tutto cio' che e' GLOBALE (grado medio, PageRank, betweenness, clustering
// globale, assortativita') NON ha una partizione naturale: richiede di
// leggere TUTTE le partizioni (token range scan) e di aggregare LATO CLIENT.
// E' il punto architetturale centrale del confronto.
// ============================================================================

CREATE KEYSPACE IF NOT EXISTS twitch
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

// ----------------------------------------------------------------------------
// (A) channels_by_id — dettagli del canale
// ----------------------------------------------------------------------------
// Partition key = id. Ogni canale e' una partizione a se'. Lookup per id =
// O(1) sul nodo che possiede quel token. Pattern in cui Cassandra eccelle:
// accesso a singola chiave nota.
CREATE TABLE IF NOT EXISTS twitch.channels_by_id (
    id           int PRIMARY KEY,
    views        bigint,
    mature       boolean,
    life_time    int,
    dead_account boolean,
    language     text,
    affiliate    boolean
);

// ----------------------------------------------------------------------------
// (B) follows_by_source — lista di adiacenza
// ----------------------------------------------------------------------------
// PARTITION KEY = source_id, CLUSTERING KEY = target_id.
// Tutti i vicini di un canale nella STESSA partizione, ordinati per
// target_id: "i follow di K" = lettura di una partizione contigua.
// Analogo Cassandra dell'adjacency list.
//
// COSTO NASCOSTO: grafo reso direzionale, ogni arco inserito DUE volte
// (a->b nella partizione a, b->a nella partizione b). ~13.6M righe.
CREATE TABLE IF NOT EXISTS twitch.follows_by_source (
    source_id int,
    target_id int,
    PRIMARY KEY (source_id, target_id)
) WITH CLUSTERING ORDER BY (target_id ASC);

// ----------------------------------------------------------------------------
// (C) channels_by_language — denormalizzazione per la lingua
// ----------------------------------------------------------------------------
// Per l'assortativita' serve scorrere i canali per lingua. Tabella dedicata
// con partition key = language: "tutti i canali in lingua L" = una partizione.
// ATTENZIONE: lingue molto frequenti (EN) creano hot partition / wide row.
// Su dataset reali si gestisce con bucketing; qui lo accettiamo.
CREATE TABLE IF NOT EXISTS twitch.channels_by_language (
    language  text,
    id        int,
    PRIMARY KEY (language, id)
);

// Nessun indice secondario: i 2i di Cassandra sono una trappola su alta
// cardinalita'. La via corretta e' sempre una tabella per query.
