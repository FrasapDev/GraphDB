-- ============================================================================
-- CockroachDB — Schema per il confronto "ambito distribuito" (CP, Raft)
-- ============================================================================
-- CockroachDB e' wire-compatible con PostgreSQL (psycopg2 funziona senza
-- modifiche) e usa lo STESSO modello relazionale di db/postgres_schema.sql:
-- archi duplicati (src,dst)+(dst,src) per rendere "i vicini di X" una
-- range-scan su (source_id, ...), esattamente come in Postgres.
--
-- LA DIFFERENZA NON E' NEL MODELLO LOGICO, E' NELL'ESECUZIONE:
--   - Postgres: una tabella = uno storage file su un disco, un processo.
--   - CockroachDB: una tabella e' divisa in RANGE (~512 MiB ciascuno di
--     default), ogni range e' un GRUPPO RAFT replicato su 3 nodi (uno e'
--     leader). Una query puo' quindi richiedere coordinamento tra range
--     diversi che vivono su nodi diversi -> e' questo il costo "distribuito"
--     che si vede nei tempi di risposta.
--
-- HOTSPOT DA CHIAVE SEQUENZIALE (concetto chiave per la relazione):
-- se la chiave primaria cresce in modo monotono (es. un source_id che nel
-- CSV e' quasi ordinato), TUTTI gli insert nuovi finiscono nell'ULTIMO
-- range della tabella, quindi su UN SOLO nodo (quello che ha la leadership
-- Raft di quel range) finche' quel range non si divide. E' l'opposto della
-- scalabilita' orizzontale che ci si aspetterebbe da un DB "distribuito".
-- Il rimedio standard e' un INDICE HASH-SHARDED: CockroachDB calcola un
-- hash della chiave e lo usa come prefisso, distribuendo le scritture su
-- piu' range/nodi fin dal primo momento. Lo dimostriamo sotto su
-- 'follows_hash' (indice aggiuntivo, vedi commento).
-- ============================================================================

DROP TABLE IF EXISTS follows CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- ----------------------------------------------------------------------------
-- USERS (= nodes di Postgres)
-- ----------------------------------------------------------------------------
CREATE TABLE users (
    id           INT8 PRIMARY KEY,
    views        INT8,
    mature       BOOL,
    life_time    INT8,
    dead_account BOOL,
    language     STRING,
    affiliate    BOOL
);

CREATE INDEX idx_users_language ON users (language);

-- ----------------------------------------------------------------------------
-- FOLLOWS (= edges di Postgres, direzionali duplicati)
-- ----------------------------------------------------------------------------
-- PRIMARY KEY (source_id, target_id): come in Postgres, "i follow di X" =
-- WHERE source_id = X, range-scan sull'indice primario.
CREATE TABLE follows (
    source_id INT8 NOT NULL,
    target_id INT8 NOT NULL,
    PRIMARY KEY (source_id, target_id)
);

-- ----------------------------------------------------------------------------
-- INDICE HASH-SHARDED (dimostrazione anti-hotspot)
-- ----------------------------------------------------------------------------
-- Indice secondario su source_id, ma con uno shard hash come prefisso
-- logico: CockroachDB distribuisce le righe su BUCKET_COUNT "secchi"
-- indipendenti dal valore di source_id, quindi su range/nodi diversi anche
-- se source_id cresce in modo sequenziale durante il load.
-- A questa scala (campione di poche migliaia/decine di migliaia di righe,
-- tutto probabilmente in 1-2 range) l'effetto sul tempo non e' misurabile:
-- l'indice serve come ARTEFATTO DIDATTICO (vedi SHOW CREATE TABLE / EXPLAIN
-- in docs/07_distribuito.md), non come ottimizzazione di questo benchmark.
CREATE INDEX follows_src_hash ON follows (source_id) USING HASH WITH BUCKET_COUNT = 8;

-- ----------------------------------------------------------------------------
-- ACCOUNTS — tabella minimale per la demo di isolamento/conflitti (Raft +
-- optimistic concurrency control). Vedi pipeline/distributed/consistency_demo.py
-- ----------------------------------------------------------------------------
-- CockroachDB fornisce SEMPRE SERIALIZABLE (non esiste un livello SNAPSHOT
-- separato come in Postgres: dalla v20.1 ogni SET TRANSACTION ISOLATION
-- LEVEL viene mappato su SERIALIZABLE). La consistenza forte e' ottenuta con
-- controllo di concorrenza OTTIMISTICO: due transazioni che si sovrappongono
-- sulla stessa riga possono fallire con SQLSTATE 40001
-- ("restart transaction") e il CLIENT deve ritentarle. E' il prezzo della
-- serializzabilita' in un sistema distribuito senza lock globali.
DROP TABLE IF EXISTS accounts CASCADE;
CREATE TABLE accounts (
    id      INT8 PRIMARY KEY,
    balance INT8 NOT NULL
);
INSERT INTO accounts (id, balance) VALUES (1, 1000), (2, 1000);
