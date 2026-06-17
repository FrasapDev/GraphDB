// ============================================================================
// Cassandra CLUSTER (3 nodi, RF=3) — Schema per il confronto "ambito
// distribuito" (AP, consistenza tunabile)
// ============================================================================
// Stesse tabelle del benchmark centralizzato (db/cassandra_schema.cql), ma:
//   - replication_factor = 3 (tante quante i nodi): ogni partizione e'
//     replicata su TUTTI i nodi. Questo e' il motivo per cui, piu' avanti,
//     CL=ONE sopravvive anche a 2 nodi su 3 spenti (il terzo ha comunque
//     una copia), mentre CL=ALL fallisce con un solo nodo spento.
//   - una tabella aggiuntiva 'consistency_demo', usata SOLO per gli
//     esperimenti di latenza/consistenza (pipeline/distributed/consistency_demo.py)
//     e per la prova di fault tolerance — tenerla separata dal grafo rende
//     l'esperimento leggero e ripetibile senza toccare 13M righe.
// ============================================================================

CREATE KEYSPACE IF NOT EXISTS twitch_dist
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
};

// ----------------------------------------------------------------------------
// (A) channels_by_id — dettagli del canale (identico al benchmark centrale)
// ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS twitch_dist.channels_by_id (
    id           int PRIMARY KEY,
    views        bigint,
    mature       boolean,
    life_time    int,
    dead_account boolean,
    language     text,
    affiliate    boolean
);

// ----------------------------------------------------------------------------
// (B) follows_by_source — adjacency list, partition key = source_id
// ----------------------------------------------------------------------------
// "i follow di K" = lettura di una singola partizione (query LOCALE, O(1)
// partizioni). Il grado MEDIO globale invece richiede di leggere TUTTE le
// partizioni (query MULTI-PARTIZIONE / token range scan). Il confronto tra
// questi due tempi e' al centro della metrica "grado medio" nella demo.
CREATE TABLE IF NOT EXISTS twitch_dist.follows_by_source (
    source_id int,
    target_id int,
    PRIMARY KEY (source_id, target_id)
) WITH CLUSTERING ORDER BY (target_id ASC);

// ----------------------------------------------------------------------------
// (C) channels_by_language — invariata, usata per l'assortativita'
// ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS twitch_dist.channels_by_language (
    language  text,
    id        int,
    PRIMARY KEY (language, id)
);

// ----------------------------------------------------------------------------
// (D) consistency_demo — tabella piccola e dedicata per gli esperimenti di
//     livello di consistenza (CL=ONE/QUORUM/ALL) e di fault tolerance.
// ----------------------------------------------------------------------------
// Partition key = id: ogni riga e' una partizione indipendente -> scrivere/
// leggere la riga K coinvolge solo i 3 nodi replica di K (con RF=3, in
// pratica TUTTI i nodi del cluster), ma la latenza percepita dal client
// dipende SOLO dal numero di repliche che devono confermare (CL), non dal
// numero totale di nodi.
CREATE TABLE IF NOT EXISTS twitch_dist.consistency_demo (
    id        int PRIMARY KEY,
    value     text,
    written_at timestamp
);
