-- =====================================================================
--  Cassandra bootstrap script
--  Applied automatically by the `cassandra-init` service of
--  infra/docker-compose.yml (or manually with: cqlsh -f cassandra_init.cql)
-- =====================================================================

CREATE KEYSPACE IF NOT EXISTS fraud_detection
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Predictions produced by the Spark streaming job (one row per model)
CREATE TABLE IF NOT EXISTS fraud_detection.transactions_pred (
    transaction_id        text,
    model                 text,
    trans_date_trans_time text,
    cc_num                text,
    merchant              text,
    category              text,
    amt                   double,
    first                 text,
    last                  text,
    gender                text,
    street                text,
    city                  text,
    state                 text,
    zip                   int,
    lat                   double,
    long                  double,
    city_pop              int,
    job                   text,
    dob                   text,
    unix_time             bigint,
    merch_lat             double,
    merch_long            double,
    prediction            int,
    PRIMARY KEY ((transaction_id), model)
);

-- Alerts generated for every transaction predicted as fraudulent
CREATE TABLE IF NOT EXISTS fraud_detection.alerts (
    alert_id        uuid,
    transaction_id  text,
    cc_num          text,
    amt             double,
    merchant        text,
    alert_type      text,
    risk_score      double,
    status          text,
    PRIMARY KEY (alert_id)
);
