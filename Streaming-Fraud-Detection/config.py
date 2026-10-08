"""Central configuration loader for the Streaming Fraud Detection project.

Every script in the repository reads its settings from here instead of
hard-coding hosts, paths or credentials.

Usage
-----
    import sys, pathlib
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[N]))
    from config import cfg, project_path

    servers = cfg.KAFKA_BOOTSTRAP_SERVERS
    csv     = project_path("TRAIN_CSV")

The values are read in this order (first one wins):
    1. real environment variables
    2. the `.env` file located at the project root
    3. the built-in defaults below
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional

PROJECT_ROOT = Path(__file__).resolve().parent
ENV_FILE = PROJECT_ROOT / ".env"

DEFAULTS: Dict[str, str] = {
    # Kafka
    "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092",
    "KAFKA_EXTERNAL_BOOTSTRAP_SERVERS": "localhost:29092",
    "KAFKA_TOPIC": "transaction_data",
    "KAFKA_CLIENT_ID": "fraud-producer",
    "KAFKA_SEND_INTERVAL_SECONDS": "3",
    "KAFKA_BATCH_MIN": "1",
    "KAFKA_BATCH_MAX": "10",
    # Paths
    "DATA_DIR": "data/raw",
    "TRAIN_CSV": "data/raw/fraudTrain.csv",
    "TEST_CSV": "data/raw/fraudTest.csv",
    "RESULTS_DIR": "resultats",
    "EDA_DIR": "resultats/EDA",
    "EDA_PLOT_DIR": "resultats/EDA/visualisation",
    "MODELS_DIR": "resultats/models",
    "SIDEBAR_IMAGE": "demo/architecture.png",
    # Spark
    "SPARK_APP_NAME": "StreamingFraudDetection",
    "SPARK_MASTER_URL": "spark://spark-master:7077",
    "SPARK_EXECUTOR_MEMORY": "4g",
    "SPARK_DRIVER_MEMORY": "2g",
    "SPARK_CHECKPOINT_DIR": "/tmp/spark_checkpoint",
    # Models
    "MODEL_GBT_PATH": "resultats/models/GBTClassifier_model",
    "MODEL_RF_PATH": "resultats/models/RandomForest_model",
    "MODEL_LR_PATH": "resultats/models/LogisticRegression_model",
    "MODELS_TO_APPLY": "GBT",
    # Cassandra
    "CASSANDRA_HOSTS": "127.0.0.1",
    "CASSANDRA_PORT": "9042",
    "CASSANDRA_KEYSPACE": "fraud_detection",
    "CASSANDRA_TRANSACTIONS_TABLE": "transactions_pred",
    "CASSANDRA_ALERTS_TABLE": "alerts",
    "CASSANDRA_REPLICATION_STRATEGY": "SimpleStrategy",
    "CASSANDRA_REPLICATION_FACTOR": "1",
    # E-mail alerts
    "SMTP_ENABLED": "false",
    "SMTP_HOST": "smtp.gmail.com",
    "SMTP_PORT": "587",
    "SMTP_USER": "",
    "SMTP_PASSWORD": "",
    "ALERT_FROM": "",
    "ALERT_TO": "",
}


def _parse_env_file(path: Path) -> Dict[str, str]:
    """Minimal `.env` parser (no external dependency required)."""
    values: Dict[str, str] = {}
    if not path.is_file():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].strip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def load_env(path: Path = ENV_FILE) -> Dict[str, str]:
    """Merge defaults, `.env` file and real environment variables."""
    merged = dict(DEFAULTS)
    merged.update(_parse_env_file(path))
    for key in list(merged):
        if key in os.environ:
            merged[key] = os.environ[key]
    os.environ.update({k: v for k, v in merged.items() if v != ""})
    return merged


_ENV: Dict[str, str] = load_env()


def get(key: str, default: Optional[str] = None) -> Optional[str]:
    """Return a configuration value."""
    return _ENV.get(key, default if default is not None else DEFAULTS.get(key))


def get_bool(key: str, default: bool = False) -> bool:
    return str(get(key, str(default))).strip().lower() in {"1", "true", "yes", "on"}


def get_int(key: str, default: int = 0) -> int:
    try:
        return int(float(get(key, str(default))))
    except (TypeError, ValueError):
        return default


def get_list(key: str, default: str = "") -> list:
    raw = get(key, default) or default
    return [item.strip() for item in raw.split(",") if item.strip()]


def project_path(key: str, default: Optional[str] = None) -> Path:
    """Resolve a configured path (relative paths -> project root)."""
    value = get(key, default) or ""
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


class _Config:
    """Attribute access to the configuration: `cfg.KAFKA_TOPIC`."""

    def __getattr__(self, item: str) -> Any:
        if item in DEFAULTS:
            return get(item)
        raise AttributeError(f"Unknown configuration key: {item}")

    @property
    def root(self) -> Path:
        return PROJECT_ROOT

    def path(self, key: str, default: Optional[str] = None) -> Path:
        return project_path(key, default)

    @property
    def cassandra_hosts(self) -> list:
        return get_list("CASSANDRA_HOSTS", "127.0.0.1")

    @property
    def smtp_enabled(self) -> bool:
        return get_bool("SMTP_ENABLED", False)


cfg = _Config()

if __name__ == "__main__":
    for _key in sorted(DEFAULTS):
        print(f"{_key}={get(_key)}")
