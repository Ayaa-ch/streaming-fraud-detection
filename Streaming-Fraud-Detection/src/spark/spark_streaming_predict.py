import logging
import sys
import uuid
from functools import reduce
from pathlib import Path

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import from_json, col, when, isnan, udf, lit
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType, LongType
from pyspark.ml import PipelineModel

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from config import cfg, get_bool, get_list, project_path

# Initialisation de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KEYSPACE = cfg.CASSANDRA_KEYSPACE
TABLE_TRANSACTIONS = cfg.CASSANDRA_TRANSACTIONS_TABLE
TABLE_ALERTS = cfg.CASSANDRA_ALERTS_TABLE


# Fonction pour envoyer l'alerte par email
def send_alert(transaction_id, amount, merchant, category, model_name):
    if not cfg.smtp_enabled:
        logger.info("SMTP_ENABLED=false -> alert e-mail skipped.")
        return False
    try:
        msg = MIMEMultipart()
        msg['From'] = cfg.ALERT_FROM or cfg.SMTP_USER
        msg['To'] = cfg.ALERT_TO
        msg['Subject'] = f"🚨 Fraud Alert: Transaction {transaction_id}"

        body = f"""
        🚨 Fraudulent Transaction Detected 🚨
        
        - ID: {transaction_id}
        - Amount: ${amount:.2f}
        - Merchant: {merchant}
        - Category: {category}
        - Model: {model_name}
        
        This transaction was flagged as potentially fraudulent.
        Please review it immediately.
        
        -- 
        Fraud Detection System
        """
        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(cfg.SMTP_HOST, int(cfg.SMTP_PORT)) as server:
            server.starttls()
            server.login(msg['From'], cfg.SMTP_PASSWORD)
            server.sendmail(msg['From'], msg['To'], msg.as_string())

        logger.info(f"✅ Alert sent for transaction {transaction_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to send alert for transaction {transaction_id}: {str(e)}")
        return False


# Création de la session Spark avec configuration de mémoire
spark = SparkSession.builder \
    .appName(cfg.SPARK_APP_NAME) \
    .config("spark.executor.memory", cfg.SPARK_EXECUTOR_MEMORY) \
    .config("spark.driver.memory", cfg.SPARK_DRIVER_MEMORY) \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Schéma pour les données de transaction
schema = StructType() \
    .add("trans_date_trans_time", StringType()) \
    .add("cc_num", StringType()) \
    .add("merchant", StringType()) \
    .add("category", StringType()) \
    .add("amt", DoubleType()) \
    .add("first", StringType()) \
    .add("last", StringType()) \
    .add("gender", StringType()) \
    .add("street", StringType()) \
    .add("city", StringType()) \
    .add("state", StringType()) \
    .add("zip", IntegerType()) \
    .add("lat", DoubleType()) \
    .add("long", DoubleType()) \
    .add("city_pop", IntegerType()) \
    .add("job", StringType()) \
    .add("dob", StringType()) \
    .add("trans_num", StringType()) \
    .add("unix_time", LongType()) \
    .add("merch_lat", DoubleType()) \
    .add("merch_long", DoubleType()) \
    .add("is_fraud", IntegerType())

# Liste des colonnes que tu veux conserver dans ta table Cassandra
selected_columns = [
    "trans_date_trans_time", "cc_num", "merchant", "category", "amt",
    "first", "last", "gender", "street", "city", "state", "zip",
    "lat", "long", "city_pop", "job", "dob", "transaction_id", "unix_time",
    "merch_lat", "merch_long", "prediction", "model"
]


# UDFs pour les alertes
@udf(returnType=StringType())
def generate_uuid():
    return str(uuid.uuid4())


@udf(returnType=StringType())
def determine_alert_type(prediction, amount):
    if prediction == 1:
        if amount > 1000:
            return "HIGH_VALUE_FRAUD"
        else:
            return "SUSPECTED_FRAUD"
    return None


@udf(returnType=DoubleType())
def calculate_risk_score(prediction, amount):
    if prediction == 1:
        base_score = 0.8
        amount_factor = min(0.2, amount / 10000)
        return base_score + amount_factor
    return 0.0


# Lecture du flux de Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", cfg.KAFKA_EXTERNAL_BOOTSTRAP_SERVERS) \
    .option("subscribe", cfg.KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()

# Parse du JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Nettoyage des données
clean_df = json_df.na.fill(0) \
    .withColumn("amt", when(isnan("amt") | col("amt").isNull(), 0.0).otherwise(col("amt"))) \
    .withColumn("lat", when(isnan("lat") | col("lat").isNull(), 0.0).otherwise(col("lat"))) \
    .withColumn("long", when(isnan("long") | col("long").isNull(), 0.0).otherwise(col("long"))) \
    .withColumn("merch_lat", when(isnan("merch_lat") | col("merch_lat").isNull(), 0.0).otherwise(col("merch_lat"))) \
    .withColumn("merch_long", when(isnan("merch_long") | col("merch_long").isNull(), 0.0).otherwise(col("merch_long")))

# Dictionnaire des modèles à charger (configurables via .env)
AVAILABLE_MODELS = {
    "GBT": str(project_path("MODEL_GBT_PATH")),
    "RF": str(project_path("MODEL_RF_PATH")),
    "LR": str(project_path("MODEL_LR_PATH")),
}
models = {name: path for name, path in AVAILABLE_MODELS.items()
          if name in get_list("MODELS_TO_APPLY", "GBT")}


# Fonction pour appliquer les modèles sur les lignes de données
def apply_models_to_rows(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        logger.info("Pas de données à traiter dans ce batch.")
        return

    # Ajouter la colonne transaction_id en amont avant d'appliquer les modèles
    current_batch = batch_df.withColumn("transaction_id", col("trans_num"))

    all_predictions = []
    fraud_predictions = []

    for model_name, model_path in models.items():
        try:
            logger.info(f"Chargement du modèle {model_name} depuis {model_path}")
            pipeline_model = PipelineModel.load(model_path)
            vector_assembler = pipeline_model.stages[0]
            required_cols = vector_assembler.getInputCols()

            model_batch = current_batch
            for colname in required_cols:
                if colname not in model_batch.columns:
                    model_batch = model_batch.withColumn(colname, F.lit(0.0))

            logger.info(f"Application du modèle {model_name} sur les données.")
            pred_df = pipeline_model.transform(model_batch)

            model_predictions = pred_df.withColumn("model", F.lit(model_name))
            model_predictions = model_predictions.select(*selected_columns)

            fraud_df = model_predictions.filter(col("prediction") == 1)
            if not fraud_df.isEmpty():
                fraud_predictions.append(fraud_df)

                fraud_rows = fraud_df.collect()
                if get_bool("SMTP_ENABLED", False):
                    for row in fraud_rows:
                        send_alert(
                            transaction_id=row["transaction_id"],
                            amount=row["amt"],
                            merchant=row["merchant"],
                            category=row["category"],
                            model_name=model_name
                        )

            all_predictions.append(model_predictions)

            logger.info(f"Sauvegarde des prédictions du modèle {model_name} dans Cassandra.")
            model_predictions.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table=TABLE_TRANSACTIONS, keyspace=KEYSPACE) \
                .save()

        except Exception as e:
            logger.error(f"Erreur lors de l'application du modèle {model_name}: {e}")

    if fraud_predictions:
        try:
            all_fraud_df = reduce(DataFrame.unionByName, fraud_predictions)
            logger.info(f"Génération d'alertes pour {all_fraud_df.count()} transactions frauduleuses détectées.")

            alerts_df = all_fraud_df.select(
                generate_uuid().alias("alert_id"),
                col("transaction_id"),
                col("cc_num"),
                col("amt"),
                col("merchant"),
                determine_alert_type(col("prediction"), col("amt")).alias("alert_type"),
                calculate_risk_score(col("prediction"), col("amt")).alias("risk_score"),
                lit("NEW").alias("status")
            )

            logger.info("Sauvegarde des alertes dans Cassandra.")
            alerts_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .mode("append") \
                .options(table=TABLE_ALERTS, keyspace=KEYSPACE) \
                .save()

            logger.info(f"Enregistré {alerts_df.count()} alertes dans Cassandra.")

        except Exception as e:
            logger.error(f"Erreur lors de la génération des alertes: {e}")

    if all_predictions:
        try:
            logger.info("Combinaison des prédictions des différents modèles.")
            final_predictions = reduce(DataFrame.unionByName, all_predictions)
        except Exception as e:
            logger.error(f"Erreur lors de la combinaison des prédictions: {e}")


# Démarrer la requête de streaming
logger.info("Démarrage du traitement en streaming.")
query = clean_df.writeStream \
    .foreachBatch(apply_models_to_rows) \
    .outputMode("append") \
    .option("checkpointLocation", cfg.SPARK_CHECKPOINT_DIR) \
    .start()

query.awaitTermination()
