import json
import logging
import math
import os
import signal
import time

import psycopg2
from confluent_kafka import Consumer, KafkaError
from psycopg2 import InterfaceError, OperationalError
from psycopg2.extras import execute_values


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "score-writer")
RETRY_DELAY_SECONDS = float(os.getenv("RETRY_DELAY_SECONDS", "3"))
WRITE_BATCH_SIZE = int(os.getenv("WRITE_BATCH_SIZE", "1000"))
WRITE_BATCH_WAIT_SECONDS = float(os.getenv("WRITE_BATCH_WAIT_SECONDS", "1"))

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud_detection"),
    "user": os.getenv("POSTGRES_USER", "fraud_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud_password"),
    "connect_timeout": int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "5")),
    "application_name": "score_writer",
}

UPSERT_SCORES_SQL = """
    INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
    VALUES %s
    ON CONFLICT (transaction_id) DO UPDATE
    SET score = EXCLUDED.score,
        fraud_flag = EXCLUDED.fraud_flag,
        created_at = CURRENT_TIMESTAMP
"""


class InvalidScoreMessage(ValueError):
    """Raised when a Kafka message does not follow the score contract."""


def deserialize_score(payload):
    try:
        message = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidScoreMessage("Message is not valid UTF-8 JSON") from error

    if not isinstance(message, dict):
        raise InvalidScoreMessage("Message must be a JSON object")

    required_fields = {"transaction_id", "score", "fraud_flag"}
    if set(message) != required_fields:
        raise InvalidScoreMessage(
            "Message must contain exactly transaction_id, score and fraud_flag"
        )

    transaction_id = message["transaction_id"]
    score = message["score"]
    fraud_flag = message["fraud_flag"]

    if not isinstance(transaction_id, str) or not transaction_id.strip():
        raise InvalidScoreMessage(
            "transaction_id must be a non-empty string"
        )
    if isinstance(score, bool) or not isinstance(score, (int, float)):
        raise InvalidScoreMessage("score must be a number")
    if not math.isfinite(score) or not 0 <= score <= 1:
        raise InvalidScoreMessage("score must be between 0 and 1")
    if isinstance(fraud_flag, bool) or not isinstance(fraud_flag, int):
        raise InvalidScoreMessage("fraud_flag must be an integer")
    if fraud_flag not in (0, 1):
        raise InvalidScoreMessage("fraud_flag must be 0 or 1")

    return {
        "transaction_id": transaction_id,
        "score": float(score),
        "fraud_flag": fraud_flag,
    }


class ScoreRepository:
    def __init__(self, connection_factory=psycopg2.connect, config=None):
        self.connection_factory = connection_factory
        self.config = config or POSTGRES_CONFIG
        self.connection = None

    def ensure_connection(self):
        if self.connection is None or self.connection.closed:
            self.connection = self.connection_factory(**self.config)
        return self.connection

    def reset_connection(self):
        if self.connection is not None and not self.connection.closed:
            self.connection.close()
        self.connection = None

    def ping(self):
        connection = self.ensure_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            connection.commit()
        except (OperationalError, InterfaceError):
            self.reset_connection()
            raise

    def upsert_many(self, score_messages):
        connection = self.ensure_connection()
        try:
            with connection.cursor() as cursor:
                execute_values(
                    cursor,
                    UPSERT_SCORES_SQL,
                    [
                        (
                            message["transaction_id"],
                            message["score"],
                            message["fraud_flag"],
                        )
                        for message in score_messages
                    ],
                )
            connection.commit()
        except (OperationalError, InterfaceError):
            self.reset_connection()
            raise
        except Exception:
            connection.rollback()
            raise

    def close(self):
        self.reset_connection()


class ScoreWriterService:
    def __init__(self, repository=None):
        self.running = True
        self.repository = repository or ScoreRepository()
        self.consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": KAFKA_GROUP_ID,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "allow.auto.create.topics": False,
            }
        )
        self.consumer.subscribe([SCORES_TOPIC])

    def stop(self, *_):
        logger.info("Shutdown requested")
        self.running = False

    def wait_for_database(self):
        while self.running:
            try:
                self.repository.ping()
                logger.info("Connected to PostgreSQL")
                return
            except (OperationalError, InterfaceError):
                logger.warning(
                    "PostgreSQL is unavailable; retrying in %.1f seconds",
                    RETRY_DELAY_SECONDS,
                )
                time.sleep(RETRY_DELAY_SECONDS)

    def process_batch(self, messages):
        score_messages = []
        invalid_count = 0
        for message in messages:
            try:
                score_messages.append(deserialize_score(message.value()))
            except InvalidScoreMessage as error:
                invalid_count += 1
                logger.warning(
                    "Skipping invalid score at partition=%s offset=%s: %s",
                    message.partition(),
                    message.offset(),
                    error,
                )

        if score_messages:
            self.repository.upsert_many(score_messages)
        for message in messages:
            self.consumer.store_offsets(message=message)
        self.consumer.commit(asynchronous=False)
        logger.info(
            "Batch stored: received=%d, written=%d, invalid=%d",
            len(messages),
            len(score_messages),
            invalid_count,
        )

    def process_messages(self):
        self.wait_for_database()
        logger.info(
            "Consuming %s via %s and writing to PostgreSQL",
            SCORES_TOPIC,
            KAFKA_BOOTSTRAP_SERVERS,
        )
        while self.running:
            messages = self.consumer.consume(
                num_messages=WRITE_BATCH_SIZE,
                timeout=WRITE_BATCH_WAIT_SECONDS,
            )
            if not messages:
                continue

            valid_messages = []
            for message in messages:
                if not message.error():
                    valid_messages.append(message)
                elif message.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug("Reached the end of a Kafka partition")
                else:
                    logger.error("Kafka consumer error: %s", message.error())
            if not valid_messages:
                continue

            while self.running:
                try:
                    self.process_batch(valid_messages)
                    break
                except Exception:
                    logger.exception(
                        "Failed to store batch of %d scores; retrying in %.1f seconds",
                        len(valid_messages),
                        RETRY_DELAY_SECONDS,
                    )
                    time.sleep(RETRY_DELAY_SECONDS)

    def close(self):
        logger.info("Closing PostgreSQL and Kafka clients")
        self.repository.close()
        self.consumer.close()


def main():
    logger.info("Starting score writer service")
    service = ScoreWriterService()
    signal.signal(signal.SIGTERM, service.stop)
    signal.signal(signal.SIGINT, service.stop)
    try:
        service.process_messages()
    finally:
        service.close()
        logger.info("Score writer service stopped")


if __name__ == "__main__":
    main()
