import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Consumer, KafkaError, Producer


SRC_PATH = Path(__file__).resolve().parents[1] / "src"
sys.path.insert(0, str(SRC_PATH))

from preprocessing import load_preprocessing_artifact, run_preproc
from scorer import make_pred


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "ml-scorer")
RETRY_DELAY_SECONDS = float(os.getenv("KAFKA_RETRY_DELAY_SECONDS", "3"))
DELIVERY_TIMEOUT_SECONDS = float(os.getenv("KAFKA_DELIVERY_TIMEOUT_SECONDS", "10"))
BATCH_SIZE = int(os.getenv("SCORING_BATCH_SIZE", "500"))
BATCH_WAIT_SECONDS = float(os.getenv("SCORING_BATCH_WAIT_SECONDS", "1"))


class InvalidTransactionMessage(ValueError):
    """Raised when a Kafka message does not follow the transaction contract."""


def deserialize_transaction(payload):
    try:
        message = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise InvalidTransactionMessage("Message is not valid UTF-8 JSON") from error

    if not isinstance(message, dict):
        raise InvalidTransactionMessage("Message must be a JSON object")

    transaction_id = message.get("transaction_id")
    transaction_data = message.get("data")
    if not isinstance(transaction_id, str) or not transaction_id.strip():
        raise InvalidTransactionMessage(
            "transaction_id must be a non-empty string"
        )
    if not isinstance(transaction_data, dict):
        raise InvalidTransactionMessage("data must be a JSON object")

    return transaction_id, transaction_data


def build_score_messages(transactions, artifact):
    transaction_ids = [transaction_id for transaction_id, _ in transactions]
    input_dataframe = pd.DataFrame([data for _, data in transactions])
    processed_dataframe = run_preproc(artifact, input_dataframe)
    predictions = make_pred(processed_dataframe, "kafka_batch").reset_index(drop=True)
    return [
        {
            "transaction_id": transaction_id,
            "score": float(prediction.score),
            "fraud_flag": int(prediction.fraud_flag),
        }
        for transaction_id, prediction in zip(
            transaction_ids,
            predictions.itertuples(index=False),
        )
    ]


class ProcessingService:
    def __init__(self):
        self.running = True
        self.preprocessing_artifact = load_preprocessing_artifact()
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
        self.producer = Producer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "enable.idempotence": True,
                "acks": "all",
                "allow.auto.create.topics": False,
            }
        )
        self.consumer.subscribe([TRANSACTIONS_TOPIC])

    def stop(self, *_):
        logger.info("Shutdown requested")
        self.running = False

    def publish_scores(self, score_messages):
        delivered_count = 0
        delivery_errors = []

        def delivery_callback(error, _message):
            nonlocal delivered_count
            delivered_count += 1
            if error is not None:
                delivery_errors.append(error)

        for score_message in score_messages:
            serialized = json.dumps(
                score_message,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8")
            while True:
                try:
                    self.producer.produce(
                        SCORES_TOPIC,
                        key=score_message["transaction_id"].encode("utf-8"),
                        value=serialized,
                        on_delivery=delivery_callback,
                    )
                    break
                except BufferError:
                    self.producer.poll(1)

        remaining = self.producer.flush(DELIVERY_TIMEOUT_SECONDS)
        if remaining or delivered_count != len(score_messages):
            raise RuntimeError("Timed out while delivering score batch to Kafka")
        if delivery_errors:
            raise RuntimeError(f"Kafka rejected score message: {delivery_errors[0]}")

    def process_batch(self, messages):
        transactions = []
        invalid_count = 0
        for message in messages:
            try:
                transactions.append(deserialize_transaction(message.value()))
            except (InvalidTransactionMessage, ValueError, TypeError) as error:
                invalid_count += 1
                logger.warning(
                    "Skipping invalid transaction at partition=%s offset=%s: %s",
                    message.partition(),
                    message.offset(),
                    error,
                )

        score_messages = []
        if transactions:
            score_messages = build_score_messages(
                transactions,
                self.preprocessing_artifact,
            )
            self.publish_scores(score_messages)

        for message in messages:
            self.consumer.store_offsets(message=message)
        self.consumer.commit(asynchronous=False)
        logger.info(
            "Batch completed: received=%d, scored=%d, invalid=%d",
            len(messages),
            len(score_messages),
            invalid_count,
        )

    def process_messages(self):
        logger.info(
            "Consuming %s and publishing scores to %s via %s",
            TRANSACTIONS_TOPIC,
            SCORES_TOPIC,
            KAFKA_BOOTSTRAP_SERVERS,
        )
        while self.running:
            messages = self.consumer.consume(
                num_messages=BATCH_SIZE,
                timeout=BATCH_WAIT_SECONDS,
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
                        "Temporary scoring failure for batch of %d; retrying in %.1f seconds",
                        len(valid_messages),
                        RETRY_DELAY_SECONDS,
                    )
                    time.sleep(RETRY_DELAY_SECONDS)

    def close(self):
        logger.info("Closing Kafka clients")
        self.producer.flush(DELIVERY_TIMEOUT_SECONDS)
        self.consumer.close()


def main():
    logger.info("Starting Kafka ML scoring service")
    service = ProcessingService()
    signal.signal(signal.SIGTERM, service.stop)
    signal.signal(signal.SIGINT, service.stop)
    try:
        service.process_messages()
    finally:
        service.close()
        logger.info("Kafka ML scoring service stopped")


if __name__ == "__main__":
    main()
