import json
import os
import uuid

import pandas as pd
import psycopg2
import streamlit as st
from kafka import KafkaConsumer, KafkaProducer, TopicPartition


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "transactions")
KAFKA_SCORER_GROUP_ID = os.getenv("KAFKA_SCORER_GROUP_ID", "ml-scorer")
KAFKA_SCORES_TOPIC = os.getenv("KAFKA_SCORES_TOPIC", "scores")
KAFKA_WRITER_GROUP_ID = os.getenv("KAFKA_WRITER_GROUP_ID", "score-writer")
KAFKA_SEND_BATCH_SIZE = int(os.getenv("KAFKA_SEND_BATCH_SIZE", "1000"))
POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "fraud_detection"),
    "user": os.getenv("POSTGRES_USER", "fraud_user"),
    "password": os.getenv("POSTGRES_PASSWORD", "fraud_password"),
    "connect_timeout": int(os.getenv("POSTGRES_CONNECT_TIMEOUT", "5")),
    "application_name": "fraud_interface",
}

LATEST_FRAUDS_SQL = """
    SELECT transaction_id, score, fraud_flag, created_at
    FROM transaction_scores
    WHERE fraud_flag = 1
    ORDER BY created_at DESC
    LIMIT %s
"""

RECENT_SCORES_SQL = """
    SELECT score
    FROM transaction_scores
    ORDER BY created_at DESC
    LIMIT %s
"""

RESULT_COUNT_SQL = "SELECT COUNT(*) FROM transaction_scores"

REQUIRED_COLUMNS = [
    "transaction_time",
    "merch",
    "cat_id",
    "amount",
    "name_1",
    "name_2",
    "gender",
    "street",
    "one_city",
    "us_state",
    "post_code",
    "lat",
    "lon",
    "population_city",
    "jobs",
    "merchant_lat",
    "merchant_lon",
]

NUMERIC_COLUMNS = [
    "amount",
    "post_code",
    "lat",
    "lon",
    "population_city",
    "merchant_lat",
    "merchant_lon",
]


class TransactionSendError(RuntimeError):
    def __init__(self, sent_count, total_count, original_error):
        super().__init__(
            f"Sent {sent_count} of {total_count} transactions: {original_error}"
        )
        self.sent_count = sent_count
        self.total_count = total_count
        self.original_error = original_error


def load_csv(uploaded_file):
    return pd.read_csv(uploaded_file)


def validate_transactions(dataframe):
    errors = []
    warnings = []

    if dataframe.empty:
        errors.append("CSV-файл не содержит транзакций.")
        return errors, warnings

    missing_columns = [
        column for column in REQUIRED_COLUMNS if column not in dataframe.columns
    ]
    if missing_columns:
        errors.append(
            "Отсутствуют обязательные колонки: " + ", ".join(missing_columns)
        )
        return errors, warnings

    extra_columns = [
        column for column in dataframe.columns if column not in REQUIRED_COLUMNS
    ]
    if extra_columns:
        warnings.append(
            "Дополнительные колонки не будут отправлены: "
            + ", ".join(extra_columns)
        )

    required_data = dataframe[REQUIRED_COLUMNS]
    missing_values = required_data.isna().sum()
    columns_with_missing = [
        f"{column} ({int(count)})"
        for column, count in missing_values.items()
        if count
    ]
    if columns_with_missing:
        errors.append(
            "Обнаружены пустые значения: " + ", ".join(columns_with_missing)
        )

    invalid_dates = pd.to_datetime(
        dataframe["transaction_time"], errors="coerce"
    ).isna()
    if invalid_dates.any():
        errors.append(
            f"Некорректный transaction_time в {int(invalid_dates.sum())} строках."
        )

    invalid_numeric = []
    for column in NUMERIC_COLUMNS:
        converted = pd.to_numeric(dataframe[column], errors="coerce")
        invalid_count = int(converted.isna().sum())
        if invalid_count:
            invalid_numeric.append(f"{column} ({invalid_count})")
    if invalid_numeric:
        errors.append(
            "Некорректные числовые значения: " + ", ".join(invalid_numeric)
        )

    duplicate_count = int(required_data.duplicated().sum())
    if duplicate_count:
        warnings.append(
            f"Обнаружено полных дублей транзакций: {duplicate_count}."
        )

    return errors, warnings


def prepare_records(dataframe):
    prepared = dataframe[REQUIRED_COLUMNS].copy()
    for column in NUMERIC_COLUMNS:
        prepared[column] = pd.to_numeric(prepared[column], errors="raise")
    for values in prepared.itertuples(index=False, name=None):
        yield dict(zip(REQUIRED_COLUMNS, values))


def build_transaction_messages(dataframe, id_factory=uuid.uuid4):
    for record in prepare_records(dataframe):
        transaction_id = str(id_factory())
        yield {
            "transaction_id": transaction_id,
            "data": record,
        }


def send_to_kafka(
    dataframe,
    topic=KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    progress_callback=None,
    producer_factory=KafkaProducer,
    send_batch_size=KAFKA_SEND_BATCH_SIZE,
):
    total_count = len(dataframe)
    sent_count = 0
    producer = None
    pending = []
    progress_step = max(total_count // 200, 1)

    def confirm_pending():
        nonlocal sent_count
        for future in pending:
            future.get(timeout=30)
            sent_count += 1
            if progress_callback is not None and (
                sent_count % progress_step == 0 or sent_count == total_count
            ):
                progress_callback(sent_count, total_count)
        pending.clear()

    try:
        producer = producer_factory(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda value: value.encode("utf-8"),
            value_serializer=lambda value: json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                separators=(",", ":"),
            ).encode("utf-8"),
            security_protocol="PLAINTEXT",
            acks="all",
            retries=5,
            linger_ms=20,
            max_block_ms=10000,
            request_timeout_ms=30000,
        )
        for message in build_transaction_messages(dataframe):
            pending.append(
                producer.send(
                    topic,
                    key=message["transaction_id"],
                    value=message,
                )
            )
            if len(pending) >= send_batch_size:
                confirm_pending()
        confirm_pending()
        producer.flush(timeout=30)
    except Exception as error:
        raise TransactionSendError(
            sent_count,
            total_count,
            error,
        ) from error
    finally:
        if producer is not None:
            producer.close(timeout=10)

    return sent_count


def fetch_results(
    connection_factory=psycopg2.connect,
    postgres_config=None,
    fraud_limit=10,
    score_limit=100,
):
    connection = connection_factory(**(postgres_config or POSTGRES_CONFIG))
    try:
        with connection.cursor() as cursor:
            cursor.execute(LATEST_FRAUDS_SQL, (fraud_limit,))
            fraud_rows = cursor.fetchall()
            cursor.execute(RECENT_SCORES_SQL, (score_limit,))
            score_rows = cursor.fetchall()
            cursor.execute(RESULT_COUNT_SQL)
            result_count = cursor.fetchone()[0]
    finally:
        connection.close()

    frauds = pd.DataFrame(
        fraud_rows,
        columns=["transaction_id", "score", "fraud_flag", "created_at"],
    )
    scores = pd.DataFrame(score_rows, columns=["score"])
    return frauds, scores, result_count


def fetch_kafka_status(topic, group_id, consumer_factory=KafkaConsumer):
    consumer = consumer_factory(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        enable_auto_commit=False,
    )
    try:
        partitions = consumer.partitions_for_topic(topic) or set()
        topic_partitions = [
            TopicPartition(topic, partition) for partition in partitions
        ]
        end_offsets = consumer.end_offsets(topic_partitions)
        total_count = sum(end_offsets.values())
        processed_count = sum(
            min(consumer.committed(topic_partition) or 0, end_offsets[topic_partition])
            for topic_partition in topic_partitions
        )
    finally:
        consumer.close()
    return total_count, processed_count, total_count - processed_count


def build_score_histogram(scores, bin_count=10):
    edges = [index / bin_count for index in range(bin_count + 1)]
    labels = [
        f"{edges[index]:.1f}-{edges[index + 1]:.1f}"
        for index in range(bin_count)
    ]
    score_bins = pd.cut(
        scores["score"],
        bins=edges,
        labels=labels,
        include_lowest=True,
    )
    counts = score_bins.value_counts(sort=False).reindex(labels, fill_value=0)
    return pd.DataFrame(
        {
            "Диапазон score": labels,
            "Количество транзакций": counts.astype(int).tolist(),
        }
    )


def render_upload_tab():
    st.subheader("Отправка транзакций")

    uploaded_file = st.file_uploader(
        "CSV-файл с транзакциями",
        type=["csv"],
    )
    if uploaded_file is None:
        return

    try:
        dataframe = load_csv(uploaded_file)
    except Exception as error:
        st.error(f"Не удалось прочитать CSV: {error}")
        return

    errors, warnings = validate_transactions(dataframe)
    metric_columns = st.columns(2)
    metric_columns[0].metric("Строк", len(dataframe))
    metric_columns[1].metric("Колонок", len(dataframe.columns))

    for warning in warnings:
        st.warning(warning)
    for error in errors:
        st.error(error)

    st.dataframe(dataframe.head(10), use_container_width=True, hide_index=True)

    if errors:
        return

    if st.button("Отправить в Kafka", type="primary"):
        progress_bar = st.progress(0, text="Подготовка отправки")

        def update_progress(sent_count, total_count):
            progress_bar.progress(
                sent_count / total_count,
                text=f"Отправлено {sent_count} из {total_count}",
            )

        try:
            sent_count = send_to_kafka(
                dataframe,
                progress_callback=update_progress,
            )
        except TransactionSendError as error:
            st.error(
                f"Отправка остановлена после {error.sent_count} из "
                f"{error.total_count} транзакций: {error.original_error}"
            )
            return

        st.success(f"В Kafka отправлено транзакций: {sent_count}")


def render_results_tab():
    st.subheader("Результаты скоринга")

    if not st.button("Посмотреть результаты", type="primary"):
        return

    try:
        frauds, scores, result_count = fetch_results()
    except Exception as error:
        st.error(f"Не удалось получить данные из PostgreSQL: {error}")
        return

    try:
        kafka_total, kafka_processed, kafka_lag = fetch_kafka_status(
            KAFKA_TOPIC,
            KAFKA_SCORER_GROUP_ID,
        )
        _, _, writer_lag = fetch_kafka_status(
            KAFKA_SCORES_TOPIC,
            KAFKA_WRITER_GROUP_ID,
        )
    except Exception:
        kafka_total = kafka_processed = kafka_lag = writer_lag = None

    status_columns = st.columns(4)
    status_columns[0].metric(
        "Всего транзакций",
        kafka_total if kafka_total is not None else "—",
    )
    status_columns[1].metric(
        "Скоринг завершён",
        kafka_processed if kafka_processed is not None else "—",
    )
    status_columns[2].metric("Записано в PostgreSQL", result_count)
    status_columns[3].metric(
        "Осталось записать",
        writer_lag if writer_lag is not None else "—",
    )
    if kafka_lag:
        st.caption(f"В очереди на скоринг осталось: {kafka_lag}.")

    st.markdown("#### Топ-10 последних транзакций с `fraud_flag = 1`")
    if frauds.empty:
        st.info("Транзакции с fraud_flag = 1 пока отсутствуют.")
    else:
        frauds_to_display = frauds.copy()
        frauds_to_display["score"] = frauds_to_display["score"].round(6)
        st.dataframe(
            frauds_to_display,
            use_container_width=True,
            hide_index=True,
        )

    st.markdown("#### Распределение 100 последних скоров")
    if scores.empty:
        st.info("В базе пока нет результатов скоринга.")
        return

    histogram = build_score_histogram(scores)
    st.bar_chart(
        histogram,
        x="Диапазон score",
        y="Количество транзакций",
        x_label="Score",
        y_label="Количество транзакций",
    )


def render_app():
    st.set_page_config(page_title="Fraud scoring", layout="wide")
    st.title("Fraud scoring")
    upload_tab, results_tab = st.tabs(["Отправка", "Результаты"])

    with upload_tab:
        render_upload_tab()
    with results_tab:
        render_results_tab()


if __name__ == "__main__":
    render_app()
