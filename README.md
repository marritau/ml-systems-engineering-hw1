# Real-Time Fraud Detection

Сервис потокового inference для скоринга транзакций. Транзакции поступают через Kafka,
обрабатываются CatBoost-моделью на CPU и сохраняются в PostgreSQL.

## Архитектура

```text
CSV -> Streamlit -> Kafka: transactions -> fraud_detector
                                      -> Kafka: scores -> score_writer -> PostgreSQL
                                                                    -> Streamlit
```

- `interface` загружает CSV в `transactions` и показывает результаты.
- `fraud_detector` читает `transactions`, выполняет препроцессинг и скоринг, затем пишет в `scores`.
- `score_writer` читает `scores` и записывает результаты в PostgreSQL.
- `database/init` создаёт таблицу `transaction_scores` при первом запуске.

Сообщение в `scores` содержит три поля:

```json
{
  "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a",
  "score": 0.995,
  "fraud_flag": 1
}
```

## Запуск

Требуется Docker Desktop с Docker Compose. Python и датасет Kaggle для запуска не нужны.

```bash
git clone https://github.com/marritau/ml-systems-engineering-hw1.git
cd ml-systems-engineering-hw1
docker compose up --build -d
```

Первый запуск занимает несколько минут. Проверить состояние:

```bash
docker compose ps
```

Kafka, ZooKeeper и PostgreSQL должны иметь статус `healthy`. `kafka-setup` создаёт топики один раз и
завершается с кодом `0`.

## Проверка

1. Открыть Streamlit: [http://localhost:8501](http://localhost:8501).
2. Во вкладке `Отправка` загрузить `sample_data/sample_test.csv`.
3. Нажать `Отправить в Kafka` и дождаться отправки 10 транзакций.
4. Во вкладке `Результаты` нажать `Посмотреть результаты`.

После обработки интерфейс покажет:

- `Всего транзакций: 10`;
- `Скоринг завершён: 10`;
- `Записано в PostgreSQL: 10`;
- `Осталось записать: 0`;
- до 10 последних транзакций с `fraud_flag = 1`;
- гистограмму скоров всех 10 транзакций.

`sample_test.csv` содержит 10 синтетических транзакций с теми же 17 колонками, что и в конкурсном
`test.csv`. Он не содержит строк из Kaggle. Полный `test.csv` также можно загрузить через интерфейс;
датасеты Kaggle в репозиторий не включены.

Kafka UI: [http://localhost:8080](http://localhost:8080).

## Модель и препроцессинг

Обучение в сервисе не выполняется. Контейнер `fraud_detector` содержит:

- обученную CatBoost-модель `fraud_detector/models/my_catboost.cbm`;
- preprocessing-код `fraud_detector/src/preprocessing.py`;
- CPU-скоринг `fraud_detector/src/scorer.py`;
- артефакт `fraud_detector/artifacts/preprocessing.json`.

Артефакт хранит словари категорий, mean encoding, средние числовых признаков и порядок колонок.
Поэтому `train.csv` не нужен во время inference. Модель запускается только на CPU.

## Остановка

```bash
docker compose down
```

Команда останавливает контейнеры, но сохраняет PostgreSQL volume. Для полного сброса локальных данных:

```bash
docker compose down -v
```
