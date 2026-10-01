import argparse
import json
import logging
from pathlib import Path

import numpy as np
import pandas as pd
from geopy.distance import great_circle


LOGGER = logging.getLogger(__name__)

TARGET_COLUMN = "target"
CATEGORICAL_COLUMNS = [
    "gender",
    "merch",
    "cat_id",
    "one_city",
    "us_state",
    "jobs",
]
TIME_COLUMNS = ["hour", "year", "month", "day_of_month", "day_of_week"]
CONTINUOUS_COLUMNS = ["amount", "population_city", "distance"]
DROP_COLUMNS = ["name_1", "name_2", "street", "post_code"]
COORDINATE_COLUMNS = ["lat", "lon", "merchant_lat", "merchant_lon"]
MAX_FREQUENT_CATEGORIES = 50

FEATURE_ORDER = [
    "hour",
    "year",
    "month",
    "day_of_month",
    "day_of_week",
    "gender_cat",
    "merch_cat",
    "cat_id_cat",
    "one_city_cat",
    "us_state_cat",
    "jobs_cat",
    "gender_cat_mean_enc",
    "merch_cat_mean_enc",
    "cat_id_cat_mean_enc",
    "one_city_cat_mean_enc",
    "us_state_cat_mean_enc",
    "jobs_cat_mean_enc",
    "hour_mean_enc",
    "year_mean_enc",
    "month_mean_enc",
    "day_of_month_mean_enc",
    "day_of_week_mean_enc",
    "amount_log",
    "population_city_log",
    "distance_log",
]


def add_time_features(dataframe):
    transaction_time = pd.to_datetime(dataframe["transaction_time"], errors="raise")
    dataframe["hour"] = transaction_time.dt.hour
    dataframe["year"] = transaction_time.dt.year
    dataframe["month"] = transaction_time.dt.month
    dataframe["day_of_month"] = transaction_time.dt.day
    dataframe["day_of_week"] = transaction_time.dt.dayofweek
    return dataframe.drop(columns="transaction_time")


def add_distance(dataframe):
    dataframe["distance"] = dataframe.apply(
        lambda row: great_circle(
            (row["lat"], row["lon"]),
            (row["merchant_lat"], row["merchant_lon"]),
        ).km,
        axis=1,
    )
    return dataframe.drop(columns=COORDINATE_COLUMNS)


def build_category_mapping(train, column):
    encoded_column = f"{column}_cat"
    frequencies = (
        train.groupby(column, dropna=False)[[TARGET_COLUMN]]
        .count()
        .sort_values(TARGET_COLUMN, ascending=False)
        .reset_index()
        .set_axis([column, "count"], axis=1)
        .reset_index()
    )
    frequencies["index"] = frequencies.apply(
        lambda row: np.nan if pd.isna(row[column]) else row["index"],
        axis=1,
    )
    frequencies[encoded_column] = [
        "cat_NAN"
        if pd.isna(index)
        else f"cat_{index}"
        if index < MAX_FREQUENT_CATEGORIES
        else f"cat_{MAX_FREQUENT_CATEGORIES}+"
        for index in frequencies["index"]
    ]
    return {
        str(source): str(encoded)
        for source, encoded in frequencies[[column, encoded_column]].itertuples(index=False)
        if not pd.isna(source)
    }


def build_artifact(train_path):
    LOGGER.info("Reading training data from %s", train_path)
    raw_train = pd.read_csv(train_path)
    required_input_columns = [
        column for column in raw_train.columns if column != TARGET_COLUMN
    ]

    train = raw_train.drop(columns=DROP_COLUMNS).copy()
    train = add_time_features(train)

    category_mappings = {}
    for column in CATEGORICAL_COLUMNS:
        encoded_column = f"{column}_cat"
        mapping = build_category_mapping(train, column)
        category_mappings[column] = mapping
        train[encoded_column] = train[column].map(mapping).fillna("cat_NAN")

    mean_encodings = {}
    encoded_columns = [f"{column}_cat" for column in CATEGORICAL_COLUMNS]
    for column in encoded_columns + TIME_COLUMNS:
        means = train.groupby(column, dropna=False)[TARGET_COLUMN].mean()
        mean_encodings[column] = {
            str(key): float(value)
            for key, value in means.items()
            if not pd.isna(key)
        }

    train = add_distance(train)
    continuous_means = {
        column: float(train[column].mean()) for column in CONTINUOUS_COLUMNS
    }

    return {
        "version": 1,
        "required_input_columns": required_input_columns,
        "drop_columns": DROP_COLUMNS,
        "categorical_columns": CATEGORICAL_COLUMNS,
        "time_columns": TIME_COLUMNS,
        "continuous_columns": CONTINUOUS_COLUMNS,
        "category_mappings": category_mappings,
        "mean_encodings": mean_encodings,
        "continuous_means": continuous_means,
        "default_mean_encoding": float(raw_train[TARGET_COLUMN].mean()),
        "feature_order": FEATURE_ORDER,
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Build compact preprocessing statistics for inference."
    )
    parser.add_argument("--train-path", type=Path, required=True)
    parser.add_argument("--output-path", type=Path, required=True)
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    args = parse_args()
    artifact = build_artifact(args.train_path)
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(
        json.dumps(artifact, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    LOGGER.info("Artifact written to %s", args.output_path)


if __name__ == "__main__":
    main()
