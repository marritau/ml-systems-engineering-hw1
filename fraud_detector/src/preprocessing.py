import json
import logging
import os
from pathlib import Path

import numpy as np
import pandas as pd
from geopy.distance import great_circle


logger = logging.getLogger(__name__)

DEFAULT_ARTIFACT_PATH = (
    Path(__file__).resolve().parents[1] / "artifacts" / "preprocessing.json"
)
COORDINATE_COLUMNS = ["lat", "lon", "merchant_lat", "merchant_lon"]


def load_preprocessing_artifact(path=None):
    artifact_path = Path(
        path or os.getenv("PREPROCESSING_ARTIFACT_PATH", str(DEFAULT_ARTIFACT_PATH))
    )
    logger.info("Loading preprocessing artifact from %s", artifact_path)
    with artifact_path.open(encoding="utf-8") as artifact_file:
        artifact = json.load(artifact_file)

    if artifact.get("version") != 1:
        raise ValueError(
            f"Unsupported preprocessing artifact version: {artifact.get('version')}"
        )

    logger.info("Preprocessing artifact loaded successfully")
    return artifact


def validate_input(input_df, artifact):
    required_columns = artifact["required_input_columns"]
    missing_columns = [
        column for column in required_columns if column not in input_df.columns
    ]
    if missing_columns:
        raise ValueError(
            "Missing required transaction fields: " + ", ".join(missing_columns)
        )


def add_time_features(dataframe):
    transaction_time = pd.to_datetime(dataframe["transaction_time"], errors="raise")
    dataframe["hour"] = transaction_time.dt.hour
    dataframe["year"] = transaction_time.dt.year
    dataframe["month"] = transaction_time.dt.month
    dataframe["day_of_month"] = transaction_time.dt.day
    dataframe["day_of_week"] = transaction_time.dt.dayofweek
    return dataframe.drop(columns="transaction_time")


def add_distance_features(dataframe):
    dataframe["distance"] = dataframe.apply(
        lambda row: great_circle(
            (row["lat"], row["lon"]),
            (row["merchant_lat"], row["merchant_lon"]),
        ).km,
        axis=1,
    )
    return dataframe.drop(columns=COORDINATE_COLUMNS)


def encode_categories(dataframe, artifact):
    for column in artifact["categorical_columns"]:
        encoded_column = f"{column}_cat"
        mapping = artifact["category_mappings"][column]
        dataframe[encoded_column] = (
            dataframe[column].astype("string").map(mapping).fillna("cat_NAN")
        )
        dataframe.drop(columns=column, inplace=True)
    return dataframe


def add_mean_encodings(dataframe, artifact):
    default_mean = artifact["default_mean_encoding"]
    encoded_columns = [
        f"{column}_cat" for column in artifact["categorical_columns"]
    ]
    for column in encoded_columns + artifact["time_columns"]:
        mapping = artifact["mean_encodings"][column]
        dataframe[f"{column}_mean_enc"] = (
            dataframe[column].astype(str).map(mapping).fillna(default_mean)
        )
    return dataframe


def transform_continuous_features(dataframe, artifact):
    for column in artifact["continuous_columns"]:
        dataframe[column] = dataframe[column].fillna(
            artifact["continuous_means"][column]
        )
        if (dataframe[column] < -1).any():
            raise ValueError(f"Column {column} contains values below -1")
        dataframe[f"{column}_log"] = np.log1p(dataframe[column])
        dataframe.drop(columns=column, inplace=True)
    return dataframe


def run_preproc(artifact, input_df):
    validate_input(input_df, artifact)
    dataframe = input_df[artifact["required_input_columns"]].copy()
    dataframe.drop(columns=artifact["drop_columns"], inplace=True)

    dataframe = encode_categories(dataframe, artifact)
    dataframe = add_time_features(dataframe)
    dataframe = add_mean_encodings(dataframe, artifact)
    dataframe = add_distance_features(dataframe)
    dataframe = transform_continuous_features(dataframe, artifact)

    feature_order = artifact["feature_order"]
    missing_features = [
        feature for feature in feature_order if feature not in dataframe.columns
    ]
    if missing_features:
        raise ValueError(
            "Preprocessing did not produce model features: "
            + ", ".join(missing_features)
        )

    logger.debug("Transaction preprocessing completed. Shape: %s", dataframe.shape)
    return dataframe[feature_order]
