import logging
import os
from pathlib import Path

import pandas as pd
from catboost import CatBoostClassifier


logger = logging.getLogger(__name__)

DEFAULT_MODEL_PATH = Path(__file__).resolve().parents[1] / "models" / "my_catboost.cbm"
MODEL_PATH = Path(os.getenv("MODEL_PATH", str(DEFAULT_MODEL_PATH)))
FRAUD_THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.98"))
CPU_THREADS = int(os.getenv("CPU_THREADS", "1"))

if not 0 <= FRAUD_THRESHOLD <= 1:
    raise ValueError("FRAUD_THRESHOLD must be between 0 and 1")
if CPU_THREADS < 1:
    raise ValueError("CPU_THREADS must be at least 1")

EXPECTED_CATEGORICAL = [
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
]

logger.info("Loading pretrained model from %s", MODEL_PATH)
model = CatBoostClassifier()
model.load_model(MODEL_PATH)
logger.info(
    "Pretrained CPU model loaded successfully. Threshold: %.4f", FRAUD_THRESHOLD
)


def make_pred(dataframe, source_info="kafka"):
    missing_features = [
        feature for feature in model.feature_names_ if feature not in dataframe.columns
    ]
    if missing_features:
        raise ValueError(
            "Missing model features: " + ", ".join(missing_features)
        )

    model_input = dataframe[model.feature_names_].copy()
    for column in EXPECTED_CATEGORICAL:
        model_input[column] = model_input[column].astype(str)

    scores = model.predict_proba(model_input, thread_count=CPU_THREADS)[:, 1]
    predictions = pd.DataFrame(
        {
            "score": scores,
            "fraud_flag": (scores > FRAUD_THRESHOLD).astype(int),
        }
    )
    logger.debug("Prediction completed for %s", source_info)
    return predictions
