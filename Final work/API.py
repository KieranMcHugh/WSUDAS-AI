from fastapi import FastAPI, HTTPException
import uvicorn
import pandas as pd

# Patch to resolve missing iteritems in pandas 2.0.0 for older libraries that strict rely on it
if not hasattr(pd.DataFrame, 'iteritems'):
    pd.DataFrame.iteritems = pd.DataFrame.items
if not hasattr(pd.Series, 'iteritems'):
    pd.Series.iteritems = pd.Series.items

import io
from autogluon.tabular import TabularPredictor
import threading
from pydantic import BaseModel
from typing import List
import math
import numpy as np

app = FastAPI(
    title="LT50 Prediction API",
    description="API for predicting Low Temperature Tolerance (LT50) values",
    version="1.0.0"
)

try:
    predictor_LT50 = TabularPredictor.load("NYUS2_2", require_version_match=False)
    model_loaded = True
except Exception as e:
    print(f"Error loading model: {e}")
    model_loaded = False


# If feature generation uses rpy2 / R globals, serialize calls.
_feature_lock = threading.Lock()

CULTIVARS = [
    "Cultivar.Aromella", "Cultivar.Cabernet_Franc", "Cultivar.Cabernet_Sauvignon",
    "Cultivar.Cayuga_White", "Cultivar.Chambourcin", "Cultivar.Chancellor",
    "Cultivar.Chardonnay", "Cultivar.Chenin_blanc", "Cultivar.Concord",
    "Cultivar.Corot_noir", "Cultivar.Gewurztraminer", "Cultivar.Gruner_Veltliner",
    "Cultivar.La_Crescent", "Cultivar.Lemberger", "Cultivar.Malbec",
    "Cultivar.Marechal_Foch", "Cultivar.Marquette", "Cultivar.Merlot",
    "Cultivar.Niagara", "Cultivar.Noiret", "Cultivar.Pinot_blanc",
    "Cultivar.Pinot_gris", "Cultivar.Pinot_noir", "Cultivar.Riesling",
    "Cultivar.Sangiovese", "Cultivar.Saperavi", "Cultivar.Sauvignon_blanc",
    "Cultivar.St_Croix", "Cultivar.Syrah", "Cultivar.Tempranillo",
    "Cultivar.Tocai_Fruliano", "Cultivar.Traminette", "Cultivar.Valvin_Muscat",
    "Cultivar.Vidal", "Cultivar.Vignoles", "Cultivar.Viognier",
    "Cultivar.Zinfandel", "Cultivar.Brianna", "Cultivar.Frontenac",
    "Cultivar.Petite_Pearl", "Cultivar.Frontenac_blanc", "Cultivar.Frontenac_gris",
    "Cultivar.Seyval", "Cultivar.St_Pepin", "Cultivar.L.Acadie",
    "Cultivar.Aravelle", "Cultivar.Aurora", "Cultivar.Caminante_blanc",
    "Cultivar.Delaware", "Cultivar.Elvira", "Cultivar.Fleurtai",
    "Cultivar.Ives", "Cultivar.Soreli", "Cultivar.Vincent",
    "Cultivar.NY_Muscat", "Cultivar.Refosco", "Cultivar.Teroldego"
]


def _normalize_cultivar_name(cultivar: str) -> str:
    if cultivar.startswith("Cultivar."):
        return cultivar.replace("Cultivar.", "")
    return cultivar


def _model_to_dict(row):
    if hasattr(row, "model_dump"):
        return row.model_dump()
    return row.dict()


def _serialize_daily_weather(rows):
    return [_model_to_dict(row) for row in rows]


def _read_uploaded_json(contents: bytes) -> pd.DataFrame:
    try:
        return pd.read_json(io.StringIO(contents.decode("utf-8")))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON input: {str(e)}")

def _json_safe_lt50(value):
    v = float(value)
    if math.isnan(v):
        return None  # or some default, but None will be null in JSON
    return round(v, 5)

def _generate_features_from_daily(
    daily_df: pd.DataFrame,
    latitude: float,
    cultivar: str,
) -> pd.DataFrame:
    try:
        from FeatureExtracted import Weather_feature_generation
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Feature generation module not available: {str(e)}"
        )

    required_columns = {"date", "tmin", "tmax"}
    missing = required_columns - set(daily_df.columns)
    if missing:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required columns: {sorted(missing)}"
        )

    daily_df = daily_df.copy()
    daily_df["date"] = pd.to_datetime(daily_df["date"], errors="coerce")
    daily_df["tmin"] = pd.to_numeric(daily_df["tmin"], errors="coerce")
    daily_df["tmax"] = pd.to_numeric(daily_df["tmax"], errors="coerce")

    if daily_df["date"].isna().any():
        raise HTTPException(status_code=400, detail="Some 'date' values are invalid")
    if daily_df["tmin"].isna().any() or daily_df["tmax"].isna().any():
        raise HTTPException(status_code=400, detail="Some 'tmin' or 'tmax' values are invalid")

    daily_df["tmin"] = ((daily_df["tmin"] - 32) * 5 / 9).astype(float)
    daily_df["tmax"] = ((daily_df["tmax"] - 32) * 5 / 9).astype(float)
    # Rename columns to match FeatureExtracted.py expectations (Tmin, Tmax with capital T)
    daily_df.rename(columns={"tmin": "Tmin", "tmax": "Tmax"}, inplace=True)

    cultivar_clean = _normalize_cultivar_name(cultivar)

    with _feature_lock:
        feature_input = daily_df[["date", "Tmin", "Tmax"]].copy()
        features = Weather_feature_generation(
            feature_input,
            latitude=latitude,
            cultivar=cultivar_clean,
            cultivars_list=CULTIVARS,
        )

    if features is None or features.empty:
        raise HTTPException(
            status_code=400,
            detail="Feature generation returned no rows. Check date range and temperatures."
        )

    if "Date" not in features.columns:
        raise HTTPException(status_code=500, detail="Generated features are missing 'Date'")

    # Fill any NaN values with 0 to prevent model prediction issues
    features.fillna(0, inplace=True)

    return features


@app.get("/health")
def health_check():

    if model_loaded:
        return {"status": "healthy", "model": "loaded"}
    else:
        return {"status": "unhealthy", "model": "not loaded"}

class DailyWeather(BaseModel):
    date: str
    tmin: float
    tmax: float

class PredictionRequest(BaseModel):
    latitude: float = 43.0
    cultivar: str = "Riesling"
    data: List[DailyWeather]

@app.post("/predict")
async def predict(body: PredictionRequest):

    if not model_loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")

    try:
        daily_df = pd.DataFrame(_serialize_daily_weather(body.data))
        daily_df = daily_df.astype({"tmin": "float64", "tmax": "float64"})

        features_df = _generate_features_from_daily(
            daily_df=daily_df,
            latitude=body.latitude,
            cultivar=body.cultivar,
        )

        dates = pd.DatetimeIndex(pd.to_datetime(features_df["Date"]))
        df_pred = features_df.drop(columns=["Date"])
        # after df_pred is created
        print("Any NaN in features:", df_pred.isna().any().any())
        numeric_features = df_pred.select_dtypes(include="number")
        print("Any Inf in features:", np.isinf(numeric_features.to_numpy()).any())
        print("Feature shape:", df_pred.shape)

        raw_pred = pd.Series(predictor_LT50.predict(df_pred))
        print("Pred NaN count:", raw_pred.isna().sum())
        print("Pred Inf count:", np.isinf(raw_pred.astype(float)).sum())
        y_pred = raw_pred.reindex(range(len(dates)))
        y_pred.index = dates
        y_pred.index.name = "Date"

        predictions = []
        for idx, value in y_pred.items():
            ts = pd.Timestamp(idx)
            predictions.append({"date": ts.strftime("%Y-%m-%d"), "lt50": _json_safe_lt50(value)})

        return {"status": "success", "predictions": predictions}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")


@app.get("/")
def root():
    return {
        "message": "LT50 Prediction API",
        "docs": "http://0.0.0.0:8000/docs",
        "health": "http://0.0.0.0:8000/health"
    }

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
