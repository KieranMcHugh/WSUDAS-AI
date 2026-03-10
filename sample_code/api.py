from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import FileResponse
import uvicorn
import pandas as pd
import io
from autogluon.tabular import TabularPredictor
import threading


import os

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


def _read_uploaded_json(contents: bytes) -> pd.DataFrame:
    try:
        return pd.read_json(io.StringIO(contents.decode("utf-8")))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON input: {str(e)}")


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

    # Rename columns to match FeatureExtracted.py expectations (Tmin, Tmax with capital T)
    daily_df.rename(columns={"tmin": "Tmin", "tmax": "Tmax"}, inplace=True)

    cultivar_clean = _normalize_cultivar_name(cultivar)

    with _feature_lock:
        features = Weather_feature_generation(
            daily_df[["date", "Tmin", "Tmax"]],
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

    return features


@app.get("/health")
def health_check():

    if model_loaded:
        return {"status": "healthy", "model": "loaded"}
    else:
        return {"status": "unhealthy", "model": "not loaded"}


@app.post("/predict")
async def predict(
    file: UploadFile = File(...),
    latitude: float = Query(43.0),
    cultivar: str = Query("Riesling"),
):

    if not model_loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")

    try:
        contents = await file.read()
        daily_df = _read_uploaded_json(contents)

        features_df = _generate_features_from_daily(
            daily_df=daily_df,
            latitude=latitude,
            cultivar=cultivar,
        )

        dates = pd.to_datetime(features_df["Date"])
        df_pred = features_df.drop(["Date"], axis=1)

        y_pred = predictor_LT50.predict(df_pred)
        y_pred.index = dates
        y_pred.index.name = "Date"

        return {
            "status": "success",
            "count": len(y_pred),
            "predictions": [
                {"Date": str(idx), "LT50": float(value)}
                for idx, value in y_pred.items()
            ]
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")


@app.get("/")
def root():
    return {
        "message": "LT50 Prediction API",
        "docs": "http://127.0.0.1:8000/docs",
        "health": "http://127.0.0.1:8000/health"
    }

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
