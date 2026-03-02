from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
import uvicorn
import pandas as pd
import io
from autogluon.tabular import TabularPredictor

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



@app.get("/health")
def health_check():

    if model_loaded:
        return {"status": "healthy", "model": "loaded"}
    else:
        return {"status": "unhealthy", "model": "not loaded"}


@app.post("/predict")
async def predict(file: UploadFile = File(...)):

    if not model_loaded:
        raise HTTPException(status_code=503, detail="Model not loaded")

    try:
        contents = await file.read()
        df = pd.read_json(io.StringIO(contents.decode('utf-8')))


        dates = pd.to_datetime(df["Date"])

        df_pred = df.drop(['Date'], axis=1)

        y_pred = predictor_LT50.predict(df_pred)

        y_pred.index = dates
        y_pred.index.name = "Date"

        output = io.StringIO()
        y_pred.to_csv(output, index=True, header=True)
        output.seek(0)

        return {
            "status": "success",
            "predictions": y_pred.to_dict(),
            "count": len(y_pred)
        }

    except pd.errors.ParserError:
        raise HTTPException(status_code=400, detail="Invalid CSV file format")
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