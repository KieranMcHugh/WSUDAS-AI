import numpy as np
import pandas as pd
import pyreadr
import math
def ftoc(temp_f):
    return temp_f - 32.0 * 5.0 / 9.0

def ewma  (series, windows):
    return series.ewm(span=windows, adjust=False).mean()

def stack_hourly_temp(df_daily, lat):
    df = df_daily.copy()
    hourly_data = []
    for _, row in df.iterrows():
        Tmin = row['Tmin']
        Tmax = row['Tmax']
        mean_temp = (Tmin + Tmax)/2
        amplitude = (Tmax - Tmin)/2
        date = pd.Timestamp(row['date'])
        for hour in range (24):
            temp = mean_temp + amplitude * math.cos((hour -15) * math.pi / 12)
            hourly_data.append({
                "date": date,
                "Year": date.year,
                "Month": date.month,
                "Day": date.day,
                "Hour": hour,
                "Temp": temp
            })
    return pd.DataFrame(hourly_data)

def chilling_temp(temp):
    temp = np.asarray(temp)
    return temp[temp >= 0] & temp[temp <= 7.2].astype(float)

