import pandas as pd
import numpy as np
from datetime import datetime

# for rpy2 specifically
import rpy2.robjects as ro
from rpy2.robjects import default_converter
from rpy2.robjects.conversion import localconverter
from rpy2.robjects import pandas2ri

# install R libraries if not already installed, I set it to install everything (probably not desirable)
ro.r("""
    require(dplyr)
    require(readr)
    require(tidyverse)
    require(ggpubr)
    require(wesanderson)
    require(pracma)
    require(data.table)
    require(weathermetrics)
    require(measurements)
    require(naniar)
    require(ggplot2)
    require(ggpubr)
    require(dormancyR)
    require(chron)
    require(ggrepel)
    require(geosphere)
    require(chillR)
    require(lubridate)
    require(caret)
    require(fruclimadapt)

if (!require(chillR)) {
    install.packages("chillR", dependencies = TRUE, repos = "https://cloud.r-project.org", quiet = TRUE, Ncpus = 1)
}
""")

# helper function to convert fahrenheit to celsius
def fahrenheit_to_celsius(temp_f):
    return (temp_f - 32) * 5.0/9.0

# performs ewma
def ewma(series, window):
    alpha = 2 / (window + 1)
    return series.ewm(alpha=alpha, adjust=False).mean()

# performs inv ewa
def inverse_ewa(series, window=10):
    reversed_series = series[::-1]
    ewa = ewma(reversed_series, window)
    return ewa[::-1]

# needs: ['Year','Month','Day','Hour','Temp','date', 'DOY'] passed thru
# returns chilling output & gdh dataframes
def _run_r_models(hourly_df):
    # use localconverter to handle datetime
    with localconverter(default_converter + pandas2ri.converter):
        r_df = pandas2ri.py2rpy(hourly_df)

    ro.globalenv['data_all_hourly'] = r_df

    # R code below
    ro.r("""
        library(chillR)
        library(fruclimadapt) # Ensure fruclimadapt is loaded for GDH_linear

        CU_r <- chilling_units(data_all_hourly$Temp, summ = FALSE)
        Utah_r <- modified_utah_model(data_all_hourly$Temp, summ = FALSE)
        NC_r <- north_carolina_model(data_all_hourly$Temp, summ = FALSE)
        DP_r <- Dynamic_Model(data_all_hourly$Temp, summ = FALSE)

        # create data_all_hourly_1 for GDH_linear (kind of like current R code)
        data_all_hourly_1 <- data.frame(Year = data_all_hourly$Year,
                                        Month = data_all_hourly$Month,
                                        Day = data_all_hourly$Day,
                                        DOY = data_all_hourly$DOY,
                                        Hour = data_all_hourly$Hour,
                                        Temp = data_all_hourly$Temp)

        GDH_10_res <- GDH_linear(data_all_hourly_1, Tb = 10, Topt = 25, Tcrit = 36)
        GDH_7_res <- GDH_linear(data_all_hourly_1, Tb = 7, Topt = 25, Tcrit = 36)
        GDH_4_res <- GDH_linear(data_all_hourly_1, Tb = 4, Topt = 25, Tcrit = 36)
        GDH_0_res <- GDH_linear(data_all_hourly_1, Tb = 0, Topt = 25, Tcrit = 36)
    """)
    # end of R code

    # make numpy arrays from R chilling output
    CU = np.array(ro.r("CU_r"))
    Utah = np.array(ro.r("Utah_r"))
    NC = np.array(ro.r("NC_r"))
    DP = np.array(ro.r("DP_r"))

    # still r dataframes here, convert back to pandas
    with localconverter(default_converter + pandas2ri.converter):
        GDH_10_df = ro.conversion.rpy2py(ro.r("GDH_10_res"))
        GDH_7_df = ro.conversion.rpy2py(ro.r("GDH_7_res"))
        GDH_4_df = ro.conversion.rpy2py(ro.r("GDH_4_res"))
        GDH_0_df = ro.conversion.rpy2py(ro.r("GDH_0_res"))

    # return all the chilling outputs & GDHs
    return CU, Utah, NC, DP, GDH_10_df, GDH_7_df, GDH_4_df, GDH_0_df

def stack_hourly(df, latitude):

    # check if date column is datetime64[ns], otherwise can misbehave
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])

    with localconverter(default_converter + pandas2ri.converter):
        r_df = ro.conversion.py2rpy(df)

    ro.globalenv['df_input'] = r_df
    ro.globalenv['lat'] = latitude

    # R code
    ro.r("""
        library(chillR)
        result <- stack_hourly_temps(df_input, latitude = lat)[[1]]
    """)

    result = ro.r("result")

    # convert back to pandas
    with localconverter(default_converter + pandas2ri.converter):
        result = ro.conversion.rpy2py(result)

    return result

def Weather_feature_generation(df, latitude=43.0, cultivar='Riesling', cultivars_list=None):

    df = df.copy()

    # FORCE SAFE DTYPES HERE
    df['Tmin'] = df['Tmin'].astype(float)
    df['Tmax'] = df['Tmax'].astype(float)
    df['date'] = pd.to_datetime(df['date'])

    # sort by date & drop entries missing temp val
    df = df.sort_values('date')
    df = df.dropna(subset=['Tmin','Tmax'])

    # doing the same check for impossible vals as base feature_extraction
    df = df[(df['Tmax'] <= 100) &
            (df['Tmin'] >= -100) &
            (df['Tmin'] <= df['Tmax'])]

    if len(df) < 20:
        return None

    # chillR needs these columns or it fails
    df['Year'] = df['date'].dt.year
    df['Month'] = df['date'].dt.month
    df['Day'] = df['date'].dt.day

    # removes may, june & july
    df = df[df['Month'].isin([1, 2, 3, 4, 8, 9, 10, 11, 12])]
    if len(df) < 20: # probably not necessary but exists in original code
        return None

    hourly = stack_hourly(
        df[['date', 'Year', 'Month', 'Day', 'Tmin', 'Tmax']],
        latitude
    )

    hourly['date'] = pd.to_datetime(hourly['date'])
    hourly['date'] = hourly['date'].dt.tz_localize(None) # normalize to timezone-naive for consistency
    hourly['Month'] = hourly['date'].dt.month # add Month to hourly for filtering
    hourly['Year'] = hourly['date'].dt.year # add Year to hourly for season definition
    hourly['DOY'] = hourly['date'].dt.dayofyear # add DOY for GDH_linear R function (later)

    # chilling and GDH models with rpy2 - raw hourly values
    CU_raw, Utah_raw, NC_raw, DP_raw, GDH_10_df, GDH_7_df, GDH_4_df, GDH_0_df = _run_r_models(hourly)

    # set raw chilling values to hourly
    hourly['CU_raw'] = CU_raw
    hourly['Utah_raw'] = Utah_raw
    hourly['NC_raw'] = NC_raw
    hourly['DP_raw'] = DP_raw

    # process daily GDH dataframes
    # each GDH contains ['Date', 'Year', 'Month', 'Day', 'DOY', 'GDH']
    GDH_dfs = {'GDH_10': GDH_10_df, 'GDH_7': GDH_7_df, 'GDH_4': GDH_4_df, 'GDH_0': GDH_0_df}
    for key, gdh_df in GDH_dfs.items():
        gdh_df['Date'] = pd.to_datetime(gdh_df['Date']) # again, check that Date is datetime
        gdh_df['Date'] = gdh_df['Date'].dt.tz_localize(None) # normalize to timezone-naive
        gdh_df['Month'] = gdh_df['Date'].dt.month # add month for filtering
        gdh_df.rename(columns={'GDH': f'{key}_raw_daily'}, inplace=True) # rename GDH column (for output csv)


    # Define dormant season function for consistency with R
    def get_dormant_season(row_date):
        month = row_date.month
        year = row_date.year
        if month in [9, 10, 11, 12]:
            return f"{year}-{year + 1}"
        elif month in [1, 2, 3, 4]:
            return f"{year - 1}-{year}"
        return np.nan # makes may-august NaN, dropping those vals

    hourly['dormant_season'] = hourly['date'].apply(get_dormant_season)
    for key, gdh_df in GDH_dfs.items():
        gdh_df['dormant_season'] = gdh_df['Date'].apply(get_dormant_season)

    # Apply month-based filtering and zeroing for chilling models (hourly)
    chilling_months_active = hourly['Month'].isin([9, 10, 11, 12, 1, 2, 3, 4])
    hourly['CU_seasonal'] = np.maximum(hourly['CU_raw'].where(chilling_months_active, 0), 0)
    hourly['Utah_seasonal'] = np.maximum(hourly['Utah_raw'].where(chilling_months_active, 0), 0)
    hourly['NC_seasonal'] = np.maximum(hourly['NC_raw'].where(chilling_months_active, 0), 0)
    hourly['DP_seasonal'] = hourly['DP_raw'].where(chilling_months_active, 0) # DP can go below 0 in R, so add a bound

    # apply month-based filtering and zeroing for GDH models (daily)
    for key, gdh_df in GDH_dfs.items():
        gdh_months_active = gdh_df['Month'].isin([1, 2, 3, 4])
        gdh_df[f'{key}_seasonal'] = np.maximum(gdh_df[f'{key}_raw_daily'].where(gdh_months_active, 0), 0)

    # cumulative sums for chilling - hourly then daily max
    hourly_filtered = hourly.dropna(subset=['dormant_season']).sort_values(by=['dormant_season', 'date', 'Hour'])

    hourly_filtered['CU_cumsum'] = hourly_filtered.groupby('dormant_season')['CU_seasonal'].cumsum()
    hourly_filtered['Utah_cumsum'] = hourly_filtered.groupby('dormant_season')['Utah_seasonal'].cumsum()
    hourly_filtered['NC_cumsum'] = hourly_filtered.groupby('dormant_season')['NC_seasonal'].cumsum()
    hourly_filtered['DP_cumsum'] = hourly_filtered.groupby('dormant_season')['DP_seasonal'].cumsum()

    # get the cumulative sums for GDH (daily)
    for key, gdh_df in GDH_dfs.items():
        # filter out dormant seasons
        gdh_df_filtered = gdh_df.dropna(subset=['dormant_season']).sort_values(by=['dormant_season', 'Date'])
        gdh_df[f'{key}_cumsum'] = gdh_df_filtered.groupby('dormant_season')[f'{key}_seasonal'].cumsum()
        # Fill NA generated on filtered groups with 0 for consistency if no values there for the season
        gdh_df[f'{key}_cumsum'] = gdh_df[f'{key}_cumsum'].fillna(0)

    # daily aggregation for temperature metrics
    daily_temp_metrics = hourly.groupby('date').agg(
        min_temp=('Temp', 'min'),
        max_temp=('Temp', 'max'),
        mean_temp=('Temp', 'mean')
    ).reset_index()

    # daily aggregation for chilling cumulative features (max per day from hourly cumulative sums)
    daily_chilling_cumulative_features = hourly_filtered.groupby('date').agg(
        CU=('CU_cumsum', 'max'),
        Utah=('Utah_cumsum', 'max'),
        NC=('NC_cumsum', 'max'),
        DP=('DP_cumsum', 'max')
    ).reset_index()

    # merge the daily temperature metrics and chilling cumulative features
    df_out = daily_temp_metrics.merge(daily_chilling_cumulative_features, on='date', how='left')

    # merge the cumulative GDH features from their respective dataframes
    for key, gdh_df in GDH_dfs.items():
        df_out = df_out.merge(
            gdh_df[['Date', f'{key}_cumsum']].rename(columns={'Date': 'date', f'{key}_cumsum': key}),
            on='date',
            how='left'
        )
        df_out[key] = df_out[key].fillna(0) # fills NaN for dates not in GDH season with 0


    df_out['within_day_range_temp'] = df_out['max_temp'] - df_out['min_temp']

    # rename mean_temp to Naive_average_temp and date to Date (so it plays nice with the rest of the pipeline)
    df_out.rename(columns={'mean_temp':'Naive_average_temp', 'date':'Date'}, inplace=True)

    # EWMA windows
    windows = [2,4,6,8,10,12,14,16,18,20]

    for w in windows:
        df_out[f'min_EWMA_{w}day'] = ewma(df_out['min_temp'], w)
        df_out[f'max_EWMA_{w}day'] = ewma(df_out['max_temp'], w)
        df_out[f'mean_EWMA_{w}day'] = ewma(df_out['Naive_average_temp'], w)

        df_out[f'min_REWMA_{w}day'] = inverse_ewa(df_out['min_temp'], w)
        df_out[f'max_REWMA_{w}day'] = inverse_ewa(df_out['max_temp'], w)
        df_out[f'mean_REWMA_{w}day'] = inverse_ewa(df_out['Naive_average_temp'], w)

    # cultivar one-hot encoding
    if cultivars_list:
        for c_full_name in cultivars_list: # c_full_name is 'Cultivar.Aromella'
            df_out[c_full_name] = 1 if c_full_name.replace('Cultivar.', '') == cultivar else 0

    # add Days_in_season
    def calculate_days_in_season(row_date):
        month = row_date.month
        year = row_date.year
        if month in [9, 10, 11, 12]:
            start_date = datetime(year, 9, 1)
        elif month in [1, 2, 3, 4]:
            start_date = datetime(year - 1, 9, 1)
        else: # for months like may, june, july & august (i.e. dropped months)
            return np.nan

        return (row_date - start_date).days

    df_out['Days_in_season'] = df_out['Date'].apply(calculate_days_in_season)
    df_out = df_out.dropna(subset=['Days_in_season']) # This filters out August and other non-season months

    # reorders the columns so the csv is consistent with original feature_extraction
    desired_output_headers_str = "Date,min_temp,max_temp,within_day_range_temp,Naive_average_temp,CU,NC,Utah,GDH_10,GDH_7,GDH_4,GDH_0,min_EWMA_2day,min_EWMA_4day,min_EWMA_6day,min_EWMA_8day,min_EWMA_10day,min_EWMA_12day,min_EWMA_14day,min_EWMA_16day,min_EWMA_18day,min_EWMA_20day,min_REWMA_2day,min_REWMA_4day,min_REWMA_6day,min_REWMA_8day,min_REWMA_10day,min_REWMA_12day,min_REWMA_14day,min_REWMA_16day,min_REWMA_18day,min_REWMA_20day,max_EWMA_2day,max_EWMA_4day,max_EWMA_6day,max_EWMA_8day,max_EWMA_10day,max_EWMA_12day,max_EWMA_14day,max_EWMA_16day,max_EWMA_18day,max_EWMA_20day,max_REWMA_2day,max_REWMA_4day,max_REWMA_6day,max_REWMA_8day,max_REWMA_10day,max_REWMA_12day,max_REWMA_14day,max_REWMA_16day,max_REWMA_18day,max_REWMA_20day,mean_EWMA_2day,mean_EWMA_4day,mean_EWMA_6day,mean_EWMA_8day,mean_EWMA_10day,mean_EWMA_12day,mean_EWMA_14day,mean_EWMA_16day,mean_EWMA_18day,mean_EWMA_20day,mean_REWMA_2day,mean_REWMA_4day,mean_REWMA_6day,mean_REWMA_8day,mean_REWMA_10day,mean_REWMA_12day,mean_REWMA_14day,mean_REWMA_16day,mean_REWMA_18day,mean_REWMA_20day,Cultivar.Aromella,Cultivar.Cabernet_Franc,Cultivar.Cabernet_Sauvignon,Cultivar.Cayuga_White,Cultivar.Chambourcin,Cultivar.Chancellor,Cultivar.Chardonnay,Cultivar.Chenin_blanc,Cultivar.Concord,Cultivar.Corot_noir,Cultivar.Gewurztraminer,Cultivar.Gruner_Veltliner,Cultivar.La_Crescent,Cultivar.Lemberger,Cultivar.Malbec,Cultivar.Marechal_Foch,Cultivar.Marquette,Cultivar.Merlot,Cultivar.Niagara,Cultivar.Noiret,Cultivar.Pinot_blanc,Cultivar.Pinot_gris,Cultivar.Pinot_noir,Cultivar.Riesling,Cultivar.Sangiovese,Cultivar.Saperavi,Cultivar.Sauvignon_blanc,Cultivar.St_Croix,Cultivar.Syrah,Cultivar.Tempranillo,Cultivar.Tocai_Fruliano,Cultivar.Traminette,Cultivar.Valvin_Muscat,Cultivar.Vidal,Cultivar.Vignoles,Cultivar.Viognier,Cultivar.Zinfandel,Cultivar.Brianna,Cultivar.Frontenac,Cultivar.Petite_Pearl,Cultivar.Frontenac_blanc,Cultivar.Frontenac_gris,Cultivar.Seyval,Cultivar.St_Pepin,Cultivar.L.Acadie,Cultivar.Aravelle,Cultivar.Aurora,Cultivar.Caminante_blanc,Cultivar.Delaware,Cultivar.Elvira,Cultivar.Fleurtai,Cultivar.Ives,Cultivar.Soreli,Cultivar.Vincent,Cultivar.NY_Muscat,Cultivar.Refosco,Cultivar.Teroldego,Days_in_season"
    desired_columns = [col.strip() for col in desired_output_headers_str.split(',')]

    # filter df_out to only contain desired_columns (from above) and reorder them
    final_columns = [col for col in desired_columns if col in df_out.columns]

    return df_out[final_columns]

def main():
    # load & read the temperature file w/ pandas
    all_data = pd.read_csv("daily_temperature_data_example.csv")

    all_data['Tmax'] = fahrenheit_to_celsius(all_data['tmax']).astype(float)
    all_data['Tmin'] = fahrenheit_to_celsius(all_data['tmin']).astype(float)
    all_data['date'] = pd.to_datetime(all_data['date'])

    # cultivar list - probably needs to change
    cultivars_to_choose_from = [
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

    features = Weather_feature_generation(
        all_data[['date','Tmin','Tmax']],
        latitude=43.0606,
        cultivar='Riesling',
        cultivars_list=cultivars_to_choose_from
    )

    features.to_csv("test_daily_temperature_data_example_feature_extracted.csv", index=False)

main() # runs Weather_feature_gen
