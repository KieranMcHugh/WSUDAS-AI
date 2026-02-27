import pandas as pd
import numpy as np
from datetime import datetime

def ftoc(temp):
    return (temp - 32) * 5.0 / 9.0

# inverse EWA function - need to verify this is correct
def inverse_ewa(series, window=10):
    reversed_series = series.iloc[::-1]
    ewa = reversed_series.ewm(span=window, adjust=False).mean()
    result = ewa.iloc[::-1]
    result.iloc[:window] = np.nan
    return result

#This function requires three inputs:
#First, a df similar with 'all_data' that have a 'date' column (properly formatted for date datatype) and two numeric columns ('Tmin' and 'Tmax') containing daily maximum and minimum temperatures
#Second, the latitude of the site of the temperature data
#Third, your cultivar of interest for the prediction of freezing tolerance. Please copy and paste a cultivar from 'cultivars_to_choose_from'
def weather_feature_generation(df, latitude=43.00, cultivar='Riesling'):

    df = df.sort_values('date') # order by date
    df = df.dropna(subset=['Tmin', 'Tmax']) # drop missing values

    df['Year'] = df['date'].dt.year # add year, month, and day
    df['Month'] = df['date'].dt.month
    df['Day'] = df['date'].dt.day

    df = df[df['Month'].isin([1,2,3,4,8,9,10,11,12])] # remove months may, june, & july

    # delete years with more than 10 missing Tmin values
    yearly_na = df.groupby('Year')['Tmin'].apply(lambda x: x.isna().sum())
    valid_years = yearly_na[yearly_na < 10].index
    df = df[df['Year'].isin(valid_years)]

    # remove "impossible" values
    df = df[
        (df['Tmax'] <= 100) &
        (df['Tmin'] >= -100) &
        (df['Tmin'] <= df['Tmax'])
    ]

    # remove if fewer than 20 rows remain after feature extraction
    # shouldn't occur but the original model had it so I've kept it in
    if len(df) < 20:
        return None

    # calculate daily temperature features
    df['Naive_average_temp'] = (df['Tmax'] + df['Tmin']) / 2
    df['within_day_range_temp'] = df['Tmax'] - df['Tmin']

    daily = df[['date','Year','Month','Day','Tmin','Tmax',
                'Naive_average_temp','within_day_range_temp']].copy()

    daily.rename(columns={
        'Tmin': 'min_temp',
        'Tmax': 'max_temp'
    }, inplace=True)

# EWMA & Inverse EWMA

    window_sizes = [2,4,6,8,10,12,14,16,18,20]

    # min temp
    for w in window_sizes:
        daily[f'min_EWMA_{w}day'] = daily['min_temp'].ewm(span=w, adjust=False).mean()
        daily[f'min_REWMA_{w}day'] = inverse_ewa(daily['min_temp'], w)

    # max temp
    for w in window_sizes:
        daily[f'max_EWMA_{w}day'] = daily['max_temp'].ewm(span=w, adjust=False).mean()
        daily[f'max_REWMA_{w}day'] = inverse_ewa(daily['max_temp'], w)

    # mean temp
    for w in window_sizes:
        daily[f'mean_EWMA_{w}day'] = daily['Naive_average_temp'].ewm(span=w, adjust=False).mean()
        daily[f'mean_REWMA_{w}day'] = inverse_ewa(daily['Naive_average_temp'], w)

    daily['season'] = np.where(
        daily['Month'].isin([9,10,11,12]),
        daily['Year'].astype(str) + '-' + (daily['Year']+1).astype(str),
        np.where(
            daily['Month'].isin([1,2,3,4]),
            (daily['Year']-1).astype(str) + '-' + daily['Year'].astype(str),
            np.nan
        )
    )

    daily = daily.dropna(subset=['season'])

    # cultivar list (should probably get these from somewhere else)
    Cultivars = [
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

    for c in Cultivars:
        daily[c] = 0

    selected_col = f'Cultivar.{cultivar}'
    if selected_col in daily.columns:
        daily[selected_col] = 1

    season_start = pd.to_datetime(
        np.where(
            daily['Month'].isin([9,10,11,12]),
            daily['Year'].astype(str) + '-09-01',
            (daily['Year']-1).astype(str) + '-09-01'
        )
    )

    daily['Days_in_season'] = (daily['date'] - season_start).dt.days

    #
    daily.rename(columns={'date': 'Date'}, inplace=True)

    window_sizes = [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]

    ordered_columns = []

    # add temp features to output csv columns
    ordered_columns += [
        'Date',
        'min_temp',
        'max_temp',
        'within_day_range_temp',
        'Naive_average_temp',
        'CU', 'NC', 'Utah',
        'GDH_10', 'GDH_7', 'GDH_4', 'GDH_0'
    ]

    # min ewma
    ordered_columns += [f'min_EWMA_{w}day' for w in window_sizes]

    # min inverse ewma
    ordered_columns += [f'min_REWMA_{w}day' for w in window_sizes]

    # max ewma
    ordered_columns += [f'max_EWMA_{w}day' for w in window_sizes]

    # max inverse ewma
    ordered_columns += [f'max_REWMA_{w}day' for w in window_sizes]

    # mean ewma
    ordered_columns += [f'mean_EWMA_{w}day' for w in window_sizes]

    # mean inverse ewma
    ordered_columns += [f'mean_REWMA_{w}day' for w in window_sizes]

    # add the cultivars to the columns (to mirror R script output)
    ordered_columns += Cultivars

    # append days in season
    ordered_columns += ['Days_in_season']

    # Keep only columns that actually exist (since chilling columns are excluded)
    ordered_columns = [col for col in ordered_columns if col in daily.columns]

    daily = daily[ordered_columns]

    return daily

# main function
# runs above functions on an input dataset from a csv file
if __name__ == "__main__":

    # read the csv file - can change to another file
    all_data = pd.read_csv('daily_temperature_data_example.csv')
    # all_data = read_csv('bc-weather-data.csv')
    # all_data = read_csv('WA-station-1.csv')

    # convert types (python's usually good about this but just to be sure)
    all_data['tmax'] = pd.to_numeric(all_data['tmax'], errors='coerce')
    all_data['tmin'] = pd.to_numeric(all_data['tmin'], errors='coerce')
    all_data['date'] = pd.to_datetime(all_data['date'], format='%m/%d/%Y')

    # rename columns
    all_data = all_data.rename(columns={
        'tmax': 'Tmax',
        'tmin': 'Tmin'
    })

    # change fahrenheit temps to celsius
    all_data['Tmax'] = ftoc(all_data['Tmax'])
    all_data['Tmin'] = ftoc(all_data['Tmin'])

    # generate the desired features
    features = weather_feature_generation(all_data, latitude=43.0606, cultivar='Riesling')

    # send to output csv file
    if features is not None:
        features.to_csv(
            'test_daily_temperature_data_example_feature_extracted.csv',
            index=False
        )
        print("Feature extraction complete.")
    else:
        print("Not enough valid data for feature extraction.")
