import pandas as pd
from boto3.s3.transfer import S3Transfer
import boto3

def group_by_country(dataframe):
    return dataframe.groupby(['Country/Region']).sum().reset_index()

def melt_dataframe_per_day(dataframe, column_name, days):
    melted = pd.melt(dataframe, id_vars=['Country/Region'], value_vars=days)
    melted.columns = ['Country', 'Day', column_name]
    return melted

def encode_day(each_day):
    month = each_day.split('/')[0]
    day = each_day.split('/')[1]
    day = '0' + day if len(day) == 1 else day
    return int(month + day)

def get_selection_id(each_day, transformed_dates):
    return transformed_dates.loc[encode_day(each_day), 'selection_id']

def update_data(event, context):
    raw_data_confirmed = pd.read_csv(
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
    raw_data_deaths = pd.read_csv(
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
    raw_data_recovered = pd.read_csv(
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv')

    grouped_confirmations = group_by_country(raw_data_confirmed)
    grouped_deaths = group_by_country(raw_data_deaths)
    grouped_recovered = group_by_country(raw_data_recovered)

    days = list(filter(lambda x: '/20' in x, grouped_deaths.columns))

    melted_confirmations = melt_dataframe_per_day(grouped_confirmations, 'Confirmations', days)
    melted_deaths = melt_dataframe_per_day(grouped_deaths, 'Deaths', days)
    melted_recovered = melt_dataframe_per_day(grouped_recovered, 'Recoveries', days)

    data = melted_confirmations.merge(melted_deaths, on=['Country', 'Day'])
    data = data.merge(melted_recovered, on=['Country', 'Day'])

    data.loc[:, 'Confirmations'] = data.Confirmations - data.Deaths - data.Recoveries
    data.loc[:, 'Total'] = data.Confirmations + data.Deaths + data.Recoveries
    data.loc[:, 'Confirmations_percentage'] = data.Confirmations / data.Total
    data.loc[:, 'Deaths_percentage'] = data.Deaths / data.Total
    data.loc[:, 'Recoveries_percentage'] = data.Recoveries / data.Total
    data = data.fillna(0)
    data.loc[data['Country'] == 'Korea, South', 'Country'] = 'South Korea'

    data = pd.melt(data, id_vars=['Country', 'Day', 'Confirmations', 'Deaths', 'Recoveries', 'Total'],
                   value_vars=['Confirmations_percentage', 'Deaths_percentage',
                               'Recoveries_percentage'])

    data.columns = ['Country', 'Day', 'Confirmations', 'Deaths', 'Recoveries', 'Total',
                    'Type', 'Percentage']

    data.loc[data['Type'] == 'Confirmations_percentage', 'Type'] = 'Confirmed'
    data.loc[data['Type'] == 'Deaths_percentage', 'Type'] = 'Deaths'
    data.loc[data['Type'] == 'Recoveries_percentage', 'Type'] = 'Recovered'

    population = pd.read_csv('https://raw.githubusercontent.com/victcebesp/covid-19-data-updater/master/population.csv') \
                   .drop(columns=['Alpha2', 'Alpha3', 'Longitude', 'Latitude'])
    data = data.merge(population, left_on='Country', right_on='Name')
    data.loc[:, 'RelativeConfirmations'] = data['Deaths'] / (data['Population2020'] * 1000)

    more_representative_countries = list(
        data.sort_values(['RelativeConfirmations'], ascending=False)['Country'].unique()[0:15])
    more_representative_countries.append('China')

    transformed_dates = [encode_day(each_day) for each_day in data.Day]
    transformed_dates = pd.DataFrame(pd.Series(transformed_dates).unique()).reset_index()
    transformed_dates.columns = ['selection_id', 'encoded_day']
    transformed_dates = transformed_dates.set_index('encoded_day')

    data.loc[:, 'EncodedDay'] = data['Day'].apply(lambda x: get_selection_id(x, transformed_dates))

    more_representative_countries_all_days = data[data['Country'].isin(more_representative_countries)]

    more_representative_countries_all_days.to_csv('s3://covid-visualization-data/representative.csv', index=False)

    return {
        "statusCode": 200
    }

if __name__ == "__main__":
    update_data('', '')
