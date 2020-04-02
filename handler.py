import datetime
import time

import pandas as pd

def update_data(event, context):
    raw_data_confirmed = pd.read_csv(
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv')
    raw_data_deaths = pd.read_csv(
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv')
    raw_data_recovered = pd.read_csv(
        'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv')

    def group_by_country(dataframe):
        return dataframe.groupby(['Country/Region']).sum().reset_index()

    grouped_confirmations = group_by_country(raw_data_confirmed)
    grouped_deaths = group_by_country(raw_data_deaths)
    grouped_recovered = group_by_country(raw_data_recovered)

    days = list(filter(lambda x: '/20' in x, grouped_deaths.columns))

    def melt_dataframe_per_day(dataframe, column_name):
        melted = pd.melt(dataframe, id_vars=['Country/Region'], value_vars=days)
        melted.columns = ['Country', 'Day', column_name]
        return melted

    melted_confirmations = melt_dataframe_per_day(grouped_confirmations, 'Confirmations')
    melted_deaths = melt_dataframe_per_day(grouped_deaths, 'Deaths')
    melted_recovered = melt_dataframe_per_day(grouped_recovered, 'Recoveries')

    data = melted_confirmations.merge(melted_deaths, on=['Country', 'Day'])
    data = data.merge(melted_recovered, on=['Country', 'Day'])

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

    population = pd.read_csv('population.csv').drop(columns=['Alpha2', 'Alpha3', 'Longitude', 'Latitude'])
    data = data.merge(population, left_on='Country', right_on='Name')
    data.loc[:, 'RelativeConfirmations'] = data['Deaths'] / (data['Population2020'] * 1000)

    def get_unix_timestamp(string):
        return time.mktime(datetime.datetime.strptime(string, "%m/%d/%y").timetuple())

    data.loc[:, 'UnixTimeStamp'] = data['Day'].apply(get_unix_timestamp)

    more_representative_countries = list(
        data.sort_values(['RelativeConfirmations'], ascending=False)['Country'].unique()[0:15])
    more_representative_countries.append('China')

    more_representative_countries_all_days = data[data['Country'].isin(more_representative_countries)]

    more_representative_countries_all_days.to_csv('s3://covid-visualization-data/representative.csv', index=False)


if __name__ == "__main__":
    main('', '')
