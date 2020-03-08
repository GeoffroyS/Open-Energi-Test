#!/usr/bin/env python

import csv
import datetime as dt
import json
import math
import matplotlib.pyplot as plt
import pandas as pd
import sqlite3
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

def _get_resampled_power_mes_df():
	"""Return a Pandas dataframe containing the power measurements from the csv file"""
	print(_get_resampled_power_mes_df.__doc__)

	power_mes = pd.read_csv('power_measurements_2019-11-25.csv', usecols=["Time", "Value"])
	power_mes.Time = pd.to_datetime(power_mes.Time)
	power_mes_resampled = power_mes.resample('30min', on='Time').Value.max().to_frame()
	power_mes_resampled.rename(columns = {'Value': 'MaxValue'}, inplace = True)
	power_mes_resampled.index.names = ['FromTime']
	power_mes_resampled.reset_index(inplace=True)
	power_mes_resampled.index.names = ["HalfHourId"]
	power_mes_resampled.index += 1

	return power_mes_resampled

def _get_carbon_intensity():
	"""Retrieve json containing carbon intensity from API"""
	print(_get_carbon_intensity.__doc__ )

	json_obj = '{}'
	url = "https://api.carbonintensity.org.uk/intensity/2019-11-25/2019-11-26"
	req = Request(url)

	try:
		response = urlopen(req)
	except HTTPError as e:
		print('Error code: ', e.code)
	except URLError as e:
		print('Error: ', e.reason)
	else:
		data = response.read()
		encoding = response.info().get_content_charset('utf-8')
		json_obj = json.loads(data.decode(encoding))

	return json_obj

def _clean_json(carbonIintensity):
	"""Create new json so we can use it to compute the value we need"""
	print(_clean_json.__doc__)
	new_dict = dict()
	for item in carbonIintensity['data']:
		time = dt.datetime.strptime(item['from'], '%Y-%m-%dT%H:%MZ')
		new_dict[time] = item['intensity']['actual']
	return new_dict

def _assign_previous_value_to_nans(powerMeasurements):
	"""Assign previous value to NaNs and 0s"""
	print(_assign_previous_value_to_nans.__doc__)
	tmp = 0
	for i, row in enumerate(powerMeasurements.itertuples()):
		if (math.isnan(row[2])) or (row[2] == 0):
			powerMeasurements.loc[i+1,'MaxValue'] = tmp
		else:
			tmp = row[2]

	return powerMeasurements


def _compute_carbon_footprint(df, carbonIntensity):
	"""Compute carbon footprint"""
	print(_compute_carbon_footprint.__doc__)
	calculations = list()
	for row in df.itertuples():
		#row[0] is the index
		#row[1] is the date
		#row[2] is the consumption
		calculations.append(row[2]*carbonIntensity[row[1]])

	df['calculation'] = calculations

	return df

def _create_connection():
	"""Create a database connection to the SQLite database stored in memory
	returns connection object or None
	"""
	print(_create_connection.__doc__)
	conn = None
	try:
		conn = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
		return conn
	except Error as e:
		print(e)
 
	return conn

def _plot_carbon_footprint(df):
	"""Plot carbon footprint"""
	print(_plot_carbon_footprint.__doc__)
	#df['calculation'].plot()
	plt.plot(df['HalfHourId'], df['calculation'])
	plt.title('Carbon Footprint = f(HalfHourId)')
	plt.show()

	return

def main():
	power_mes_resampled = _get_resampled_power_mes_df()
	carbon_intensity = _get_carbon_intensity()
	clean_carbon_intensity = _clean_json(carbon_intensity)
	power_mes_resampled = _assign_previous_value_to_nans(power_mes_resampled)
	power_mes_resampled = _compute_carbon_footprint(power_mes_resampled, clean_carbon_intensity)
	print(power_mes_resampled.head())

	connection = _create_connection()
	power_mes_resampled.to_sql(name='computations', con=connection)
	sql_results = pd.read_sql('select * from computations where HalfHourId>=14 and HalfHourId<=20', connection)
	sql_results.FromTime = pd.to_datetime(sql_results.FromTime)
	print(sql_results)
	connection.close()

	_plot_carbon_footprint(sql_results)

	return

if __name__ == '__main__':
	main()

