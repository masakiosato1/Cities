import pandas as pd
import requests
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

class weather_pipeline:

	def __init__(self, params):
	    self.params = params

	def get_city_data(self):
		params = self.params
		url = URL.create(**params)
		print(url)
		self.engine = create_engine(url, echo=True)
		self.connection = self.engine.connect()
		
		self.df00 = pd.read_sql( 
		    'select * from city', 
		    con=self.engine 
		) 


	def get_weather_data(self):
		city_data = []
		state_data = []
		temperature_data = []
		date_data = []
		c = 0
		tc = self.df00.shape[0]

		for i, row in self.df00.iterrows():
		    c += 1
		    lat = row['lat']
		    lng = row['lng']
		    url = f'https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lng}&start_date=2023-01-01&end_date=2023-12-31&daily=temperature_2m_max&temperature_unit=fahrenheit'
		    response = requests.get(url)
		    response_data = response.json()['daily']['temperature_2m_max']
		    temperature_data += response_data
		    city_data += [row['city']]*len(response_data)
		    state_data += [row['state_id']]*len(response_data)
		    date_data += response.json()['daily']['time']
		    b = f"{int(c*100/tc)}% complete"
		    print (b, end="\r")
		    
		    

		final_data = [city_data, state_data, date_data, temperature_data]


		transposed_data = [list(i) for i in zip(*final_data)]
		self.df01 = pd.DataFrame(transposed_data, columns = ['city', 'state_id', 'date', 'temperature'])


	def write_weather_data(self):
		self.connection.execute('drop table if exists weather')
		self.df01.to_sql(name='weather', con=self.engine)


