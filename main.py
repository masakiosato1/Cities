import weather
from config import config




def main():
	weather_pipeline = weather.weather_pipeline(config())
	
	weather_pipeline.get_city_data()
	weather_pipeline.get_weather_data()
	weather_pipeline.write_weather_data()
	



if __name__ == '__main__':
	main()