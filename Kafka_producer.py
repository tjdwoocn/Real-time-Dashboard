import time
import json
from kafka import KafkaProducer
import requests

# kafka broker local server
kafka_bootstrap_servers = '127.0.0.1:9092'
# topic name
kafka_topic_name = 'weatherinfo'

# producer 객체 생성
producer = KafkaProducer(bootstrap_server = kafka_bootstrap_servers,
                        value_serializer = lambda v: json.dumps(v).encode('utf-8'))

json_message = None
city_name = None
temperature = None
humidity = None
openweathermap_api_endpoint = None
appid = None

# 날씨 정보 Json 형태로 받아오기
def get_weather_detail(openweathermap_api_endpoint):
    # api_key = 58cb3ff75f312bb25da103448913d969
    # api 호출 & get response
    api_response = requests.get(openweathermap_api_endpoint)
    # json 형태로 변환
    json_data = api_response.json()
    # 필요 정보만 저장
    city_name = json_data['name']
    humidity = json_data['main']['humidity']
    temperature = json_data['main']['temp']
    json_message = {'CityName': city_name, 'Temperature': temperature, "Humidity": humidity,
                    'CreationTime': time.strftime('%Y-%m-%d %H:%M:%S')}

    return json_message


def get_appid(appid):
    if appid is None:
        appid = 'WeatherInfo'
    else:
        pass

while True:
    # 날씨 정보 가져올 도시
    city_name = 'Seoul'
    appid = get_appid(appid)
    # 발급받은 api 입력, 날씨 확인할 'city_name' 입력
    openweathermap_api_endpoint = 'http://api.openweathermap.org/data/2.5/weather?appid=' + appid + '&q=' + city_name
    # 'get_weather_detail' 함수로 가져온 json 형태의 날씨정보 입력
    json_message = get_weather_detail(openweathermap_api_endpoint)
    # 지정된 kafka topic의 프로듀서로 메세지 전송 
    producer.send(kafka_topic_name, json_message)
    print('Published message 1: ' + json.dumps(json_message))
    print('Wait for 2 seconds ...')
    # 2초간 정지
    time.sleep(2)

    city_name = 'Busan'
    appid = get_appid(appid)
    openweathermap_api_endpoint = 'http://api.openweathermap.org/data/2.5/weather?appid=' + appid + '&q=' + city_name
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print('Published message 1: ' + json.dumps(json_message))
    print('Wait for 2 seconds ...')
    time.sleep(2)

    city_name = 'Daegu'
    appid = get_appid(appid)
    openweathermap_api_endpoint = 'http://api.openweathermap.org/data/2.5/weather?appid=' + appid + '&q=' + city_name
    json_message = get_weather_detail(openweathermap_api_endpoint)
    producer.send(kafka_topic_name, json_message)
    print('Published message 1: ' + json.dumps(json_message))
    print('Wait for 2 seconds ...')
    time.sleep(2)