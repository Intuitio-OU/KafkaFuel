from .engine_wialon import get_engine_hours
import datetime
# Import KafkaProducer from Kafka library
from kafka import KafkaProducer
import json

# from data import get_registered_user
# import time

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name where the message will publish
topicName = 'all_engine_hours'


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=json_serializer)


def get_today():
    t = datetime.datetime.today()
    return datetime.datetime(t.year, t.month, t.day, 23, 59)


def get_days_range(start, end, step=5):
    days_list = []
    diff = end - start
    rng = range(0, diff.days + 1, step)
    start_of_day, end_of_day = None, None
    for i in rng:
        start_of_day = datetime.datetime(start.year, start.month, start.day, 00, 00) + datetime.timedelta(days=i)
        end_of_day = datetime.datetime(start.year, start.month, start.day, 23, 59) + datetime.timedelta(days=i)
        if end_of_day > end:
            end_of_day = end
        days_list.append(
            (
                start_of_day.timestamp(),
                end_of_day.timestamp()
            )
        )
    if end_of_day < end:
        start_of_day = datetime.datetime(end.year, end.month, end.day, 00, 00)
        end_of_day = datetime.datetime(end.year, end.month, end.day, 23, 59)
        days_list.append(
            (
                start_of_day.timestamp(),
                end_of_day.timestamp()
            )
        )
    # print(days_list)
    return days_list


start = 1577836800
end = 1580342400
today = get_today()
# print(start)
# print(end)
if end > today.timestamp():
    end = today.timestamp()
date_range = get_days_range(datetime.datetime.fromtimestamp(start), datetime.datetime.fromtimestamp(end))

for i in range(len(date_range) - 1):
    data = {}
    st_stmp = int(date_range[i][0])
    end_stmp = int(date_range[i + 1][1])
    # print(start, end)
    rows = get_engine_hours(start_timestamp=st_stmp, end_timestamp=end_stmp)
    # print(rows)
    data['start'] = st_stmp
    data['end'] = end_stmp
    data['engine_hours'] = rows
    print(data['start'], data['end'], len(data['engine_hours']))
    producer.send(topicName, data)
    # producer.flush()
