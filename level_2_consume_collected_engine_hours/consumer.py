# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
from kafka import KafkaProducer
import json
# Import sys module
import sys

# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name from where the message will recieve
consumerTopicName = 'all_engine_hours'
producerTopicName = 'formatted_engine_hours'


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


def parse_c_data(c, t1, t2):
    eng_data = dict()
    eng_data['unit'] = c[0]
    eng_data['beginning'] = t1
    eng_data['initial_location'] = c[3]['t']
    eng_data['end'] = t2
    eng_data['final_location'] = c[5]['t']
    eng_data['total_time'] = c[6]
    eng_data['off_time'] = c[7]
    eng_data['engine_hours'] = c[8]
    eng_data['in_motion'] = c[9]
    eng_data['idling'] = c[10]
    eng_data['mileage'] = c[11]
    eng_data['productivity'] = c[12]
    eng_data['movement_productivity'] = c[13]
    eng_data['driver'] = c[14]
    eng_data['penalties'] = c[15]
    eng_data['utilization'] = c[16]
    eng_data['consumed'] = c[17]
    eng_data['consumed_by_math'] = c[18]
    eng_data['avg_consumption_by_math'] = c[19]
    eng_data['consumed_by_math_in_motion'] = c[20]
    eng_data['avg_consumed_by_math_in_motion'] = c[21]
    eng_data['consumed_by_math_in_idle_run'] = c[22]
    eng_data['avg_consumed_by_math_in_idle_run'] = c[23]
    eng_data['avg_consumed_by_math_in_trips'] = c[24]

    return eng_data


def get_formatted_engine_hours_record(engh):
    print(engh)
    c = engh.get("c")
    t1 = engh.get("t1")
    t2 = engh.get("t2")
    eng_data = parse_c_data(c, t1, t2)
    return eng_data


# Initialize consumer variable
engine_consumer = KafkaConsumer(consumerTopicName, group_id='engine_hours', bootstrap_servers=bootstrap_servers)

engine_producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=json_serializer)

# Read and print message from consumer
for msg in engine_consumer:
    print("Topic Name=%s,Message=%s" % (msg.topic, msg.value))
    data = json.loads(msg.value)
    engine_hours = data.get('engine_hours')
    # print(engine_hours)
    incoming_batch = []
    for engh in engine_hours:
        incoming_batch.append(get_formatted_engine_hours_record(engh))
    engine_producer.send(producerTopicName, incoming_batch)

# Terminate the script
sys.exit()
