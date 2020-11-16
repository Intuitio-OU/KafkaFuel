# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer
from kafka import KafkaProducer
import mysql.connector
import sys
import json

cnx = mysql.connector.connect(
    user='root',
    password='',
    host='127.0.0.1',
    database='dev_fuel_db'
)

cursor = cnx.cursor()


def records_tuple(payload):
    return (
        payload['unit'],
        payload['beginning'],
        payload['initial_location'],
        payload['end'],
        payload['final_location'],
        payload['total_time'],
        payload['off_time'],
        payload['engine_hours'],
        payload['in_motion'],
        payload['idling'],
        payload['mileage'],
        payload['productivity'],
        payload['movement_productivity'],
        payload['driver'],
        payload['penalties'],
        payload['utilization'],
        payload['consumed'],
        payload['consumed_by_math'],
        payload['avg_consumption_by_math'],
        payload['consumed_by_math_in_motion'],
        payload['avg_consumed_by_math_in_motion'],
        payload['consumed_by_math_in_idle_run'],
        payload['avg_consumed_by_math_in_idle_run'],
        payload['avg_consumed_by_math_in_trips']
    )


def construct_sql():
    return """


    INSERT INTO `engine_hours_manager_enginehours`(`unit`, `beginning`, `initial_location`, `end`, `final_location`, `total_time`, `off_time`, `engine_hours`, `in_motion`, `idling`, `mileage`, `productivity`, `movement_productivity`, `driver`, `penalties`, `utilization`, `consumed`, `consumed_by_math`, `avg_consumption_by_math`, `consumed_by_math_in_motion`, `avg_consumed_by_math_in_motion`, `consumed_by_math_in_idle_run`, `avg_consumed_by_math_in_idle_run`, `avg_consumed_by_math_in_trips`) 
    VALUES (

     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s,
     %s,
     %s, 
     %s, 
     %s, 
     %s, 
     %s, 
     %s

     );

    """


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


# Define server with port
bootstrap_servers = ['localhost:9092']

# Define topic name from where the message will recieve
consumerTopicName = 'formatted_engine_hours'
producerTopicName = 'non_duplicate_engine_hour_records'

# Initialize consumer variable
engine_consumer = KafkaConsumer(consumerTopicName, group_id='engine_hours_group', bootstrap_servers=bootstrap_servers)

engine_producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=json_serializer)

# Read and print message from consumer
for msg in engine_consumer:
    print("Topic Name=%s,Message=%s" % (msg.topic, msg.value))
    data = json.loads(msg.value)
    many_rows = []
    for row in data:
        row_tuple = records_tuple(row)
        print(row_tuple)
        cursor.execute("""
          SELECT 
            COUNT(*) 
            from `engine_hours_manager_enginehours` 
          WHERE
            `unit`=%s AND `beginning`=%s
        """, (row_tuple[0], row_tuple[1]))
        for result in cursor:
            count = result[0]
            print(row_tuple[0], " COUNT :", count)
            if count == 0:
                # Use the consumer for the next level and insert it to the database
                many_rows.append(row_tuple)
    if many_rows == []:
        print("Noting to insert: ", many_rows)
    else:
        engine_producer.send(producerTopicName, many_rows)

# Terminate the script
sys.exit()
