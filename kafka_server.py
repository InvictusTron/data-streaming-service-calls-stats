import producer_server
import os

KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "SFPD_Service_Calls"

def run_kafka_server():
	# TODO get the json file path
    input_file = os.path.abspath("police-department-calls-for-service.json")    

    # TODO fill in blanks
    producer = producer_server.ProducerServer(
        input_file=input_file,
        topic=TOPIC_NAME,
        bootstrap_servers=KAFKA_SERVER,
        client_id="rprasad-project2"
    )

    return producer


def feed():
    producer = run_kafka_server()
    producer.generate_data()


if __name__ == "__main__":
    feed()
