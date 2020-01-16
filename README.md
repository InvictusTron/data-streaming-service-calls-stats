# data-streaming-service-calls-stats

## Add the following configuration to `config/server.properties`

`
port=9092
offsets.topic.replication.factor=1
`

## Run `Zookeeper` and `Kafka`, each in a separate terminal
`
/usr/bin/zookeeper-server-start config/zookeeper.properties
/usr/bin/kafka-server-start config/server.properties
`

## In a separate terminal, run the `producer` program
`
python producer-server.py
`

## In yet another separate terminal, run the `consumer` program
* Either
`python consumer_server.py`
* OR
`/usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic SFPD_Service_Calls --from-beginning`
