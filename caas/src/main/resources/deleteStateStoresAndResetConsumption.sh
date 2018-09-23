# delete state store topics
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic calculations-saved-state-consumer-group-calculate-0
./bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic calculations-saved-state-consumer-group-calculate-1

# reset consumption
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --topic calculate --to-earliest --group calculations-saved-state-consumer-group --reset-offsets --execute

# current consumption
./bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group calculations-saved-state-consumer-group
