# Connect to a node to use its terminal to run commands
docker exec -it kafka-node1 /bin/bash

# Get cluster members
/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:19092 | awk '/id/{print $1}' | sort

### TOPICS
#################
# Get the list of topics
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list

# Create a topic with defauts regarding partitions and replication factor
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --topic topic-0 --create

# Create a topic with partitions and replication factor
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --topic topic-1 --create --partitions 5 --replication-factor 2

# Create a topic (non working as replication-factor > number of nodes)
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --topic test1 --partitions 5 --replication-factor 4

# List topics
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --list

# Describe a topic
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --topic topic-1 --describe

# Delete a topic, only works if delete.topic.enable=true 
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --topic topic-1 --delete

# Alter the number of partitions (DANGEROUS OPERATION)
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --alter --topic test1 --partitions 6

### PRODUCERS
#################
# Producing messages in specific topic
/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic topic-1

# Producing messages with properties
/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic topic-1 --producer-property acks=all

# Producing messages in a non existing topic that will be created...
/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic topic-x

# ...our new topic only has 1 partition (default value)
/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --topic topic-x --describe

# Producing messages with keys (it will raise an error if key is not provided)
/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:19092 --topic topic-1 --property parse.key=true --property key.separator=:


### CONSUMERS
#################
# Consuming messages from specific topic
/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic topic-1

# Consuming messages from beginning
/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic topic-1 --from-beginning

# Consuming messages displaying key, values and timestamp
/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic topic-1 --formatter kafka.tools.DefaultMessageFormatter --property print.timestamp=true --property print.key=true --property print.value=true --property print.partition=true --property print.offset=true --from-beginning


### CONSUMERS GROUPS
####################
# Start one consumer in the specific consumer group
/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic topic-1 --group group-1

# Start one producer and start producing
/kafka/bin/kafka-console-producer.sh --boots trap-server localhost:19092 --topic topic-1

# Start another consumer part of the same group. See messages being spread
/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic topic-1 --group group-1

# Start another consumer part of a different group from beginning
/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic topic-1 --group group-2 --from-beginning

# List consumer groups
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --list
 
# Describe a specific group
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group group-1

# Describe another group
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group group-2

# Start a new consumer in a existing group
/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic topic-1 --group group-1

# Describe that group now
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group group-1

# Describe a console consumer group (change the end number)
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group console-consumer-10592


## SHOWCASE THE OFFSET IF PRODUCER PRODUCES AND THERE ARE NO CONSUMERS


### OFFSETS
####################

# Reset the offsets to the beginning of each partition, won't be executed
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --group group-1 --reset-offsets --to-earliest

# Execute flag is needed, won't be executed
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --group group-1 --reset-offsets --to-earliest --execute

# Topic flag is also needed
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --group group-1 --reset-offsets --to-earliest --execute --topic topic-1

# Consume from where the offsets have been reset
/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:19092 --topic topic-1 --group group-1

# Describe the group again
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --describe --group group-1

# Documentation for more options
/kafka/bin/kafka-consumer-groups.sh

# Shift offsets by 2 (forward) as another strategy
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --group group-1 --reset-offsets --shift-by 2 --execute --topic topic-1

# shift offsets by 2 (backward) as another strategy
/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:19092 --group group-1 --reset-offsets --shift-by -2 --execute --topic topic-1
