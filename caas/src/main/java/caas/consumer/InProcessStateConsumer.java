package caas.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/**
 * Consumes the "calculate" topic from the beginning because it keeps all state in memory
 */
public class InProcessStateConsumer {
    private static final Logger log = LoggerFactory.getLogger(InProcessStateConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "calculations-in-process-state-consumer");
        // group is more importantly to be set (since the offset tracked is <topic, partition, groupid>
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "calculations-in-process-state-consumer-group");

        Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("calculate"));
        // Producer<String, String> producer = setupProducer();

        // we don't care what we get here, since we want to start consumption from beginning
        // and seek can't be done until we get the partitions assigned <-- imp (TODO: what happens inside poll?)
        // which happens in app's first poll call
        consumer.poll(0);
        consumer.seekToBeginning(consumer.assignment());

        // we start empty
        Map<String, Integer> userState = new HashMap<>();

        try {
            while(true){
                /*  TODO what happens in the poll loop?
                    we get logs from Fetcher, FetchSessionHandler, ConsumerCoordinator before the 5 seconds
                */
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

                for(ConsumerRecord<String, String> record: records){
                    log.info("Record consumed: {}", record);

                    if(!userState.containsKey(record.key())){
                        userState.put(record.key(), 0);
                    }

                    // TODO integer, operator validation should've happened on producer side
                    userState.put(record.key(), doOperation(userState.get(record.key()),
                                                            record.value().charAt(0),
                                                            Integer.parseInt(record.value().substring(1))));
                }

                if(!records.isEmpty()){
                    log.info("UserState In-Memory Map: {}", userState);
                }

            }
        }
        finally {
            consumer.close();
        }
    }

    // really, in demo we're only interested in one of these, maybe "+"
    private static int doOperation(int state, char operator, int operand){
        if(operator == '+'){
            return state + operand;
        }
        else if(operator == '-'){
            return state - operand;
        }
        else if(operator == '*'){
            return state * operand;
        }
        else {
            return state / operand;
        }
    }

}
