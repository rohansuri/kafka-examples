package caas.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.Reference;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Consumes the "calculate" topic from the beginning every time (even after rebalances)
 * because it keeps all state in memory, hence has no StateStore to restore a partition's consumption state
 * (unrealistic example yes -- but for demonstration only)
 */
public class InProcessStateConsumer {
    private static final Logger log = LoggerFactory.getLogger(InProcessStateConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "calculations-in-process-state-consumer");

        // group is more importantly to be set (since the offset tracked is <topic, partition, groupid>
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "calculations-in-process-state-consumer-group");

        //properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000);

        // no one ever commits, just to show rebalance listeners in effect
        // that have to rebuild the cache by going over again
        // assuming there's no StateStore
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // we start empty
        Map<String, Integer> userState = new HashMap<>();

        Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("calculate"), new RebalanceListener(consumer, userState));

        // we don't care what we get here, since we want to start consumption from beginning
        // and seek can't be done until we get the partitions assigned <-- imp (TODO: what happens inside poll?)
        // which happens in app's first poll call
        consumer.poll(0);
        consumer.seekToBeginning(consumer.assignment());

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

                    // integer, operator validation should've happened on producer side
                    userState.put(record.key(), doOperation(userState.get(record.key()),
                                                            record.value().charAt(0),
                                                            Integer.parseInt(record.value().substring(1))));
                }

                log.info("UserState In-Memory Map: {}", userState);

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

    static class RebalanceListener implements ConsumerRebalanceListener {

        private final Consumer<String, String> consumer;
        private final Map<String, Integer> userState;

        public RebalanceListener(Consumer<String, String> consumer, Map<String, Integer> userState){
            this.consumer = consumer;
            this.userState = userState;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            userState.clear();
            consumer.seekToBeginning(partitions);
        }
    }
}
/*
When is onPartitionsRevoked called?
Will it be called for LeaveGroups requests by consumer? No
It is always called from the main/foreground thread doing the polls
It could be called in following scenario

c1 - p1, p2, p3

we add a new consumer c2
rebalance

c1 - p1, p3
c2 - p2

so c1's p2 got revoked
 */

