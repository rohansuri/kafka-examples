package caas.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.sys.Prop;

import java.time.Duration;
import java.util.*;

public class SavedStateConsumer {
    private static final Logger log = LoggerFactory.getLogger(SavedStateConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "calculations-saved-state-consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "calculations-saved-state-consumer-group");

        // oh, we must've crashed and just come up -- or maybe a graceful restart
        // we gotta rebuild our state first
        // let's do it by consuming our "calculated" output topic first

        Map<String, Integer> userState = buildUserState(properties);

        // now let's consume our running topic of operations
        // from where?
        // from where we were?
        // hmm...I could've crashed at any moment
        // I could've pushed intermediate results inside my consumer loop to output topic "calculated"
        // And didn't get a chance to commit my consumption (next poll didn't happen)
        // Hence I run the risk of reconsuming the same operations!
        // What do I do?
        // Turn off auto-offset-commit firstly
        // We need to do:
        //  - consume record
        //  - produce result record
        //  - commit consuming topic
        // this atomically (i.e in a transaction)
        Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
         consumer.subscribe(Collections.singletonList("calculate"));

        Producer<String, String> producer = setupProducer();

        try {
            while(true){
                /*  TODO what happens in the poll loop?
                    we get logs from Fetcher, FetchSessionHandler, ConsumerCoordinator before the 5 seconds
                */
                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(5000));

                for(ConsumerRecord<String, String> record: records){
                    log.info(record + "");

                    if(!userState.containsKey(record.key())){
                        userState.put(record.key(), 0);
                    }

                    // TODO integer, operator validation should've happened on producer side
                    userState.put(record.key(), doOperation(userState.get(record.key()),
                            record.value().charAt(0),
                            Integer.parseInt(record.value().substring(1))));

                    producer.send(new ProducerRecord<String, String>(record.key(),
                                                                    String.valueOf(userState.get(record.key()))));
                }
                log.info(userState.toString());
            }
        }
        finally {
             consumer.close();
        }
    }

    // TODO: what if TopicPartition assignments change after we return from here?
    // our built user state would be incorrect
    // if we got assigned new partitions then we'd have no state for users in those partitions
    // what if assignments were taken away? -- no harm then
    private static Map<String, Integer> buildUserState(Properties properties){
        Map<String, Integer> userState = new HashMap<>();

        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("calculated"));

        Map<TopicPartition, Long> currentConsumption = new HashMap<>();

        do {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            for(ConsumerRecord<String, String> record: records){

                // the compacted topic "calculated" may not be completely compacted at all times
                // so we may put for the same key multiple times -- doesn't matter
                // but just to show the behaviour of compaction
                if(userState.containsKey(record.key())){
                    log.debug("Operations for user {} haven't completely been compacted yet", record.key());
                }
                userState.put(record.key(), Integer.parseInt(record.value()));

                currentConsumption.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            }
            // if end of all partitions that are assigned to me, then quit loop
            // since I'm going to be the one producing next...

        } while (allConsumed(consumer, currentConsumption));

        return userState;
    }

    private static boolean allConsumed(Consumer<String, String> consumer, Map<TopicPartition, Long> currentConsumption){
        Set<TopicPartition> assignments = consumer.assignment();
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(assignments);

        for(TopicPartition assignment: assignments){
            if(currentConsumption.containsKey(assignment) &&
                    !currentConsumption.get(assignment).equals(endOffsets.get(assignment))){
                return false;
            }
        }
        return true;
    }

    // really, in demo we're only interested in one of these "+"
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

    private static Producer<String, String> setupProducer(){
        Properties props = new Properties();
        // later replace them with constants from ProducerConfig
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer<>(props);
    }
}
