package caas.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.requests.IsolationLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class SavedStateConsumer {
    private static final Logger log = LoggerFactory.getLogger(SavedStateConsumer.class);
    private static final String CONSUMER_GROUP = "calculations-saved-state-consumer-group";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "calculations-saved-state-consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // now let's consume our running topic of operations
        // from where?
        // from where we were?
        // hmm...I could've crashed at any moment
        // I could've pushed intermediate results inside my consumer loop to stateStore
        // And didn't get a chance to commit my consumption (next poll didn't happen)
        // Hence I run the risk of reconsuming the same operations!
        // What do I do?
        // Turn off auto-offset-commit firstly
        // We need to do:
        //  - consume record
        //  - produce result record
        //  - commit consuming topic
        // this atomically (i.e in a transaction)

        Map<String, Integer> userState = new HashMap<>();
        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList("calculate"), new Rebalance(userState));

/*        // oh, we must've crashed and just come up -- or maybe a graceful restart
        // we gotta rebuild our state first
        // let's do it by consuming the stateStore topic first
        userState.putAll(buildUserState(consumer.assignment()));

        // the stateStores don't exist
        if(userState.isEmpty()){
            log.info("State stores don't exist, will consume from beginning");
            consumer.poll(0);
            consumer.seekToBeginning(consumer.assignment());
        }

        log.info("UserState built from StateStores before resuming consumption: {}", userState.toString());*/

        Producer<String, String> producer = setupProducer();
        producer.initTransactions();

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

        setupShutdownHook(consumer);

        try {
            while(true){

                ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(5000));
                producer.beginTransaction();

                for(ConsumerRecord<String, String> record: records){
                    log.info("{}:{}", record.key(), record.value());

                    if(!userState.containsKey(record.key())){
                        userState.put(record.key(), 0);
                    }

                    currentOffsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, ""));

                    userState.put(record.key(), doOperation(userState.get(record.key()),
                            record.value().charAt(0),
                            Integer.parseInt(record.value().substring(1))));

                    producer.send(new ProducerRecord<>(getStateStoreTopic(new TopicPartition(record.topic(),
                                                                          record.partition())),
                                                        record.key(), // userID based compaction
                                                        String.valueOf(userState.get(record.key()))));
                }
                log.info(userState.toString());
                producer.sendOffsetsToTransaction(currentOffsets, CONSUMER_GROUP);
                producer.commitTransaction();
            }
        }
        catch (WakeupException e){}
        finally {
            producer.close();
            consumer.close();
        }
    }

    private static String getStateStoreTopic(TopicPartition topicPartition){
        return CONSUMER_GROUP + "-" + topicPartition.topic() + "-" + topicPartition.partition();
    }

    // state stores have only one partition
    private static TopicPartition getStateStoreTopicPartition(TopicPartition topicPartition){
        return new TopicPartition(getStateStoreTopic(topicPartition), 0);
    }

    private static Map<String, Integer> buildUserState(Collection<TopicPartition> topicPartitions){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // important for the state store
        properties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));

        // we never even want to commit
        // since we consume the entire state store each time
        // (this is bad, but for time being...)

        Map<String, Integer> userState = new HashMap<>();

        List<TopicPartition> stateStores = new ArrayList<>(topicPartitions.size());
        for(TopicPartition topicPartition: topicPartitions){
            stateStores.add(getStateStoreTopicPartition(topicPartition));
        }

        Consumer<String, String> consumer = new KafkaConsumer<>(properties);
        // we don't need group management facilities for state stores, do we?
        // because there's only one partition to consume
        consumer.assign(stateStores);

        try {
            consumer.poll(0);
        }
        // not assigned any topics/partitions -- maybe/mostly because it's the very first launch!
        // when the state stores don't even exist yet
        // this logic is wrong/incomplete! What if while restarts the topic partitions got resized?
        // and for some of the TPs we already had the state store? but for this new one
        // we don't? we should at least the stateStores for those TPs for which they exist
        // TODO: at least build state store for those which exist
        catch (IllegalStateException e){
            return userState;
        }

        consumer.seekToBeginning(stateStores);

        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(stateStores);

        log.info("End offsets for StateStores: {}", endOffsets);

        Map<TopicPartition, Long> currentConsumption = new HashMap<>();
        ConsumerRecords<String, String> records;

        do {
            records = consumer.poll(Duration.ofMillis(5000));

            for(ConsumerRecord<String, String> record: records){
                log.info("StateStore consumption: key:{}, value:{}, topic:{}, partition:{}, offset:{}", record.key(),
                        record.value(), record.topic(), record.partition(), record.offset());
                // safe to simply put for any given key, since StateStores are compacted topics
                // duplicate keys would mean the later is an update for the same key
                userState.put(record.key(), Integer.parseInt(record.value()));
                currentConsumption.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            }

            // if we reached the end of all partitions that are assigned to me, then quit loop
            // since I'm going to be the one producing next...

        } while (!records.isEmpty() && !allConsumed(endOffsets, currentConsumption));
        // TODO: do we need allConsumed? Isn't isEmpty() enough?

        consumer.close();

        return userState;
    }

    private static boolean allConsumed(Map<TopicPartition, Long> endOffsets, Map<TopicPartition, Long> currentConsumption){
        for(Map.Entry<TopicPartition, Long> tp: currentConsumption.entrySet()){
           if(!endOffsets.containsKey(tp.getKey()) || endOffsets.get(tp.getKey()).equals(tp.getValue())){
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
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "state-store-producer-" + ThreadLocalRandom.current().nextInt());

        log.info("Created state store producer with transaction.id {}", props.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG));

        return new KafkaProducer<>(props);
    }

    private static class Rebalance implements ConsumerRebalanceListener {
        private final Map<String, Integer> userState;

        public Rebalance(Map<String, Integer> userState){
            this.userState = userState;
        }

        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            // we're currently rebuilding the state for all partitions
            // even for all of those for which we already have state built
            // (i.e the ones which got assigned again back to us)
            // this is only for the example!
            log.info("Partitions Rebalanced, build local state by consuming StateStores for {}", partitions);
            userState.clear();
            userState.putAll(buildUserState(partitions));
        }
    }

    private static void setupShutdownHook(Consumer consumer){
        final Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Shutdown hook invoked, waking up Consumer to abort polling");
                consumer.wakeup(); // causes WakeupException on main thread

                // we join on main else if we don't wait on main to end,
                // we don't guarantee the close to be called, JVM might shut before that
                try {
                    main.join();
                }
                catch (InterruptedException e){
                    log.info("", e);
                }
            }
        });
    }

}
