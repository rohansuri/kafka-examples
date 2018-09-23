package caas.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;

/*
Go through all the basic API stuff with this consumer
The other consumers would rather help imagine the real streaming use case

Easy to see group rebalancing with this, since we consume from the latest offset at all times
 */
public class EchoConsumer {

    private static final Logger log = LoggerFactory.getLogger(EchoConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        // properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "calculations-echo-consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "calculations-echo-consumer-group");
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1 * 1000);
        // properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 5 * 1000);

        Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("calculate"));

        setupShutdownHook(consumer);

        try {
            while(true){
                /*  what happens in the poll loop?
                    we get logs from Fetcher, FetchSessionHandler, ConsumerCoordinator before the 5 seconds
                    RebalanceListeners are also called in poll
                */
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

                for(ConsumerRecord record: records){
                    // printing to console is a non-transactional, non-idempotent operation
                    // you can't take it back
                    // but hey, this is just a simple EchoConsumer example
                    // and therefore we have no explicit commits/rebalance listeners
                    log.info("{}:{}", record.key(), record.value());
                    // log.info(record + ""); // toString()
                }
            }
        }
        catch (WakeupException e){
            // shutdown
        }
        finally {
            log.debug("Calling close() on Consumer");
            consumer.close(); // very imp! explicit indication to GroupCoordinator to trigger rebalance
            log.debug("Consumer closed.");
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
/**
 * AbstractCoordinator (discover group coordinator mbp:9092)
 * ConsumerCoordinator - gives newly assigned partitions
 */
