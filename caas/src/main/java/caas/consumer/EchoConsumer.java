package caas.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

// Go through all the basic API stuff with this consumer
// The other consumers would rather help imagine the real streaming use case
public class EchoConsumer {

    private static final Logger log = LoggerFactory.getLogger(EchoConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "calculations-echo-consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "calculations-echo-consumer-group");

        Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("calculate"));

        final Thread main = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Shutdown hook invoked, waking up Consumer to abort polling");
                consumer.wakeup();
                // hook recommendation picked from Neha's book
                // why join on main?
                // else if we don't wait on main to end, we don't guarantee the close to be called, JVM might shut before that
                // TODO: there should be a better way?
                try {
                    main.join();
                }
                catch (InterruptedException e){
                    log.info("", e);
                }
            }
        });

        try {
            while(true){
                /*  what happens in the poll loop?
                    we get logs from Fetcher, FetchSessionHandler, ConsumerCoordinator before the 5 seconds
                */
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));

                for(ConsumerRecord record: records){
                    log.info(record + ""); // toString()
                }
            }
        }
        catch (WakeupException e){
            // shutdown
        }
        finally {
            // very imp! explicit indication to GroupCoordinator to trigger rebalance
            // read more in the book by Neha
            log.debug("Calling close() on Consumer");
            consumer.close();
            log.debug("Consumer closed.");
        }
    }
}
/**
 * AbstractCoordinator (discover group coordinator mbp:9092)
 * ConsumerCoordinator - gives newly assigned partitions
 */
