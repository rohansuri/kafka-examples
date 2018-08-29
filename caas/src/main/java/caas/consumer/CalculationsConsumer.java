package caas.consumer;

import org.apache.kafka.clients.consumer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CalculationsConsumer {

    private static final Logger log = LoggerFactory.getLogger(CalculationsConsumer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "calculations-consumer-1");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "calculations-consumer-group");

        Consumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Collections.singletonList("calculate"));

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
        finally {
            consumer.close();
        }
    }
}
/**
 * AbstractCoordinator (discover group coordinator mbp:9092)
 * ConsumerCoordinator - gives newly assigned partitions
 */
