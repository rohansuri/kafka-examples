package caas.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Scanner;

/* To see batching taking place
   ./bin/kafka-run-class.sh kafka.tools.DumpLogSegments --files /tmp/kafka-logs/calculate-0/*6.log --print-data-log
   shows different offsets but the same position in the log (hence the MessageSet abstraction)

   What if segment size is too small to fit the batch?
   Does the batch then split on the server side? or does it reject the client request?

   Relevant client-side APIs to look at for batching:
   - RecordAccumulator.ready, expiredBatches
   - MemoryRecordsBuilder
   - Sender.run
   - KafkaProducer.send
*/
public class FastCalculationsProducer {
    private static final Logger log = LoggerFactory.getLogger(FastCalculationsProducer.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        // later replace them with constants from ProducerConfig
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        Scanner in = new Scanner(System.in);

        try {
            produce(in, producer);
        }
        finally {
            in.close();
            producer.close();
        }
    }

    private static void produce(Scanner in, Producer<String, String> producer) {
        for(int i = 1; i <= 4; i++){
            ProducerRecord<String, String> record = new ProducerRecord<>("calculate", "foo", "+" + i);
            // sends are always async
            producer.send(record);
        }
    }
}
