package caas.producer;

import java.util.Arrays;
import java.util.Properties;
import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes calculations from stdin and produces them to topic "calculate"
 */
public class CalculationsProducer {

	private static final Logger log = LoggerFactory.getLogger(CalculationsProducer.class);

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		
		Scanner in = new Scanner(System.in);
		
		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				log.info("Shutdown hook invoked, closing Producer and stdin Scanner");
				in.close();
				producer.close();
			}
		});
		
		processStdin(in, producer);
	}
	
	private static void processStdin(Scanner in, Producer<String, String> producer) {
		log.info("Separate key, value by single space");
		// TODO java 8 streams style?
		while(true) {
			String input[] = in.nextLine().split(" ");
			// System.out.printf(Arrays.toString(input));
			ProducerRecord<String, String> record = new ProducerRecord<>("calculate", input[0], input[1]);
			// see DefaultPartitioner that already chooses the target partition for us, if keys are provided
			// by doing a murmur hash % no of partitions
			producer.send(record);
		}
	}

}
