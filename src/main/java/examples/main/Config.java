package examples.main;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.support.LogIfLevelEnabled.Level;

@Configuration
@EnableKafka // for @KafkaListener annotations to be processed
public class Config {
	
	// we need a kmlcFactory when using the EnableKafka annotation
	// that creates the containers on the fly for any endpoints/listeners it identifiers at runtime
	/*
	@Bean
	public KafkaMessageListenerContainer kmlc() {
		// single threaded message listener container
		// listener container means it contains reference to the message handling/listener code
		// it gives the call to the handle registered
		// also it listens to topic by using the factory to create a consumer
		
		// we need to tell KMLC two things: 
		// give it a consumer factory that will hand it over a connection to the kafka broker
		// (basically something that knows the host:port for the Kafka broker
		// and
		// the 2nd thing being what topic should I listen to?
		ContainerProperties containerProperties = new ContainerProperties("orders"); 
		KafkaMessageListenerContainer<Integer, String> kmlc = new KafkaMessageListenerContainer<>(consumerFactory(),
				containerProperties);
		return kmlc;
	}*/
	
	// whenever runtime comes across a KafkaListener, it would want to create a listener container for this particular endpoint
	// and hence it needs a factory! and the default factory it looks for is with name "kafkaListenerContainerFactory"
	// ah! I see, there's a single listener to listenerContainer correlation :)
	// hence we need a container factory too!
	// so that means, I could now easily have another Listener, say StockService, and spring runtime would nicely
	// give me a container for that too
	// Another point to note, since the factory only needs to understand how to create consumers
	// and it's a factory, it can't be linked to a topic yet, a kmlc is linked to a topic
	// but the kmlcFactory is a factory that could generate kmlc for any topic
	// hence we actually now need to specify the topic the listener listens to, with the listener
	// annotation @KafkaListener
	@Bean
	public ConcurrentKafkaListenerContainerFactory<Integer, String> kafkaListenerContainerFactory(){
		ConcurrentKafkaListenerContainerFactory<Integer, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
		factory.getContainerProperties().setCommitLogLevel(Level.INFO);
		// TODO: difference between MANUAL and MANUAL_IMMEDIATE?
		factory.getContainerProperties().setAckMode(AckMode.MANUAL_IMMEDIATE);
		factory.setConsumerFactory(consumerFactory());
        return factory;
	}
	
	// hello world for now, later orderService and stockService
	@Bean
	public Listener listener() {
		return new Listener();
	}
	
	@Bean
	public ConsumerFactory<Integer, String> consumerFactory() {
		DefaultKafkaConsumerFactory<Integer, String> factory = new DefaultKafkaConsumerFactory<>(consumerConfig());
		return factory;
	}
	
	private Map<String, Object> consumerConfig(){
		Map<String, Object> props = new HashMap<>();
	    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // TODO: kafka brokers
	    
	    // this is no where being used! rather the id specified in KafkaListener got picked up as "group"
	    // props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumers2"); 
	    
	    // o.s.k.l.KafkaMessageListenerContainer$ListenerConsumer is who actually commits the offset
	    // setting this to false, alone doesn't work, because the AckMode by default is BATCH
	    // which is to commit the offset after a batch of messages is received
	    // hence apart from a false here, we need to configure the "Ack" mode on spring
	    // to also sort of play well and make it manual commit 
	    
	    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // TODO: ??
	    // props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
	    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000"); // when to initiate rebalance
	    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
	    // why have javadocs as string constant?
	    
	    // let's subscribe to __consumer_offsets topic!
	    // but the high-level consumer we use (given by spring-kafka abstraction), we can't deserialize this consumer_offset group
	    // it throws SerializationException
	    // (I did read somewhere that the offsets topic also has it's schema)
	    // but the kafka-clients jar does not include the GroupMetadataManager that knows how to deserialize them
	    // Maybe see how kafka-console-producer does it with the GroupMetadatManager's formatter
	    // https://stackoverflow.com/questions/35107824/in-kafka-0-9-is-there-a-way-i-can-list-out-the-offset-for-all-the-consumers-in-a
	    props.put(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, false);
	    
	    // key? on which the partition is distributed/picked on?
	    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
	    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	    return props;
	}
}

class Listener {

	// TODO: according to my understanding, not specifying the topic here should fail the creation of listener?
	// Verify?
	
	// by default the String foo below is annotated with @Payload, hence we only receive the payload
	
	// TODO: topics here can take a string array? How would the same listener listen to multiple topics?
	// how would you differentiate between messages from different topics in that case?
	
	// having same clientId but being in different groups, I still don't receive the message
	// auto.offset.reset
	
    @KafkaListener(id = "orderListener12", topics = {"orders","__consumer_offsets"})
    public void listen(ConsumerRecord<?, ?> foo) {
        //System.out.println("Listener called with: " + foo);
    }

}
