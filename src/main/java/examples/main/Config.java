package examples.main;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

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
	    props.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumers"); 
	    
	    
	    // props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true); // TODO: ??
	    // props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
	    props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "15000"); // when to initiate rebalance
	    
	    // why have javadocs as string constant?
	    
	    // key? on which the partition is distributed/picked on?
	    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
	    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
	    return props;
	}
}

class Listener {

	// TODO: according to my understanding, not specifying the topic here should fail the creation of listener?
	// Verify?
	// TODO: no way to get the key with which the message was published?
    @KafkaListener(id = "orderListener", topics = "orders")
    public void listen(String foo) {
        System.out.println("Listener called with: " + foo);
    }

}
