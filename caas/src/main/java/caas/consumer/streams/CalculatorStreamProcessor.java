package caas.consumer.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Properties;

public class CalculatorStreamProcessor {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "calculator-stream-processor");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> kstream = builder.stream("calculate");

        KTable<String, Integer> ktable = kstream.groupByKey().<Integer>aggregate(() -> 0, (userId, op, currState) -> {
            char c = op.charAt(0);
            // System.out.println("currentState:" + currState);
           // System.out.println("OPERATION DONE: " + c);
            if(c == '+'){
             //   System.out.println("result: " + currState + Integer.parseInt(op.substring(1)));
                return currState + Integer.parseInt(op.substring(1));
            }
            return currState;
        }, Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("calculations").withValueSerde(Serdes.Integer()));

        ktable.toStream().print(Printed.toSysOut());

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

    }
}
