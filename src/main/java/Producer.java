import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {

    final KafkaProducer<String, String> mProducer;
    final Logger mLogger = LoggerFactory.getLogger(Producer.class);

    public Producer(String bootstrapServer) {
        Properties props = producerProps(bootstrapServer);
        mProducer = new KafkaProducer<>(props);

        mLogger.info("Producer initialized");
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        String server = "127.0.0.1:9092";
        String topic = "user_registered";

        Producer producer = new Producer(server);
        producer.put(topic, "user1", "John");
        producer.put(topic, "user2", "Peter");
        producer.close();
    }

    /**
     *
     * @param bootstrapServer an IP for the Kafka broker
     * @return
     */
    private Properties producerProps(String bootstrapServer) {
        String serializer = StringSerializer.class.getName();
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializer);
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, serializer);

        return props;
    }

    public void put(String topic, String key, String value)
            throws ExecutionException, InterruptedException {
        mLogger.info("Put value: " + value + ", for key: " + key);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        mProducer.send(record, (recordMetadata, e) -> {
            if(e != null) {
                mLogger.error("Error while producing", e);
                return;
            }

            mLogger.info("Recieved new meta. \n" +
                    "Topic: " + recordMetadata.topic() + "\n" +
                    "Partition: " + recordMetadata.partition() + "\n" +
                    "Offset: " + recordMetadata.offset() + "\n" +
                    "Timestamp: " + recordMetadata.timestamp());
        }).get();
    }

    public void close() {
        mLogger.info("Closing producer's connection");
        mProducer.close();
    }
}
