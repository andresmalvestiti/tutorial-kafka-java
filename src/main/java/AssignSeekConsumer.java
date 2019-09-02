import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 *  I do this in a very imperative way just for the sake of clarity.
 *  Don’t write such code in production, please.
 */
public class AssignSeekConsumer {

    private final Logger mLogger = LoggerFactory.getLogger(Consumer.class.getName());
    private final String mBootstrapServer;
    private final String mTopic;

    public AssignSeekConsumer(String bootstrapServer, String topic) {
        this.mBootstrapServer = bootstrapServer;
        this.mTopic = topic;
    }

    public static void main(String[] args) {
        String server = "127.0.0.1:9092";
        String topic = "user_registered";
        long offset = 15L;
        int partitionNum = 0;
        int numOfMessages = 5;

        new AssignSeekConsumer(server, topic).run(offset, partitionNum, numOfMessages);
    }

    private Properties consumerProps(String bootstrapServer) {
        String deserializer = StringDeserializer.class.getName();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }

    public void run(long offset, int partitionNum, int numOfMessages) {
        Properties props = consumerProps(mBootstrapServer);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        setupConsumer(consumer, offset, partitionNum);
        fetchMessages(consumer, numOfMessages);
    }

    private void setupConsumer(KafkaConsumer<String, String> consumer, long offset, int partitionNum) {
        TopicPartition partition = new TopicPartition(mTopic, partitionNum);
        consumer.assign(Collections.singletonList(partition));
        consumer.seek(partition, offset);
    }

    private void fetchMessages(KafkaConsumer<String, String> consumer, int numOfMessages) {
        int numberOfMessagesRead = 0;
        boolean keepOnReading = true;

        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesRead++;

                mLogger.info("Key: " + record.key() + ", Value: " + record.value());
                mLogger.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                if (numberOfMessagesRead >= numOfMessages) {
                    keepOnReading = false;
                    break;
                }
            }
        }
    }


}
