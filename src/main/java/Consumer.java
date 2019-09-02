import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Consumer {

    private final Logger mLogger = LoggerFactory.getLogger(Consumer.class.getName());
    private final String mBootstrapServer;
    private final String mGroupId;
    private final String mTopic;

    public Consumer(String bootstrapServer, String groupId, String topic) {
        mBootstrapServer = bootstrapServer;
        mGroupId = groupId;
        mTopic = topic;
    }

    public static void main(String[] args) throws InterruptedException {
        String server = "127.0.0.1:9092";
        String groupId = "some_application";
        String topic = "user_registered";

        new Consumer(server, groupId, topic).run();
    }

    private Properties consumerProps(String bootstrapServer, String groupId) {
        String deserializer = StringDeserializer.class.getName();
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return properties;
    }

    public void run() throws InterruptedException {
        mLogger.info("Creating consumer thread");

        CountDownLatch latch = new CountDownLatch(1);

        ConsumerRunnable consumerRunnable = new ConsumerRunnable(mBootstrapServer, mGroupId, mTopic, latch);
        Thread thread = new Thread(consumerRunnable);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            mLogger.info("Caught shutdown hook");
            consumerRunnable.shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            mLogger.info("Application has exited");
        }));

        latch.await();
    }

    private class ConsumerRunnable implements Runnable {

        private CountDownLatch mLatch;
        private KafkaConsumer<String, String> mConsumer;

        public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
            this.mLatch = latch;

            Properties props = consumerProps(bootstrapServer, groupId);
            mConsumer = new KafkaConsumer<>(props);
            mConsumer.subscribe(Collections.singleton(topic));
        }

        private void shutDown() {
            mConsumer.wakeup();
        }

        @Override
        public void run() {
            try {
                do {
                    ConsumerRecords<String, String> records = mConsumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        mLogger.info("Key: " + record.key() + ", Value: " + record.value());
                        mLogger.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                    }
                } while (true);
            } catch (WakeupException e) {
                mLogger.info("Received shutdown signal");
            } finally {
                mConsumer.close();
                mLatch.countDown();
            }
        }
    }
}
