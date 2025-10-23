package io.conducktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {
        log.info("============== Kafka Consumer ==============");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // 1. Producer Properties 생성
        Properties properties = new Properties();
        // localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // Conduktor Properties Example
        // properties.setProperty("bootstrap.servers", ""); // localhost
        // properties.setProperty("security.protocol", ""); // localhost
        // properties.setProperty("sasl.jaas.config", ""); // localhost
        // properties.setProperty("sasl.mechanism", ""); // localhost

        // 2. Consumer Serializer Properties 설정
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); // none / earliest / latest

        // 3. Consumer 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 4. Subscribe Topic
        consumer.subscribe(Arrays.asList(topic));

        // 5. 데이터 poll
        while (true) {
            log.info("Polling");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
            }
        }
    }

}
