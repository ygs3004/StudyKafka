package io.conducktor.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("Hello world");

        // 1. Producer Properties 생성
        Properties properties = new Properties();
        // localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");
        // Conduktor Properties Example
        // properties.setProperty("bootstrap.servers", ""); // localhost
        // properties.setProperty("security.protocol", ""); // localhost
        // properties.setProperty("sasl.jaas.config", ""); // localhost
        // properties.setProperty("sasl.mechanism", ""); // localhost

        // 2. Serializer Properties 설정
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // 3. Producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 4. 전송 정보 생성
        // kafka-topics.sh --bootstrap-server localhost:9092 --topic demo_java --create --partitions 3  --> CLI 를 통한 토픽 생성
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");
        
        // 5. 데이터 전송
        producer.send(producerRecord);
        
        // 6. 실행 - 동기적(synchronous)
        producer.flush();
        producer.close();

        // CLI 메세지 확인 => kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java --from-beginning
    }

}
