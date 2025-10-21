package io.conducktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    public static void main(String[] args) {
        log.info("Hello ProducerDemoWithCallback");

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

        // batch size properties
        properties.setProperty("batch.size", "400");

        // 3. Producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        // batch size 에 따라 partition 변경
        for (int j = 0; j < 10; j++) {

            // StickyPartitioner, batch size 에 따라 하나의  파티션으로 Batch 처리
            for (int i = 0; i < 10; i++) {
                // 4. 전송 정보 생성
                // kafka-topics.sh --bootstrap-server localhost:9092 --topic demo_java --create --partitions 3 --> CLI 를 통한 토픽 생성
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello Kafka" + i);

                // 5. 데이터 전송
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // 전송 성공 or 실패
                        if (e == null) {
                            // 성공
                            log.info("====== Success ====== " + System.lineSeparator() +
                                    "Topic: " + metadata.topic() + System.lineSeparator() +
                                    "Partition: " + metadata.partition() + System.lineSeparator() +
                                    "Offset: " + metadata.offset() + System.lineSeparator() +
                                    "Timestamp: " + metadata.timestamp()
                            );
                        } else {
                            // 실패
                            log.error("Exception Producer Send", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // 6. 실행 - 동기적(synchronous)
        producer.flush();
        producer.close();

        // CLI 메세지 확인 => kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo_java --from-beginning
    }

}
