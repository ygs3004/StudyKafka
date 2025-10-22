package io.conducktor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class);

    // 동일한 Key로 재전송시 동일 파티션으로 전송하는지 체크하는 Method
    public static void main(String[] args) {
        log.info("Hello ProducerDemoWithCallback");

        // 1. Producer Properties 생성
        Properties properties = new Properties();
        // localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // 2. Serializer Properties 설정
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // 3. Producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 동일한 Key로 재전송시 동일 파티션으로 전송되는지 루프 체크
        for (int j = 0; j < 2; j++) {

            for (int i = 0; i < 10; i++) {

                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello value " + i;

                // 4. 전송 정보 생성
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // 5. 데이터 전송
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        // 전송 성공 or 실패
                        if (e == null) {
                            // 성공
                            log.info("Key: " + key + " | Partition: " + metadata.partition()
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
