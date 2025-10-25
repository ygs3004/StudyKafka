package io.conducktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class);

    public static void main(String[] args) {
        log.info("============== Kafka Consumer ==============");

        String groupId = "my-java-application";
        String topic = "demo_java";

        // Consumer Properties 생성
        Properties properties = new Properties();
        // localhost
        properties.setProperty("bootstrap.servers", "localhost:9092");

        // Consumer Serializer Properties 설정
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest"); // none / earliest / latest

        // Stop the world 없이 partition rebalancing
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        // properties.setProperty("group.instance.id", "...."); // strategy of static assignment

        // Consumer 생성
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 메인 쓰레드 종료시 shutdown hook 추가
        final Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                log.info("Detected a shutdown, by consumer.wakeup()");
                // 메인 쓰레드의 consumer 객체에 wakeup exception 발생
                consumer.wakeup();

                // main thread를 감지
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            }
        });

        try{

            // Subscribe Topic
            consumer.subscribe(Arrays.asList(topic));

            // 데이터 poll
            while (true) {
                // log.info("Polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }

        }catch (WakeupException e){
            log.info("Consumer is starting to shutdown");
        }catch (Exception e){
            log.error("Unexpected Exception", e);
        }finally {
            consumer.close(); // consumer close, offset commit
            log.info("The consumer is now gracefully shutdown");
        }

    }

}
