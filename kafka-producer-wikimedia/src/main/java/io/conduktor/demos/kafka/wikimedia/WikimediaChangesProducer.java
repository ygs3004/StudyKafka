package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import okhttp3.Headers;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    public static void main(String[] args) throws InterruptedException {

        String bootstrapServer = "localhost:9092";
        // Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 안정성을 위한 configs(Kafka <= 2.8)
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // 멱등성 처리
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all"); // -1, replica의 데이터 복제처리 안정성
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));

        // Producer 생성
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        Headers headers = Headers.of("User-Agent", "kafka-demo"); // 없을시 403
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url)).headers(headers);
        EventSource eventSource = builder.build();

        // 다른 스레드에서 Producer 실행
        eventSource.start();

        // 시간초과시 InterruptedException 발생하여 메인 쓰레드를 종료시켜 전체 종료시키기 
        TimeUnit.MINUTES.sleep(10);

    }

}
