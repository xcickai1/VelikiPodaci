package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerKafkaCSV {

    private final static Logger log = LoggerFactory.getLogger(Consumer.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Kafka Consumer");

        String groupId = "my-application";
        String topic = "wikimedia.recentchange";

        // Create Consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe Consumer to topic
        consumer.subscribe(Arrays.asList(topic));

        // Poll new data
        while (true) {
            log.info("Polling...");

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value:" + record.value() + "\n" +
                        "Partition: " + record.partition() + ", Offset: " + record.offset() + "\n");

                // Write record to file (CSV or Parquet)
                try {
                    writeToCSV(record.value());
                } catch (IOException e) {
                    log.error("Error writing to file", e);
                }
            }
        }
    }

    private static void writeToCSV(String data) throws IOException {

        String filePath = "C:\\Users\\prcki\\Desktop\\RAF\\VelikiPodaciProjekat\\data.csv";

        String csvData = data + "\n";

        // Write to the file
        Files.writeString(Paths.get(filePath), csvData, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}
