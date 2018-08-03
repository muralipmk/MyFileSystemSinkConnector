package org.murali.producer;


import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.my.fun.FileData;
import org.my.fun.MetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Properties;
import java.util.Scanner;

public class Kafka_Avro_Producer {
    private static Logger LOG = LoggerFactory.getLogger(Kafka_Avro_Producer.class);

    private final static String APP_ID = "my-avro-file-agent";
    private final static String TOPIC = "my-file-avro-topic2";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
    private final static String SCHEMA_REGISTRY_URL = "http://localhost:8081";

    private static Producer<String, FileData> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, APP_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

        return new KafkaProducer<>(props);
    }

    static void runProducer() throws InterruptedException {

        final Producer<String, FileData> producer = createProducer();
        long time = System.currentTimeMillis();

        Scanner scanner= new Scanner(System.in);

        int i= 0;

        while(true) {
            System.out.print("Enter directory path: ");
            String filePath = scanner.nextLine();

            File file = new File(filePath);
            if (!file.isDirectory()) {
                throw new ConfigException("Not a directory. Please provide a full path to target folder.");
            }

            Collection<File> files = FileUtils.listFiles(file, null, true);

            for(File f: files) {
                FileData fileData = FileData.newBuilder()
                        .setAbsolutePath(f.getAbsolutePath())
                        .setFileName(f.getName())
                        .setMetaData((MetaData.newBuilder().setSize(f.length()).build()))
                        .build();
                final ProducerRecord<String, FileData> record = new ProducerRecord<>(TOPIC, fileData.getAbsolutePath(), fileData);
                producer.send(record);
                producer.flush();
            }
        }

    }

    public static void main(String[] args) {
        try{
            runProducer();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
