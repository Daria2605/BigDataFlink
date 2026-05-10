package ru.bigdata.flink;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

public class SalesStarJob {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "sales_raw");
        String groupId = System.getenv().getOrDefault("KAFKA_GROUP_ID", "flink-star-schema-job");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10_000L);
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<SalesRecord> records = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka sales_raw source")
                .map(json -> {
                    Map<String, Object> row = MAPPER.readValue(json, new TypeReference<>() {});
                    return SalesRecord.fromMap(row);
                })
                .name("JSON to star-schema record");

        records.addSink(new SalesStarSink()).name("PostgreSQL star schema sink");
        env.execute("BigDataFlink CSV Kafka to PostgreSQL star schema");
    }
}
