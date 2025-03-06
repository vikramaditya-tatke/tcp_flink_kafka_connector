package com.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataConsumerFlink {
    private static final Logger LOG = LoggerFactory.getLogger(DataConsumerFlink.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        final String hostname = params.get("hostname", "localhost");
        final int port = params.getInt("port", 9999);
        final String kafkaBrokers = params.get("kafka-brokers", "localhost:9092");
        final String kafkaTopic = params.get("kafka-topic", "windows-events");
        
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);
        env.getConfig().setGlobalJobParameters(params);
        
        // Create pipeline: socket -> JSON parsing -> Kafka
        DataStream<String> socketData = env.socketTextStream(hostname, port);
        DataStream<JsonNode> jsonData = socketData.map(new JsonParserFunction()).name("JSON Parser");
        
        // Configure and create Kafka sink
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
            .setBootstrapServers(kafkaBrokers)
            .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                .setTopic(kafkaTopic)
                .setValueSerializationSchema(new SimpleStringSchema())
                .build()
            )
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build();
        
        // Convert to string and send to Kafka
        jsonData.map(node -> node.toString()).sinkTo(kafkaSink).name("Kafka Sink");
        
        env.execute("Flink Socket to Kafka Processor");
    }
    
    public static class JsonParserFunction implements MapFunction<String, JsonNode> {
        @Override
        public JsonNode map(String json) throws Exception {
            try {
                return OBJECT_MAPPER.readTree(json);
            } catch (Exception e) {
                LOG.error("Error parsing JSON: {}", json, e);
                return OBJECT_MAPPER.createObjectNode()
                    .put("error", "Failed to parse")
                    .put("raw", json);
            }
        }
    }
}