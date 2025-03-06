package com.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Flink application that consumes data from a TCP socket.
 * This application connects to a DataProducer running on a specified host and port,
 * which generates synthetic Windows event log entries in NDJSON format.
 */
public class DataConsumerFlink {
    private static final Logger LOG = LoggerFactory.getLogger(DataConsumerFlink.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    
    public static void main(String[] args) throws Exception {
        // Parse command line parameters
        final ParameterTool params = ParameterTool.fromArgs(args);
        
        // Host where DataProducer is running
        // Default depends on deployment scenario:
        // - When run locally (outside docker): use "localhost"
        // - When run in Docker container: use "host.docker.internal" to access host network
        final String hostname = params.get("hostname", "localhost");
        final int port = params.getInt("port", 9999);
        
        // Set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Enable checkpointing for fault tolerance
        env.enableCheckpointing(5000);
        
        // Set parallelism to 1 for simpler debugging
        env.setParallelism(1);
        
        // Make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);
        
        LOG.info("Connecting to socket at {}:{}", hostname, port);
        
        // Create a DataStream from the socket
        DataStream<String> rawEvents = env.socketTextStream(hostname, port);
        
        // Parse JSON events
        DataStream<JsonNode> jsonEvents = rawEvents
            .map(new JsonParserFunction())
            .name("JSON Parser");
        
        // Extract event info and print
        jsonEvents
            .map(new EventInfoExtractor())
            .name("Event Info Extractor")
            .print()
            .name("Print Raw Events");
            
        // Filter for security events and print count per 10 seconds
        jsonEvents
            .filter(new SecurityEventFilter())
            .map(event -> Tuple2.of("Security Event", 1), Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(tuple -> tuple.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .sum(1)
            .name("Count Security Events")
            .print()
            .name("Print Security Event Counts");
        
        // Group events by source and count them per 10 seconds
        jsonEvents
            .map(event -> {
                String source = event.get("source").asText();
                return Tuple2.of(source, 1);
            }, Types.TUPLE(Types.STRING, Types.INT))
            .keyBy(tuple -> tuple.f0)
            .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
            .sum(1)
            .name("Count Events By Source")
            .print()
            .name("Print Source Counts");
        
        // Execute program
        env.execute("Flink Socket Data Consumer");
    }
    
    /**
     * Function to parse JSON from raw event strings
     */
    public static class JsonParserFunction implements MapFunction<String, JsonNode> {
        @Override
        public JsonNode map(String json) throws Exception {
            try {
                return OBJECT_MAPPER.readTree(json);
            } catch (Exception e) {
                LOG.error("Error parsing JSON: {}", json, e);
                // Return a simple JsonNode with error info to avoid breaking the stream
                return OBJECT_MAPPER.createObjectNode()
                    .put("error", "Failed to parse")
                    .put("raw", json);
            }
        }
    }
    
    /**
     * Function to extract relevant event information
     */
    public static class EventInfoExtractor implements MapFunction<JsonNode, Tuple4<Integer, String, String, String>> {
        @Override
        public Tuple4<Integer, String, String, String> map(JsonNode event) throws Exception {
            int eventId = event.has("eventId") ? event.get("eventId").asInt() : -1;
            String level = event.has("level") ? event.get("level").asText() : "Unknown";
            String source = event.has("source") ? event.get("source").asText() : "Unknown";
            String message = event.has("message") ? event.get("message").asText() : "No message";
            
            return Tuple4.of(eventId, level, source, message);
        }
    }
    
    /**
     * Filter to extract only security events
     */
    public static class SecurityEventFilter implements FilterFunction<JsonNode> {
        @Override
        public boolean filter(JsonNode event) throws Exception {
            return event.has("source") &&
                   "Security".equals(event.get("source").asText());
        }
    }
}