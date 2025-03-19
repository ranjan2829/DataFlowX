// FlinkJob.java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import com.datastax.driver.core.Cluster;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class FlinkJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("ecommerce-stream", new SimpleStringSchema(), props());
        var stream = env.addSource(consumer);

        ObjectMapper mapper = new ObjectMapper();
        stream.map(value -> {
            JsonNode node = mapper.readTree(value);
            return new StreamData(node.get("user_id").asText(), node.get("price").asInt());
        }).addSink(CassandraSink.addSink(stream)
            .setQuery("INSERT INTO ecommerce.stream_data (user_id, price) VALUES (?, ?);")
            .setClusterBuilder(() -> Cluster.builder().addContactPoint("cassandra").build())
            .build());

        env.execute("Flink Kafka to Cassandra");
    }

    private static java.util.Properties props() {
        java.util.Properties p = new java.util.Properties();
        p.setProperty("bootstrap.servers", "localhost:9092");
        return p;
    }
}

class StreamData {
    public String user_id;
    public int price;
    public StreamData(String user_id, int price) { this.user_id = user_id; this.price = price; }
}