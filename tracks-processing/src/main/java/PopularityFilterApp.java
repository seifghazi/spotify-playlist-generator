import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.ConfigFactory;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class PopularityFilterApp {
    private static List<String> trackIDs = new ArrayList<String>();
    private static Logger log = LoggerFactory.getLogger(PopularityFilterApp.class.getName());
    private AppConfig appConfig;

    public static void main(String[] args) {
        PopularityFilterApp popularFilterApp = new PopularityFilterApp();
        popularFilterApp.start();
    }

    private PopularityFilterApp() {
        appConfig = new AppConfig(ConfigFactory.load());
    }

    private void start() {
        Properties config = getKafkaStreamsConfig();
        KafkaStreams streams = createTopology(config);
        streams.cleanUp();
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Properties getKafkaStreamsConfig() {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        return config;
    }

    private KafkaStreams createTopology(Properties config) {
        StreamsBuilder builder = new StreamsBuilder();

        // JSON Serde
        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
        KStream<String, JsonNode> playlistTracks = builder.stream(appConfig.getSourceTopicName(),
                Consumed.with(Serdes.String(), jsonSerde));

        KStream<String, JsonNode>[] branches = playlistTracks.branch(
                (trackID, trackData) -> isPopular(trackData),
                (trackID, trackData) -> true
        );

        KStream<String, JsonNode> popularTracks = branches[0];
        KStream<String, JsonNode> lessPopularTracks = branches[1];

        popularTracks.peek((trackID, trackData) -> log.info("Popular Track Name: " + trackData.get("name")))
                 .filter((trackID, trackData) -> isDuplicate(trackID))
                .to(appConfig.getPopularTrackTopicName(), Produced.with(Serdes.String(), jsonSerde));
        lessPopularTracks.peek((trackID, trackData) -> log.info("Less Popular Track Name " + trackData.get("name")))
                .filter((trackID, trackData) -> isDuplicate(trackID))
                .to(appConfig.getLessPopularTrackTopicName(), Produced.with(Serdes.String(), jsonSerde));

        playlistTracks.selectKey((trackID, trackData) -> extractArtists(trackData))
                .groupByKey(Serialized.with(Serdes.String(), jsonSerde))
                .count()
                .toStream()
                .through("artist-count", Produced.with(Serdes.String(), Serdes.Long()))
                .filter((artistID, count) -> count > 2)
                .to("popular-artists", Produced.with(Serdes.String(), Serdes.Long()));

        return new KafkaStreams(builder.build(), config);
    }

    private static boolean isPopular(JsonNode playlistTracks) {
        int popularity = playlistTracks.get("popularity").asInt();
        return popularity > 80;
    }

    private static boolean isDuplicate(String trackID) {
        if (trackIDs.contains(trackID)) {
            return true;
        }
        trackIDs.add(trackID);
        return false;
    }

    private static String extractArtists(JsonNode trackData) {
        List<String> artists = new ArrayList<>();
        // add all artists featured on track
        JsonNode trackArtists = trackData.get("artists");
        for (final JsonNode objNode : trackArtists) {
            System.out.println(objNode.get("name"));
            artists.add(objNode.get("name").asText());
        }

        return String.join("-", artists);
    }
}
