import com.typesafe.config.Config;

public class AppConfig {
    private final String bootstrapServers;
    private final String sourceTopicName;
    private final String applicationId;
    private final String popularTopicName;
    private final String lessPopularTopicName;

    public AppConfig(Config config) {
        this.bootstrapServers = config.getString("kafka.bootstrap.servers");
        this.sourceTopicName = config.getString("kafka.source.topic.name");
        this.popularTopicName = config.getString("kafka.popular.topic.name");
        this.lessPopularTopicName = config.getString("kafka.less-popular.topic.name");
        this.applicationId = config.getString("kafka.streams.application.id");
    }


    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getSourceTopicName() {
        return sourceTopicName;
    }

    public String getPopularTrackTopicName() {
        return popularTopicName;
    }

    public String getFraudTopicName() {
        return lessPopularTopicName;
    }

    public String getApplicationId() {
        return applicationId;
    }

}
