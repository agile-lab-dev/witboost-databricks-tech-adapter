package it.agilelab.witboost.provisioning.databricks.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "databricks.permissions")
public class DatabricksPermissionsConfig {

    private Workload workload;
    private OutputPort outputPort;

    @Getter
    @Setter
    public static class Workload {
        private String owner;
        private String developer;
    }

    @Getter
    @Setter
    public static class OutputPort {
        private String owner;
        private String developer;
    }
}
