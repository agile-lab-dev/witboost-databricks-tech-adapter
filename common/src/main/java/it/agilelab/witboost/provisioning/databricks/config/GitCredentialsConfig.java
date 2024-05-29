package it.agilelab.witboost.provisioning.databricks.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "git")
public class GitCredentialsConfig {
    private String username;
    private String token;
}
