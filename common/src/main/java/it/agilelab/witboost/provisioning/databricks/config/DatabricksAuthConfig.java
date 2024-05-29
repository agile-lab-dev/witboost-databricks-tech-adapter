package it.agilelab.witboost.provisioning.databricks.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Data
@Component
@ConfigurationProperties(prefix = "databricks.auth")
public class DatabricksAuthConfig {
    private String accountId;
}
