package it.agilelab.witboost.provisioning.databricks.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "databricks.auth")
public class DatabricksAuthConfig {
    private String accountId;
}
