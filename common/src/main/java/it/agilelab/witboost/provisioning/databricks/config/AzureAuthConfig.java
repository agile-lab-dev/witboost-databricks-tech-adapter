package it.agilelab.witboost.provisioning.databricks.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "azure.auth")
public class AzureAuthConfig {

    private String clientId;

    private String tenantId;

    private String clientSecret;
}
