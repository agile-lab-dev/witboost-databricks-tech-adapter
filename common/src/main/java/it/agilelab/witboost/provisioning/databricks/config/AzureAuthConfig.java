package it.agilelab.witboost.provisioning.databricks.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "azure.auth")
public class AzureAuthConfig {

    private String clientId;

    private String tenantId;

    private String clientSecret;

    private String skuType;
}
