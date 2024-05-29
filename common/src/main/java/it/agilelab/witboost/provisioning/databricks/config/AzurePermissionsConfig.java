package it.agilelab.witboost.provisioning.databricks.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "azure.permissions")
public class AzurePermissionsConfig {

    private String auth_clientId;

    private String auth_tenantId;

    private String auth_clientSecret;

    private String roleDefinitionId;

    private String subscriptionId;

    private String resourceGroup;
}
