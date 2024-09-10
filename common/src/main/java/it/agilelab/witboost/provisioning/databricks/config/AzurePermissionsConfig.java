package it.agilelab.witboost.provisioning.databricks.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "azure.permissions")
public class AzurePermissionsConfig {

    private String auth_clientId;

    private String auth_tenantId;

    private String auth_clientSecret;

    private String subscriptionId;

    private String resourceGroup;

    private String dpOwnerRoleDefinitionId;

    private String devGroupRoleDefinitionId;
}
