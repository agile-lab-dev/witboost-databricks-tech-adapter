package it.agilelab.witboost.provisioning.databricks.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "azure.permissions")
public class AzurePermissionsConfig {
    private String resourceId;

    private String roleAssignmentName;

    private String roleDefinitionId;

    private String principalId;

    private String principalType;
}
