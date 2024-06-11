package it.agilelab.witboost.provisioning.databricks.bean;

import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatabricksWorkspaceClientConfig {

    @Bean
    public DatabricksWorkspaceClientBean databricksWorkspaceClientBean(
            DatabricksAuthConfig databricksAuthConfig,
            AzureAuthConfig azureAuthConfig,
            GitCredentialsConfig gitCredentialsConfig) {
        return new DatabricksWorkspaceClientBean(
                null,
                null,
                databricksAuthConfig,
                azureAuthConfig,
                gitCredentialsConfig); // workspaceHost will be set later
    }
}
