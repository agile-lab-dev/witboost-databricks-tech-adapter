package it.agilelab.witboost.provisioning.databricks.bean;

import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatabricksApiClientConfig {

    @Bean
    public DatabricksApiClientBean databricksApiClientBean(
            DatabricksAuthConfig databricksAuthConfig, AzureAuthConfig azureAuthConfig) {
        return new DatabricksApiClientBean(
                null, databricksAuthConfig, azureAuthConfig); // workspaceHost will be set later
    }
}
