package it.agilelab.witboost.provisioning.databricks.bean;

import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatabricksTablesAPIConfig {

    @Bean
    public DatabricksTableAPIBean databricksTableAPIBean(
            DatabricksAuthConfig databricksAuthConfig, AzureAuthConfig azureAuthConfig) {
        return new DatabricksTableAPIBean(
                null, databricksAuthConfig, azureAuthConfig); // workspaceHost will be set later
    }
}
