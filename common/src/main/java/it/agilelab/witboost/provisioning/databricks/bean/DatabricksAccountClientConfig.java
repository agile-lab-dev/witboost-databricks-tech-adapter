package it.agilelab.witboost.provisioning.databricks.bean;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.core.DatabricksConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatabricksAccountClientConfig {
    @Bean
    public AccountClient AccountClientBean(DatabricksAuthConfig databricksAuthConfig, AzureAuthConfig azureAuthConfig) {

        DatabricksConfig cfgAdmin = new DatabricksConfig()
                .setAuthType("azure-client-secret")
                .setHost("https://accounts.azuredatabricks.net/")
                .setAccountId(databricksAuthConfig.getAccountId())
                .setAzureTenantId(azureAuthConfig.getTenantId())
                .setAzureClientId(azureAuthConfig.getClientId())
                .setAzureClientSecret(azureAuthConfig.getClientSecret());
        AccountClient accountAdmin = new AccountClient(cfgAdmin);

        return accountAdmin;
    }
}
