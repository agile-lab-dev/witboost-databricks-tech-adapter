package it.agilelab.witboost.provisioning.databricks.bean;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration class for Azure Databricks manager bean.
 */
@Configuration
public class AzureDatabricksManagerConfig {

    @Bean
    public AzureDatabricksManager azureDatabricksManager() {
        AzureProfile profile = new AzureProfile(AzureEnvironment.AZURE);
        TokenCredential credential = new DefaultAzureCredentialBuilder()
                .authorityHost(profile.getEnvironment().getActiveDirectoryEndpoint())
                .build();

        return AzureDatabricksManager.authenticate(credential, profile);
    }
}
