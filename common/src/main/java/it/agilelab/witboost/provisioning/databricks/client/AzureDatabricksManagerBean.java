package it.agilelab.witboost.provisioning.databricks.client;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import org.springframework.beans.factory.FactoryBean;

/**
 * Factory bean for creating AzureDatabricksManager instances.
 * This bean authenticates with Azure using default credentials and provides access to Azure Databricks resources.
 */
public class AzureDatabricksManagerBean implements FactoryBean<AzureDatabricksManager> {

    private AzureDatabricksManager azureDatabricksManager;

    @Override
    public AzureDatabricksManager getObject() throws IllegalArgumentException {

        if (azureDatabricksManager == null) {
            AzureProfile profile = new AzureProfile(AzureEnvironment.AZURE);
            TokenCredential credential = new DefaultAzureCredentialBuilder()
                    .authorityHost(profile.getEnvironment().getActiveDirectoryEndpoint())
                    .build();

            azureDatabricksManager = AzureDatabricksManager.authenticate(credential, profile);
        }
        return azureDatabricksManager;
    }

    @Override
    public Class<?> getObjectType() {
        return AzureDatabricksManager.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    public void setAzureDatabricksManager(AzureDatabricksManager azureDatabricksManager) {
        this.azureDatabricksManager = azureDatabricksManager;
    }
}
