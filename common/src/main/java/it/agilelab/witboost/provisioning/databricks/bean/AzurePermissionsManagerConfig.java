package it.agilelab.witboost.provisioning.databricks.bean;

import com.azure.resourcemanager.AzureResourceManager;
import it.agilelab.witboost.provisioning.databricks.permissions.AzurePermissionsManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AzurePermissionsManagerConfig {

    @Autowired
    private AzureResourceManager azureResourceManager;

    @Bean
    public AzurePermissionsManager azurePermissionsManager() {

        return new AzurePermissionsManager(azureResourceManager);
    }
}
