package it.agilelab.witboost.provisioning.databricks.bean.azure;

import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureClient;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AzureMapperConfig {

    @Autowired
    AzureClient azureClient;

    @Bean
    public AzureMapper azureMapper() {
        return new AzureMapper(azureClient);
    }
}
