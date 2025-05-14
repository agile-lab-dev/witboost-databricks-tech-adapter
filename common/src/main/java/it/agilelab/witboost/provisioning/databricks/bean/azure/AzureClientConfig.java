package it.agilelab.witboost.provisioning.databricks.bean.azure;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureClient;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureGraphClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(AzurePermissionsConfig.class)
public class AzureClientConfig {

    @Autowired
    AzurePermissionsConfig azurePermissionsConfig;

    @Bean
    public AzureClient azureClient() {
        String clientId = azurePermissionsConfig.getAuth_clientId();
        String tenantId = azurePermissionsConfig.getAuth_tenantId();
        String clientSecret = azurePermissionsConfig.getAuth_clientSecret();

        String[] scopes = new String[] {"https://graph.microsoft.com/.default"};

        ClientSecretCredential credential = new ClientSecretCredentialBuilder()
                .clientId(clientId)
                .tenantId(tenantId)
                .clientSecret(clientSecret)
                .build();

        GraphServiceClient graphServiceClient = new GraphServiceClient(credential, scopes);

        return new AzureGraphClient(graphServiceClient);
    }
}
