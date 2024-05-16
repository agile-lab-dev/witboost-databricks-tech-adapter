package it.agilelab.witboost.provisioning.databricks.principalsmapping.azure;

import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import io.vavr.control.Try;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;

public class AzureClientFactory {

    /**
     * Build a concrete instance of an AzureClient
     * @param config config for the service principal
     * @return a Try containing an AzureGraphClient or an error
     */
    public static Try<AzureClient> getClient(AzureAuthConfig azureAuthConfig) {
        return Try.of(() -> {
            String clientId = azureAuthConfig.getClientId();
            String tenantId = azureAuthConfig.getTenantId();
            String clientSecret = azureAuthConfig.getClientSecret();

            String[] scopes = new String[] {"https://graph.microsoft.com/.default"};

            ClientSecretCredential credential = new ClientSecretCredentialBuilder()
                    .clientId(clientId)
                    .tenantId(tenantId)
                    .clientSecret(clientSecret)
                    .build();

            GraphServiceClient graphServiceClient = new GraphServiceClient(credential, scopes);

            return new AzureGraphClient(graphServiceClient);
        });
    }
}
