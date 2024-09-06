package it.agilelab.witboost.provisioning.databricks.bean;

import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import it.agilelab.witboost.provisioning.databricks.bean.params.ApiClientConfigParams;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DatabricksApiClientConfig {

    private static final Logger logger = LoggerFactory.getLogger(DatabricksApiClientConfig.class);

    @Bean
    public Function<ApiClientConfigParams, ApiClient> apiClientFactory() {
        return arg -> createApiClient(arg);
    }

    @Bean
    @Scope(value = "prototype")
    public ApiClient createApiClient(ApiClientConfigParams apiClientConfigParams) {

        try {
            DatabricksConfig config = buildDatabricksConfig(
                    apiClientConfigParams.getDatabricksAuthConfig(),
                    apiClientConfigParams.getAzureAuthConfig(),
                    apiClientConfigParams.getWorkspaceHost());

            ApiClient apiClient = new ApiClient(config);

            return apiClient;

        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error initializing the apiClient for %s. Please try again and if the error persists contact the platform team. Details: %s",
                    apiClientConfigParams.getWorkspaceName(), e.getMessage());
            logger.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    protected DatabricksConfig buildDatabricksConfig(
            DatabricksAuthConfig databricksAuthConfig, AzureAuthConfig azureAuthConfig, String workspaceHost) {
        return new DatabricksConfig()
                .setHost(workspaceHost)
                .setAccountId(databricksAuthConfig.getAccountId())
                .setAzureTenantId(azureAuthConfig.getTenantId())
                .setAzureClientId(azureAuthConfig.getClientId())
                .setAzureClientSecret(azureAuthConfig.getClientSecret());
    }
}
