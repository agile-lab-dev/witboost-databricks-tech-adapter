package it.agilelab.witboost.provisioning.databricks.bean;

import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

public class DatabricksApiClientBean implements FactoryBean<ApiClient> {

    private DatabricksConfig databricksConfig;
    private String workspaceHost;
    private final DatabricksAuthConfig databricksAuthConfig;
    private final AzureAuthConfig azureAuthConfig;
    private ApiClient apiClient;

    private final Logger logger = LoggerFactory.getLogger(DatabricksApiClientBean.class);

    public DatabricksApiClientBean(
            String workspaceHost, DatabricksAuthConfig databricksAuthConfig, AzureAuthConfig azureAuthConfig) {
        this.workspaceHost = workspaceHost;
        this.databricksAuthConfig = databricksAuthConfig;
        this.azureAuthConfig = azureAuthConfig;
    }

    @Override
    public ApiClient getObject() {
        setApiClient(initializeApiClient());
        return apiClient;
    }

    public ApiClient getObject(String workspaceHost) {
        setWorkspaceHost(workspaceHost);
        setApiClient(initializeApiClient());

        return apiClient;
    }

    @Override
    public Class<?> getObjectType() {
        return ApiClient.class;
    }

    private DatabricksConfig setDatabricksConfig(String workspaceHost) {
        return new DatabricksConfig()
                .setHost(workspaceHost)
                .setAccountId(databricksAuthConfig.getAccountId())
                .setAzureTenantId(azureAuthConfig.getTenantId())
                .setAzureClientId(azureAuthConfig.getClientId())
                .setAzureClientSecret(azureAuthConfig.getClientSecret());
    }

    private ApiClient initializeApiClient() {
        try {
            DatabricksConfig config = setDatabricksConfig(workspaceHost);

            ApiClient apiClient = new ApiClient(config);

            return apiClient;

        } catch (Exception e) {
            logger.error("Error initializing the ApiClient: {}", e.getMessage());
            throw new RuntimeException("Unable to initialize ApiClient", e);
        }
    }

    public void setApiClient(ApiClient apiClient) {
        this.apiClient = apiClient;
    }

    public void setWorkspaceHost(String workspaceHost) {
        this.workspaceHost = workspaceHost;
    }
}
