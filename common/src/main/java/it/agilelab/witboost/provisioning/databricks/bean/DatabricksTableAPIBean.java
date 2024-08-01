package it.agilelab.witboost.provisioning.databricks.bean;

import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.catalog.TablesAPI;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

public class DatabricksTableAPIBean implements FactoryBean<TablesAPI> {

    private DatabricksConfig databricksConfig;
    private String workspaceHost;
    private final DatabricksAuthConfig databricksAuthConfig;
    private final AzureAuthConfig azureAuthConfig;
    private TablesAPI tablesAPI;

    private final Logger logger = LoggerFactory.getLogger(DatabricksTableAPIBean.class);

    public DatabricksTableAPIBean(
            String workspaceHost, DatabricksAuthConfig databricksAuthConfig, AzureAuthConfig azureAuthConfig) {
        this.workspaceHost = workspaceHost;
        this.databricksAuthConfig = databricksAuthConfig;
        this.azureAuthConfig = azureAuthConfig;
    }

    @Override
    public TablesAPI getObject() {
        setTablesAPI(initializeTablesAPI());
        return tablesAPI;
    }

    public TablesAPI getObject(String workspaceHost) {
        setWorkspaceHost(workspaceHost);
        setTablesAPI(initializeTablesAPI());

        return tablesAPI;
    }

    @Override
    public Class<?> getObjectType() {
        return TablesAPI.class;
    }

    public DatabricksConfig setDatabricksConfig(String workspaceHost) {
        return new DatabricksConfig()
                .setHost(workspaceHost)
                .setAccountId(databricksAuthConfig.getAccountId())
                .setAzureTenantId(azureAuthConfig.getTenantId())
                .setAzureClientId(azureAuthConfig.getClientId())
                .setAzureClientSecret(azureAuthConfig.getClientSecret());
    }

    private TablesAPI initializeTablesAPI() {
        try {
            DatabricksConfig config = setDatabricksConfig(workspaceHost);

            ApiClient apiClient = new ApiClient(config);

            return new TablesAPI(apiClient);

        } catch (Exception e) {
            logger.error("Error initializing the TablesAPI", e);
            throw new RuntimeException("Unable to initialize TablesAPI", e);
        }
    }

    public void setTablesAPI(TablesAPI tablesAPI) {
        this.tablesAPI = tablesAPI;
    }

    public void setWorkspaceHost(String workspaceHost) {
        this.workspaceHost = workspaceHost;
    }
}
