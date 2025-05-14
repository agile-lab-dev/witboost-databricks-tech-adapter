package it.agilelab.witboost.provisioning.databricks.client;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class WorkspaceLevelManagerFactory {

    private final Logger logger = LoggerFactory.getLogger(WorkspaceLevelManagerFactory.class);
    private final AccountClient accountClient;
    private final DatabricksAuthConfig databricksAuthConfig;
    private final AzureAuthConfig azureAuthConfig;

    public WorkspaceLevelManagerFactory(
            AccountClient accountClient, DatabricksAuthConfig databricksAuthConfig, AzureAuthConfig azureAuthConfig) {
        this.accountClient = accountClient;
        this.databricksAuthConfig = databricksAuthConfig;
        this.azureAuthConfig = azureAuthConfig;
    }

    public WorkspaceLevelManager createDatabricksWorkspaceLevelManager(WorkspaceClient workspaceClient) {
        return new WorkspaceLevelManager(workspaceClient, accountClient);
    }
}
