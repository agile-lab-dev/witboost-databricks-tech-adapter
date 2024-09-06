package it.agilelab.witboost.provisioning.databricks.bean.params;

import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import lombok.Getter;

@Getter
public class WorkspaceClientConfigParams {
    private DatabricksAuthConfig databricksAuthConfig;
    private AzureAuthConfig azureAuthConfig;
    private GitCredentialsConfig gitCredentialsConfig;
    private String workspaceHost;
    private String workspaceName;

    public WorkspaceClientConfigParams(
            DatabricksAuthConfig databricksAuthConfig,
            AzureAuthConfig azureAuthConfig,
            GitCredentialsConfig gitCredentialsConfig,
            String workspaceHost,
            String workspaceName) {
        this.databricksAuthConfig = databricksAuthConfig;
        this.azureAuthConfig = azureAuthConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
        this.workspaceHost = workspaceHost;
        this.workspaceName = workspaceName;
    }
}
