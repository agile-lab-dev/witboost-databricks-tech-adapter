package it.agilelab.witboost.provisioning.databricks.bean.params;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import it.agilelab.witboost.provisioning.databricks.bean.WorkspaceClientConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkspaceClientAzureConfigParamsTest {

    private DatabricksAuthConfig mockDatabricksAuthConfig;
    private AzureAuthConfig mockAzureAuthConfig;
    private GitCredentialsConfig mockGitCredentialsConfig;
    private String workspaceHost;
    private String workspaceName;

    @BeforeEach
    void setUp() {
        mockDatabricksAuthConfig = mock(DatabricksAuthConfig.class);
        mockAzureAuthConfig = mock(AzureAuthConfig.class);
        mockGitCredentialsConfig = mock(GitCredentialsConfig.class);
        workspaceHost = "https://example.databricks.com";
        workspaceName = "TestWorkspace";
    }

    @Test
    void testConstructorAndGetters() {
        WorkspaceClientConfig.WorkspaceClientConfigParams params =
                new WorkspaceClientConfig.WorkspaceClientConfigParams(
                        WorkspaceClientConfig.WorkspaceClientConfigParams.AuthType.AZURE,
                        mockDatabricksAuthConfig,
                        mockAzureAuthConfig,
                        workspaceHost,
                        workspaceName);

        assertEquals(mockDatabricksAuthConfig, params.getDatabricksAuthConfig());
        assertEquals(mockAzureAuthConfig, params.getAzureAuthConfig());
        assertEquals(workspaceHost, params.getWorkspaceHost());
        assertEquals(workspaceName, params.getWorkspaceName());
    }
}
