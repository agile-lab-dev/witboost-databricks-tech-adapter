package it.agilelab.witboost.provisioning.databricks.bean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WorkspaceClientConfigTest {

    @InjectMocks
    private WorkspaceClientConfig workspaceClientConfig;

    @Test
    public void testCreateWorkspaceClient_Success() {
        WorkspaceClientConfig configSpy = Mockito.spy(new WorkspaceClientConfig());

        WorkspaceClientConfig.WorkspaceClientConfigParams params =
                mock(WorkspaceClientConfig.WorkspaceClientConfigParams.class);
        DatabricksAuthConfig databricksAuthConfig = mock(DatabricksAuthConfig.class);
        AzureAuthConfig azureAuthConfig = mock(AzureAuthConfig.class);

        when(params.getAuthType()).thenReturn(WorkspaceClientConfig.WorkspaceClientConfigParams.AuthType.AZURE);
        when(params.getDatabricksAuthConfig()).thenReturn(databricksAuthConfig);
        when(params.getAzureAuthConfig()).thenReturn(azureAuthConfig);
        when(params.getWorkspaceHost()).thenReturn("https://fake.host");
        when(databricksAuthConfig.getAccountId()).thenReturn("test-account");
        when(azureAuthConfig.getTenantId()).thenReturn("test-tenant");
        when(azureAuthConfig.getClientId()).thenReturn("test-client");
        when(azureAuthConfig.getClientSecret()).thenReturn("test-secret");
        WorkspaceClient workspaceClient = configSpy.createWorkspaceClient(params);

        assert (workspaceClient != null);
        assertEquals("https://fake.host", workspaceClient.config().getHost());
        verify(configSpy, times(1)).buildAzureDatabricksConfig(any());
    }

    @Test
    public void testCreateWorkspaceClient_OAuthSuccess() {
        WorkspaceClientConfig configSpy = Mockito.spy(new WorkspaceClientConfig());

        WorkspaceClientConfig.WorkspaceClientConfigParams params =
                mock(WorkspaceClientConfig.WorkspaceClientConfigParams.class);

        when(params.getAuthType()).thenReturn(WorkspaceClientConfig.WorkspaceClientConfigParams.AuthType.OAUTH);
        when(params.getDatabricksClientID()).thenReturn("test-client-id");
        when(params.getDatabricksClientSecret()).thenReturn("test-client-secret");
        when(params.getWorkspaceHost()).thenReturn("https://fake.oauth.host");

        WorkspaceClient workspaceClient = configSpy.createWorkspaceClient(params);

        assert (workspaceClient != null);
        assertEquals("https://fake.oauth.host", workspaceClient.config().getHost());
        verify(configSpy, times(1)).buildOAuthDatabricksConfig(any());
    }

    @Test
    public void testCreateWorkspaceClient_ExceptionThrown() {
        WorkspaceClientConfig.WorkspaceClientConfigParams params =
                mock(WorkspaceClientConfig.WorkspaceClientConfigParams.class);
        when(params.getAuthType()).thenReturn(WorkspaceClientConfig.WorkspaceClientConfigParams.AuthType.AZURE);
        when(params.getWorkspaceHost()).thenThrow(new RuntimeException("Test Exception"));

        assertThrows(RuntimeException.class, () -> workspaceClientConfig.createWorkspaceClient(params));
    }
}
