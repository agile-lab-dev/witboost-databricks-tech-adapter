package it.agilelab.witboost.provisioning.databricks.bean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.workspace.*;
import it.agilelab.witboost.provisioning.databricks.bean.params.WorkspaceClientConfigParams;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import java.util.ArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.Logger;

@ExtendWith(MockitoExtension.class)
public class DatabricksWorkspaceClientConfigTest {

    @Mock
    private Logger logger;

    @Spy
    @InjectMocks
    private DatabricksWorkspaceClientConfig databricksWorkspaceClientConfig;

    @Mock
    private WorkspaceClient workspaceClient;

    @Mock
    private GitCredentialsService gitCredentialsService;

    @Mock
    private GitCredentialsAPI gitCredentialsAPI;

    @Mock
    private CredentialInfo credentialInfo;

    @Mock
    private DatabricksConfig databricksConfig;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCreateWorkspaceClient_Success() {
        WorkspaceClientConfigParams params = mock(WorkspaceClientConfigParams.class);
        DatabricksAuthConfig databricksAuthConfig = mock(DatabricksAuthConfig.class);
        AzureAuthConfig azureAuthConfig = mock(AzureAuthConfig.class);
        GitCredentialsConfig gitCredentialsConfig = mock(GitCredentialsConfig.class);

        when(params.getDatabricksAuthConfig()).thenReturn(databricksAuthConfig);
        when(params.getAzureAuthConfig()).thenReturn(azureAuthConfig);
        when(params.getWorkspaceHost()).thenReturn("https://test.databricks.com");
        when(params.getGitCredentialsConfig()).thenReturn(gitCredentialsConfig);
        when(params.getWorkspaceName()).thenReturn("test-workspace");

        // Mock the internal methods
        doReturn(databricksConfig)
                .when(databricksWorkspaceClientConfig)
                .buildDatabricksConfig(any(DatabricksAuthConfig.class), any(AzureAuthConfig.class), any(String.class));
        doNothing()
                .when(databricksWorkspaceClientConfig)
                .setGitCredentials(any(WorkspaceClient.class), any(GitCredentialsConfig.class), any(String.class));

        WorkspaceClient result = databricksWorkspaceClientConfig.createWorkspaceClient(params);

        verify(databricksWorkspaceClientConfig, times(1))
                .buildDatabricksConfig(databricksAuthConfig, azureAuthConfig, "https://test.databricks.com");
        verify(databricksWorkspaceClientConfig, times(1))
                .setGitCredentials(any(WorkspaceClient.class), eq(gitCredentialsConfig), eq("test-workspace"));
    }

    @Test
    public void testCreateWorkspaceClient_ExceptionThrown() {
        WorkspaceClientConfigParams params = mock(WorkspaceClientConfigParams.class);
        when(params.getWorkspaceHost()).thenThrow(new RuntimeException("Test Exception"));

        assertThrows(RuntimeException.class, () -> databricksWorkspaceClientConfig.createWorkspaceClient(params));
    }

    @Test
    public void testSetGitCredentials_NewCredentials() {
        GitCredentialsConfig gitCredentialsConfig = mock(GitCredentialsConfig.class);
        when(gitCredentialsConfig.getProvider()).thenReturn("github");
        when(gitCredentialsConfig.getToken()).thenReturn("token");
        when(gitCredentialsConfig.getUsername()).thenReturn("username");
        when(workspaceClient.gitCredentials()).thenReturn(gitCredentialsAPI);
        Iterable<CredentialInfo> credentialInfos = new ArrayList<>();
        when(gitCredentialsAPI.list()).thenReturn(credentialInfos);

        databricksWorkspaceClientConfig.setGitCredentials(workspaceClient, gitCredentialsConfig, "test-workspace");

        ArgumentCaptor<CreateCredentials> captor = ArgumentCaptor.forClass(CreateCredentials.class);
        verify(gitCredentialsAPI, times(1)).create(captor.capture());
        CreateCredentials capturedCredentials = captor.getValue();

        assertEquals("github", capturedCredentials.getGitProvider());
        assertEquals("token", capturedCredentials.getPersonalAccessToken());
        assertEquals("username", capturedCredentials.getGitUsername());
    }

    @Test
    public void testSetGitCredentials_UpdateExistingCredentials() {
        GitCredentialsConfig gitCredentialsConfig = mock(GitCredentialsConfig.class);
        when(gitCredentialsConfig.getProvider()).thenReturn("github");
        when(gitCredentialsConfig.getToken()).thenReturn("token");
        when(gitCredentialsConfig.getUsername()).thenReturn("username");
        when(workspaceClient.gitCredentials()).thenReturn(gitCredentialsAPI);
        ArrayList<CredentialInfo> credentialInfos = new ArrayList<>();
        credentialInfos.add(new CredentialInfo()
                .setCredentialId(5l)
                .setGitUsername("username")
                .setGitProvider("github"));
        when(gitCredentialsAPI.list()).thenReturn(credentialInfos);
        databricksWorkspaceClientConfig.setGitCredentials(workspaceClient, gitCredentialsConfig, "test-workspace");

        ArgumentCaptor<UpdateCredentials> captor = ArgumentCaptor.forClass(UpdateCredentials.class);
        verify(gitCredentialsAPI, times(1)).update(captor.capture());
        UpdateCredentials capturedCredentials = captor.getValue();

        assertEquals(5l, capturedCredentials.getCredentialId());
        assertEquals("GITHUB", capturedCredentials.getGitProvider());
        assertEquals("token", capturedCredentials.getPersonalAccessToken());
        assertEquals("username", capturedCredentials.getGitUsername());
    }

    @Test
    public void testSetGitCredentials_ExceptionThrown() {
        GitCredentialsConfig gitCredentialsConfig = mock(GitCredentialsConfig.class);
        when(workspaceClient.gitCredentials()).thenReturn(gitCredentialsAPI);
        when(gitCredentialsAPI.list()).thenThrow(new RuntimeException("Test Exception"));

        assertThrows(
                RuntimeException.class,
                () -> databricksWorkspaceClientConfig.setGitCredentials(
                        workspaceClient, gitCredentialsConfig, "test-workspace"));
    }

    @Test
    public void testSetGitCredentials_NullCred() {
        GitCredentialsConfig gitCredentialsConfig = mock(GitCredentialsConfig.class);
        when(gitCredentialsConfig.getProvider()).thenReturn("github");
        when(gitCredentialsConfig.getToken()).thenReturn("token");
        when(gitCredentialsConfig.getUsername()).thenReturn("username");
        when(workspaceClient.gitCredentials()).thenReturn(gitCredentialsAPI);
        when(gitCredentialsAPI.list()).thenReturn(null);
        databricksWorkspaceClientConfig.setGitCredentials(workspaceClient, gitCredentialsConfig, "test-workspace");
        ArgumentCaptor<CreateCredentials> captor = ArgumentCaptor.forClass(CreateCredentials.class);
        verify(gitCredentialsAPI, times(1)).create(captor.capture());
        CreateCredentials capturedCredentials = captor.getValue();

        assertEquals("github", capturedCredentials.getGitProvider());
        assertEquals("token", capturedCredentials.getPersonalAccessToken());
        assertEquals("username", capturedCredentials.getGitUsername());
    }
}
