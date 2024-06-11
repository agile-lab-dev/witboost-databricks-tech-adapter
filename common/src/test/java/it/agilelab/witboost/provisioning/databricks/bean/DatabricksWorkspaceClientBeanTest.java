package it.agilelab.witboost.provisioning.databricks.bean;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.workspace.CreateCredentials;
import com.databricks.sdk.service.workspace.GitCredentialsAPI;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class DatabricksWorkspaceClientBeanTest {

    private DatabricksWorkspaceClientBean clientBean;
    private WorkspaceClient mockedClient;

    @BeforeEach
    public void setUp() {
        // Mocking dependencies
        String workspaceHost = "https://example.databricks.com";
        String workspaceName = "example";
        DatabricksAuthConfig databricksAuthConfig = mock(DatabricksAuthConfig.class);
        AzureAuthConfig azureAuthConfig = mock(AzureAuthConfig.class);
        GitCredentialsConfig gitCredentialsConfig = mock(GitCredentialsConfig.class);

        // Mocking WorkspaceClient
        mockedClient = mock(WorkspaceClient.class);

        // Mocking GitCredentialsAPI
        GitCredentialsAPI mockedGitCredentialsAPI = mock(GitCredentialsAPI.class);
        when(mockedClient.gitCredentials()).thenReturn(mockedGitCredentialsAPI);

        // Creating instance of DatabricksWorkspaceClientBean
        clientBean = new DatabricksWorkspaceClientBean(
                workspaceHost, workspaceName, databricksAuthConfig, azureAuthConfig, gitCredentialsConfig);
        clientBean.setWorkspaceClient(mockedClient);
    }

    @Test
    public void testGetObjectType() {
        // Verify that getObjectType() returns WorkspaceClient.class
        assertEquals(WorkspaceClient.class, clientBean.getObjectType());
    }

    @Test
    public void testIsSingleton() {
        // Verify that isSingleton() returns true
        assertTrue(clientBean.isSingleton());
    }

    @Test
    public void testSetGitCredentials() {
        // Setup
        String personalAccessToken = "myAccessToken";
        String gitUsername = "myUsername";
        GitProvider gitProvider = GitProvider.GITLAB;

        // Test
        clientBean.setGitCredentials(personalAccessToken, gitUsername, gitProvider);

        // Verification
        verify(mockedClient.gitCredentials(), times(1)).list(); // Verify list() method call
        verify(mockedClient.gitCredentials(), times(1))
                .create(any(
                        CreateCredentials.class)); // Verify create() method call with any CreateCredentials argument
    }
}
