package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.mixin.ClustersExt;
import com.databricks.sdk.service.compute.ClusterDetails;
import com.databricks.sdk.service.compute.ListClustersRequest;
import com.databricks.sdk.service.iam.ListServicePrincipalsRequest;
import com.databricks.sdk.service.iam.ServicePrincipal;
import com.databricks.sdk.service.iam.ServicePrincipalsAPI;
import com.databricks.sdk.service.oauth2.CreateServicePrincipalSecretRequest;
import com.databricks.sdk.service.oauth2.CreateServicePrincipalSecretResponse;
import com.databricks.sdk.service.oauth2.DeleteServicePrincipalSecretRequest;
import com.databricks.sdk.service.oauth2.ServicePrincipalSecretsAPI;
import com.databricks.sdk.service.provisioning.Workspace;
import com.databricks.sdk.service.provisioning.WorkspacesAPI;
import com.databricks.sdk.service.sql.DataSource;
import com.databricks.sdk.service.sql.DataSourcesAPI;
import com.databricks.sdk.service.workspace.CredentialInfo;
import com.databricks.sdk.service.workspace.GitCredentialsAPI;
import com.databricks.sdk.service.workspace.UpdateCredentialsRequest;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class WorkspaceLevelManagerTest {

    @Mock
    private WorkspaceClient workspaceClient;

    @Mock
    private AccountClient accountClient;

    private WorkspaceLevelManager workspaceLevelManager;

    @Mock
    private WorkspacesAPI workspacesAPI;

    @Mock
    private ServicePrincipalsAPI servicePrincipalsAPI;

    @BeforeEach
    void setUp() {
        workspaceClient = mock(WorkspaceClient.class);
        accountClient = mock(AccountClient.class);
        workspaceLevelManager = new WorkspaceLevelManager(workspaceClient, accountClient);
    }

    @Test
    void testGetServicePrincipalFromName_PrincipalFound() {
        Workspace workspace =
                new Workspace().setDeploymentName("deploymentName").setWorkspaceName("TestWorkspace");
        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://deploymentName.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(workspacesAPI);
        when(workspacesAPI.list()).thenReturn(Collections.singletonList(workspace));

        ServicePrincipal principal =
                new ServicePrincipal().setDisplayName("TestPrincipal").setApplicationId("appId");

        when(workspaceClient.servicePrincipals()).thenReturn(servicePrincipalsAPI);
        when(servicePrincipalsAPI.list(any(ListServicePrincipalsRequest.class)))
                .thenReturn(Collections.singletonList(principal));

        Either<FailedOperation, ServicePrincipal> result =
                workspaceLevelManager.getServicePrincipalFromName("TestPrincipal");

        assertTrue(result.isRight());
        assertEquals("appId", result.get().getApplicationId());
        assertEquals("TestPrincipal", result.get().getDisplayName());
    }

    @Test
    void testGetServicePrincipalFromName_NoServicePrincipalsAvailable() {
        Workspace workspace =
                new Workspace().setDeploymentName("deploymentName").setWorkspaceName("TestWorkspace");
        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://deploymentName.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(workspacesAPI);
        when(workspacesAPI.list()).thenReturn(Collections.singletonList(workspace));

        when(workspaceClient.servicePrincipals()).thenReturn(servicePrincipalsAPI);
        when(servicePrincipalsAPI.list(any(ListServicePrincipalsRequest.class))).thenReturn(Collections.emptyList());

        Either<FailedOperation, ServicePrincipal> result =
                workspaceLevelManager.getServicePrincipalFromName("NonexistentPrincipal");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains("No service principals found"));
    }

    @Test
    void testGetServicePrincipalFromName_PrincipalNotFound() {
        Workspace workspace =
                new Workspace().setDeploymentName("deploymentName").setWorkspaceName("TestWorkspace");
        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://deploymentName.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(workspacesAPI);
        when(workspacesAPI.list()).thenReturn(Collections.singletonList(workspace));

        ServicePrincipal principal =
                new ServicePrincipal().setDisplayName("DifferentPrincipal").setApplicationId("appId");

        when(workspaceClient.servicePrincipals()).thenReturn(servicePrincipalsAPI);
        when(servicePrincipalsAPI.list(any(ListServicePrincipalsRequest.class)))
                .thenReturn(Collections.singletonList(principal));

        Either<FailedOperation, ServicePrincipal> result =
                workspaceLevelManager.getServicePrincipalFromName("NonexistentPrincipal");

        assertTrue(result.isLeft());
        assertTrue(
                result.getLeft().problems().get(0).description().contains("No principal named NonexistentPrincipal"));
    }

    @Test
    void testGetServicePrincipalFromName_ExceptionThrown() {
        Workspace workspace =
                new Workspace().setDeploymentName("deploymentName").setWorkspaceName("TestWorkspace");
        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://deploymentName.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(workspacesAPI);
        when(workspacesAPI.list()).thenReturn(Collections.singletonList(workspace));

        when(workspaceClient.servicePrincipals()).thenThrow(new RuntimeException("Simulated exception"));

        Either<FailedOperation, ServicePrincipal> result =
                workspaceLevelManager.getServicePrincipalFromName("AnyPrincipal");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains("Simulated exception"));
    }

    @Test
    void testGetSqlWarehouseIdFromName_WarehouseFound() {
        DataSource warehouse1 = new DataSource().setName("warehouse1").setWarehouseId("id1");
        DataSource warehouse2 = new DataSource().setName("warehouse2").setWarehouseId("id2");

        when(workspaceClient.dataSources()).thenReturn(mock(DataSourcesAPI.class));
        when(workspaceClient.dataSources().list()).thenReturn(List.of(warehouse1, warehouse2));

        Either<FailedOperation, String> result = workspaceLevelManager.getSqlWarehouseIdFromName("warehouse1");

        assertTrue(result.isRight());
        assertEquals("id1", result.get());
    }

    @Test
    void testGetSqlWarehouseIdFromName_WarehouseNotFound() {
        when(workspaceClient.dataSources()).thenReturn(mock(DataSourcesAPI.class));
        when(workspaceClient.dataSources().list()).thenReturn(List.of());

        Either<FailedOperation, String> result = workspaceLevelManager.getSqlWarehouseIdFromName("nonexistent");

        assertTrue(result.isLeft());
        FailedOperation error = result.getLeft();
        assert (error.problems()
                .get(0)
                .description()
                .contains("An error occurred while searching for Sql Warehouse 'nonexistent' details."));
    }

    @Test
    void testGetWorkspaceName_WorkspaceFound() {
        Workspace workspace =
                new Workspace().setDeploymentName("deploymentName").setWorkspaceName("TestWorkspace");

        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://deploymentName.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(workspacesAPI);
        when(workspacesAPI.list()).thenReturn(Collections.singletonList(workspace));

        String result = workspaceLevelManager.getWorkspaceName();
        assertEquals("TestWorkspace", result);
    }

    @Test
    void testGetWorkspaceName_MultipleWorkspaces_WorkspaceFound() {
        Workspace workspace1 =
                new Workspace().setDeploymentName("deploymentName1").setWorkspaceName("Workspace1");
        Workspace workspace2 =
                new Workspace().setDeploymentName("deploymentName2").setWorkspaceName("Workspace2");

        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://deploymentName2.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(workspacesAPI);
        when(workspacesAPI.list()).thenReturn(List.of(workspace1, workspace2));

        String result = workspaceLevelManager.getWorkspaceName();
        assertEquals("Workspace2", result);
    }

    @Test
    void testGetWorkspaceName_MultipleWorkspaces_NoMatch() {
        Workspace workspace1 =
                new Workspace().setDeploymentName("deploymentName1").setWorkspaceName("Workspace1");
        Workspace workspace2 =
                new Workspace().setDeploymentName("deploymentName2").setWorkspaceName("Workspace2");

        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://unknownHost.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(workspacesAPI);
        when(workspacesAPI.list()).thenReturn(List.of(workspace1, workspace2));

        Exception exception =
                assertThrows(NoSuchElementException.class, () -> workspaceLevelManager.getWorkspaceName());
        assertTrue(exception.getMessage().contains("No value present"));
    }

    @Test
    void testGetWorkspaceName_WorkspaceNotFound() {
        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://unknownHost.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(workspacesAPI);
        when(workspacesAPI.list()).thenReturn(Collections.emptyList());

        Exception exception =
                assertThrows(NoSuchElementException.class, () -> workspaceLevelManager.getWorkspaceName());
        assertTrue(exception.getMessage().contains("No value present"));
    }

    @Test
    void testGetComputeClusterIdFromName_ClusterFound() {
        ClusterDetails cluster1 =
                new ClusterDetails().setClusterName("cluster1").setClusterId("id1");
        ClusterDetails cluster2 =
                new ClusterDetails().setClusterName("cluster2").setClusterId("id2");

        when(workspaceClient.clusters()).thenReturn(mock(ClustersExt.class));
        when(workspaceClient.clusters().list(any(ListClustersRequest.class))).thenReturn(List.of(cluster1, cluster2));

        Either<FailedOperation, String> result = workspaceLevelManager.getComputeClusterIdFromName("cluster1");

        assertTrue(result.isRight());
        assertEquals("id1", result.get());
    }

    @Test
    void testGetComputeClusterIdFromName_ClusterNotFound() {
        when(workspaceClient.clusters()).thenReturn(mock(ClustersExt.class));
        when(workspaceClient.clusters().list(any(ListClustersRequest.class))).thenReturn(Collections.emptyList());

        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://deploymentName.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(mock(WorkspacesAPI.class));
        Workspace workspace = new Workspace().setWorkspaceName("workspaceName");
        workspace.setDeploymentName("deploymentName");

        when(accountClient.workspaces().list()).thenReturn(Collections.singletonList(workspace));

        Either<FailedOperation, String> result = workspaceLevelManager.getComputeClusterIdFromName("nonexistent");

        assertTrue(result.isLeft());
        FailedOperation error = result.getLeft();
        assert (error.problems()
                .get(0)
                .description()
                .contains("An error occurred while searching for Cluster 'nonexistent' in workspace 'workspaceName'"));
    }

    @Test
    void testSetGitCredentials_CredentialsCreatedSuccessfully() {
        GitCredentialsConfig gitCredentialsConfig = new GitCredentialsConfig();
        gitCredentialsConfig.setProvider("GitHub");
        gitCredentialsConfig.setUsername("exampleUser");
        gitCredentialsConfig.setToken("exampleToken");

        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://exampleWorkspace.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(mock(WorkspacesAPI.class));
        when(accountClient.workspaces().list())
                .thenReturn(Collections.singletonList(
                        new Workspace().setWorkspaceName("exampleWorkspace").setDeploymentName("exampleWorkspace")));

        GitCredentialsAPI gitCredentialsAPI = mock(GitCredentialsAPI.class);
        when(workspaceClient.gitCredentials()).thenReturn(gitCredentialsAPI);

        when(workspaceClient.gitCredentials()).thenReturn(mock(GitCredentialsAPI.class));
        when(workspaceClient.gitCredentials().list()).thenReturn(Collections.emptyList());

        Either<FailedOperation, Void> result =
                workspaceLevelManager.setGitCredentials(workspaceClient, gitCredentialsConfig);

        assertTrue(result.isRight());
    }

    @Test
    void testSetGitCredentials_CredentialsUpdatedSuccessfully() {
        GitCredentialsConfig gitCredentialsConfig = new GitCredentialsConfig();
        gitCredentialsConfig.setProvider("GitHub");
        gitCredentialsConfig.setUsername("updatedUser");
        gitCredentialsConfig.setToken("updatedToken");

        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://exampleWorkspace.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(mock(WorkspacesAPI.class));
        when(accountClient.workspaces().list())
                .thenReturn(Collections.singletonList(
                        new Workspace().setWorkspaceName("exampleWorkspace").setDeploymentName("exampleWorkspace")));

        CredentialInfo existingCredential =
                new CredentialInfo().setCredentialId(123L).setGitProvider("GitHub");

        when(workspaceClient.gitCredentials()).thenReturn(mock(GitCredentialsAPI.class));
        when(workspaceClient.gitCredentials().list()).thenReturn(Collections.singletonList(existingCredential));

        Either<FailedOperation, Void> result =
                workspaceLevelManager.setGitCredentials(workspaceClient, gitCredentialsConfig);

        assertTrue(result.isRight());
        verify(workspaceClient.gitCredentials()).update(any(UpdateCredentialsRequest.class));
    }

    @Test
    void testSetGitCredentials_ThrowsException() {
        GitCredentialsConfig gitCredentialsConfig = new GitCredentialsConfig();
        gitCredentialsConfig.setProvider("GitHub");
        gitCredentialsConfig.setUsername("exampleUser");
        gitCredentialsConfig.setToken("exampleToken");

        when(workspaceClient.config()).thenReturn(mock(DatabricksConfig.class));
        when(workspaceClient.config().getHost()).thenReturn("https://exampleWorkspace.azuredatabricks.net");
        when(accountClient.workspaces()).thenReturn(mock(WorkspacesAPI.class));
        when(accountClient.workspaces().list())
                .thenReturn(Collections.singletonList(
                        new Workspace().setWorkspaceName("exampleWorkspace").setDeploymentName("exampleWorkspace")));

        when(workspaceClient.gitCredentials()).thenThrow(new RuntimeException("Simulated exception"));

        Either<FailedOperation, Void> result =
                workspaceLevelManager.setGitCredentials(workspaceClient, gitCredentialsConfig);

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains("Simulated exception"));
    }

    @Test
    void testGenerateSecretForServicePrincipal_Success() {
        long servicePrincipalId = 123L;
        String lifetimeSeconds = "3600s";

        CreateServicePrincipalSecretResponse response =
                new CreateServicePrincipalSecretResponse().setId("secretId").setSecret("secretValue");

        when(accountClient.servicePrincipalSecrets()).thenReturn(mock(ServicePrincipalSecretsAPI.class));
        when(accountClient.servicePrincipalSecrets().create(any(CreateServicePrincipalSecretRequest.class)))
                .thenReturn(response);

        Either<FailedOperation, Map.Entry<String, String>> result =
                workspaceLevelManager.generateSecretForServicePrincipal(servicePrincipalId, lifetimeSeconds);

        assertTrue(result.isRight());
        assertEquals("secretId", result.get().getKey());
        assertEquals("secretValue", result.get().getValue());
    }

    @Test
    void testDeleteServicePrincipalSecret_Success() {
        long servicePrincipalId = 123L;
        String secretId = "secretId";

        when(accountClient.servicePrincipalSecrets()).thenReturn(mock(ServicePrincipalSecretsAPI.class));
        Either<FailedOperation, Void> result =
                workspaceLevelManager.deleteServicePrincipalSecret(servicePrincipalId, secretId);

        assertTrue(result.isRight());
        verify(accountClient.servicePrincipalSecrets(), times(1))
                .delete(any(DeleteServicePrincipalSecretRequest.class));
    }

    @Test
    void testDeleteServicePrincipalSecret_ExceptionThrown() {
        long servicePrincipalId = 123L;
        String secretId = "secretId";

        when(accountClient.servicePrincipalSecrets()).thenThrow(new RuntimeException("Simulated exception"));

        Either<FailedOperation, Void> result =
                workspaceLevelManager.deleteServicePrincipalSecret(servicePrincipalId, secretId);

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains("Simulated exception"));
    }

    @Test
    void testGenerateSecretForServicePrincipal_ExceptionThrown() {
        long servicePrincipalId = 123L;
        String lifetimeSeconds = "3600s";

        when(accountClient.servicePrincipalSecrets()).thenReturn(mock(ServicePrincipalSecretsAPI.class));
        when(accountClient.servicePrincipalSecrets().create(any(CreateServicePrincipalSecretRequest.class)))
                .thenThrow(new RuntimeException("Simulated exception"));

        Either<FailedOperation, Map.Entry<String, String>> result =
                workspaceLevelManager.generateSecretForServicePrincipal(servicePrincipalId, lifetimeSeconds);

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains("Simulated exception"));
    }
}
