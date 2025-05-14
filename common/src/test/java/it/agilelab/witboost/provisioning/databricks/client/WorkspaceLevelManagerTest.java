package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.mixin.ClustersExt;
import com.databricks.sdk.service.compute.ClusterDetails;
import com.databricks.sdk.service.compute.ListClustersRequest;
import com.databricks.sdk.service.provisioning.Workspace;
import com.databricks.sdk.service.provisioning.WorkspacesAPI;
import com.databricks.sdk.service.sql.DataSource;
import com.databricks.sdk.service.sql.DataSourcesAPI;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import java.util.Collections;
import java.util.List;
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

    @BeforeEach
    void setUp() {
        workspaceClient = mock(WorkspaceClient.class);
        accountClient = mock(AccountClient.class);
        workspaceLevelManager = new WorkspaceLevelManager(workspaceClient, accountClient);
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
}
