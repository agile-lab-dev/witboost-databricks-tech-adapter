package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import com.azure.resourcemanager.databricks.implementation.WorkspaceImpl;
import com.azure.resourcemanager.databricks.implementation.WorkspacesImpl;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class AzureWorkspaceManagerTest {

    AzureDatabricksManager mockManager;
    AzureWorkspaceManager workspaceManager;

    @BeforeEach
    void setUp() {
        mockManager = mock(AzureDatabricksManager.class);
        workspaceManager = new AzureWorkspaceManager(mockManager);
    }

    @Test
    void testDeleteWorkspace_Success() {
        String resourceGroupName = "testResourceGroup";
        String workspaceName = "testWorkspace";

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(mockManager.workspaces()).thenReturn(mockWorkspaces);

        Either<FailedOperation, Void> result = workspaceManager.deleteWorkspace(resourceGroupName, workspaceName);

        assertEquals(Either.right(null), result);
        verify(mockManager.workspaces(), times(1))
                .delete(resourceGroupName, workspaceName, com.azure.core.util.Context.NONE);
    }

    @Test
    void testDeleteWorkspace_Failure() {
        String resourceGroupName = "testResourceGroup";
        String workspaceName = "testWorkspace";
        String errorMessage = "Workspace deletion failed";

        when(mockManager.workspaces()).thenThrow(new RuntimeException(errorMessage));
        Either<FailedOperation, Void> result = workspaceManager.deleteWorkspace(resourceGroupName, workspaceName);

        assertEquals(Either.left(new FailedOperation(Collections.singletonList(new Problem(errorMessage)))), result);
    }

    @Test
    void testCreateWorkspace_Success() {
        String workspaceName = "testWorkspace";
        String region = "westeurope";
        String existingResourceGroupName = "existingResourceGroup";
        String managedResourceGroupId = "managedResourceGroup";
        SkuType skuType = SkuType.TRIAL;

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(mockManager.workspaces()).thenReturn(mockWorkspaces);

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);

        when(mockManager.workspaces().define(workspaceName)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(region)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(existingResourceGroupName))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(managedResourceGroupId))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceManager.createWorkspace(
                workspaceName, region, existingResourceGroupName, managedResourceGroupId, skuType);

        assertTrue(result.isRight());
        assertTrue(result.get().getClass().equals(DatabricksWorkspaceInfo.class));
    }

    @Test
    void testCreateWorkspace_Failure() {
        String workspaceName = "testWorkspace";
        String region = "westeurope";
        String existingResourceGroupName = "existingResourceGroup";
        String managedResourceGroupId = "managedResourceGroup";
        SkuType skuType = SkuType.TRIAL;

        String errorMessage = "Failed to create workspace";
        RuntimeException exception = new RuntimeException(errorMessage);

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(mockManager.workspaces()).thenReturn(mockWorkspaces);

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);

        when(mockManager.workspaces().define(workspaceName)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(region)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(existingResourceGroupName))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(managedResourceGroupId))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenThrow(new RuntimeException(errorMessage));

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceManager.createWorkspace(
                workspaceName, region, existingResourceGroupName, managedResourceGroupId, skuType);

        assertEquals(Either.left(new FailedOperation(Collections.singletonList(new Problem(errorMessage)))), result);
    }
}
