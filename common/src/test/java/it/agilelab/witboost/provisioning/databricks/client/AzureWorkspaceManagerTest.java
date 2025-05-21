package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.core.http.rest.PagedIterable;
import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import com.azure.resourcemanager.databricks.implementation.WorkspaceImpl;
import com.azure.resourcemanager.databricks.implementation.WorkspacesImpl;
import com.azure.resourcemanager.databricks.models.Workspace;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class AzureWorkspaceManagerTest {

    AzureDatabricksManager mockManager;
    AzureWorkspaceManager workspaceManager;

    @Mock
    AzurePermissionsConfig azurePermissionsConfig;

    @BeforeEach
    void setUp() {
        mockManager = mock(AzureDatabricksManager.class);
        workspaceManager = new AzureWorkspaceManager(mockManager, azurePermissionsConfig);
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
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
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

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceManager.createIfNotExistsWorkspace(
                workspaceName, region, existingResourceGroupName, managedResourceGroupId, skuType);

        assertTrue(result.isRight());
        assertTrue(result.get().getClass().equals(DatabricksWorkspaceInfo.class));
    }

    @Test
    void testCreateWorkspace_AlreadyExists() {
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

        Workspace mockWorkspace = mock(Workspace.class);
        when(mockWorkspace.name()).thenReturn(workspaceName);
        when(mockWorkspace.managedResourceGroupId()).thenReturn(managedResourceGroupId);
        when(mockWorkspace.workspaceUrl()).thenReturn("workspaceUrl");
        when(mockWorkspace.id()).thenReturn("id");

        PagedIterable<Workspace> mockPagedIterable = mock(PagedIterable.class);
        when(mockWorkspaces.list()).thenReturn(mockPagedIterable);

        List<Workspace> workspaceList = Collections.singletonList(mockWorkspace);
        Iterator<Workspace> workspaceIterator = workspaceList.iterator();
        when(mockPagedIterable.iterator()).thenReturn(workspaceIterator);
        when(mockPagedIterable.spliterator()).thenReturn(Spliterators.spliteratorUnknownSize(workspaceIterator, 0));

        when(mockWorkspaces.list()).thenReturn(mockPagedIterable);

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceManager.createIfNotExistsWorkspace(
                workspaceName, region, existingResourceGroupName, managedResourceGroupId, skuType);

        assertTrue(result.isRight());
        assertTrue(result.get().getClass().equals(DatabricksWorkspaceInfo.class));
    }

    @Test
    void testCreateWorkspace_GetWorkspaceLeft() {
        String workspaceName = "testWorkspace";
        String region = "westeurope";
        String existingResourceGroupName = "existingResourceGroup";
        String managedResourceGroupId = "managedResourceGroup";
        SkuType skuType = SkuType.TRIAL;

        String errorMessage = "Failed to create workspace";
        RuntimeException exception = new RuntimeException(errorMessage);

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceManager.createIfNotExistsWorkspace(
                workspaceName, region, existingResourceGroupName, managedResourceGroupId, skuType);

        assertTrue(result.isLeft());
        assertTrue(
                result.getLeft()
                        .problems()
                        .get(0)
                        .description()
                        .contains(
                                "Cannot invoke \"com.azure.resourcemanager.databricks.models.Workspaces.list()\" because the return value of \"com.azure.resourcemanager.databricks.AzureDatabricksManager.workspaces()\" is null"));
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

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceManager.createIfNotExistsWorkspace(
                workspaceName, region, existingResourceGroupName, managedResourceGroupId, skuType);

        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    void testGetWorkspace_Success() {

        String workspaceName = "testWorkspace";
        String managedResourceGroupId = "managedResourceGroup";

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(mockManager.workspaces()).thenReturn(mockWorkspaces);

        Workspace mockWorkspace = mock(Workspace.class);
        when(mockWorkspace.name()).thenReturn(workspaceName);
        when(mockWorkspace.managedResourceGroupId()).thenReturn(managedResourceGroupId);
        when(mockWorkspace.workspaceUrl()).thenReturn("workspaceUrl");
        when(mockWorkspace.id()).thenReturn("id");

        PagedIterable<Workspace> mockPagedIterable = mock(PagedIterable.class);
        when(mockWorkspaces.list()).thenReturn(mockPagedIterable);

        List<Workspace> workspaceList = Collections.singletonList(mockWorkspace);
        Iterator<Workspace> workspaceIterator = workspaceList.iterator();
        when(mockPagedIterable.iterator()).thenReturn(workspaceIterator);
        when(mockPagedIterable.spliterator()).thenReturn(Spliterators.spliteratorUnknownSize(workspaceIterator, 0));

        when(mockWorkspaces.list()).thenReturn(mockPagedIterable);

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> result =
                workspaceManager.getWorkspace(workspaceName, managedResourceGroupId);

        assertTrue(result.isRight());
        assertTrue(result.get().isPresent());
        DatabricksWorkspaceInfo info = result.get().get();
        assertEquals(workspaceName, info.getName());
        assertEquals("workspaceUrl", info.getDatabricksHost());
        assertEquals("id", info.getAzureResourceId());
    }

    @Test
    void testGetWorkspace_NotFound() {
        String workspaceName = "testWorkspace";
        String managedResourceGroupId = "managedResourceGroup";

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(mockManager.workspaces()).thenReturn(mockWorkspaces);

        PagedIterable<Workspace> mockPagedIterable = mock(PagedIterable.class);
        when(mockWorkspaces.list()).thenReturn(mockPagedIterable);
        when(mockPagedIterable.iterator()).thenReturn(Collections.emptyIterator());

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> result =
                workspaceManager.getWorkspace(workspaceName, managedResourceGroupId);

        assertTrue(result.isRight());
        assertFalse(result.get().isPresent());
    }

    @Test
    void testGetWorkspace_Failure() {
        String workspaceName = "testWorkspace";
        String managedResourceGroupId = "managedResourceGroup";
        String errorMessage = "Failed to get workspace";

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(mockManager.workspaces()).thenReturn(mockWorkspaces);
        when(mockWorkspaces.list()).thenThrow(new RuntimeException(errorMessage));

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> result =
                workspaceManager.getWorkspace(workspaceName, managedResourceGroupId);

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }
}
