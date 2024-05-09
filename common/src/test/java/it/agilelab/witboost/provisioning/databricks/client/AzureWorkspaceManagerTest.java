package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import com.azure.resourcemanager.databricks.implementation.WorkspacesImpl;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class AzureWorkspaceManagerTest {

    AzureDatabricksManager mockManager;
    AzureWorkspaceManager workspaceManager;

    @BeforeEach
    void setUp() {
        mockManager = mock(AzureDatabricksManager.class);
        workspaceManager = new AzureWorkspaceManager(mockManager);

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(mockManager.workspaces()).thenReturn(mockWorkspaces);
    }

    @Test
    void testDeleteWorkspace_Success() {
        String resourceGroupName = "testResourceGroup";
        String workspaceName = "testWorkspace";

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
}
