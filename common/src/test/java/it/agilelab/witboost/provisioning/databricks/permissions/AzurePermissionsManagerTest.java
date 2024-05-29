package it.agilelab.witboost.provisioning.databricks.permissions;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.resourcemanager.AccessManagement;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.authorization.AuthorizationManager;
import com.azure.resourcemanager.authorization.fluent.AuthorizationManagementClient;
import com.azure.resourcemanager.authorization.fluent.RoleAssignmentsClient;
import com.azure.resourcemanager.authorization.fluent.models.RoleAssignmentInner;
import com.azure.resourcemanager.authorization.models.PrincipalType;
import com.azure.resourcemanager.authorization.models.RoleAssignmentCreateParameters;
import com.azure.resourcemanager.authorization.models.RoleAssignments;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.DatabricksWorkspaceClientBean;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
class AzurePermissionsManagerTest {

    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @MockBean
    private AzureResourceManager azureResourceManager;

    @MockBean
    private DatabricksWorkspaceClientBean databricksWorkspaceClientBean;

    @InjectMocks
    @Autowired
    private AzurePermissionsManager azurePermissionsManager;

    @Mock
    private AccessManagement accessManagement;

    @Mock
    private RoleAssignments roleAssignments;

    @Mock
    private AuthorizationManager authorizationManager;

    @Mock
    private AuthorizationManagementClient authorizationManagementClient;

    @Mock
    private RoleAssignmentsClient roleAssignmentsClient;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(azureResourceManager.accessManagement()).thenReturn(accessManagement);
        when(accessManagement.roleAssignments()).thenReturn(roleAssignments);
        when(roleAssignments.manager()).thenReturn(authorizationManager);
        when(authorizationManager.roleServiceClient()).thenReturn(authorizationManagementClient);
        when(authorizationManagementClient.getRoleAssignments()).thenReturn(roleAssignmentsClient);
    }

    @Test
    void testAssignPermissions_Success() {
        Response<RoleAssignmentInner> simulatedResponse = mock(Response.class);
        when(simulatedResponse.getStatusCode()).thenReturn(201);

        when(roleAssignmentsClient.createWithResponse(
                        anyString(), anyString(), any(RoleAssignmentCreateParameters.class), any(Context.class)))
                .thenReturn(simulatedResponse);

        Either<FailedOperation, Void> result = azurePermissionsManager.assignPermissions(
                "resId", "GUID", azurePermissionsConfig.getRoleDefinitionId(), "principalId", PrincipalType.USER);

        assertTrue(result.isRight());
    }

    @Test
    void testAssignPermissions_Failure() {
        String errorMessage = "Error performing operations";

        when(roleAssignmentsClient.createWithResponse(
                        anyString(), anyString(), any(RoleAssignmentCreateParameters.class), any(Context.class)))
                .thenThrow(new RuntimeException(errorMessage));

        Either<FailedOperation, Void> result = azurePermissionsManager.assignPermissions(
                "resId", "GUID", azurePermissionsConfig.getRoleDefinitionId(), "principalId", PrincipalType.USER);

        assertEquals(Either.left(new FailedOperation(Collections.singletonList(new Problem(errorMessage)))), result);
    }
}
