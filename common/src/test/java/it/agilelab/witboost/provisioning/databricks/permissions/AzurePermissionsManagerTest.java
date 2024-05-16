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
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class AzurePermissionsManagerTest {

    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @Test
    void testAssignPermissions_Success() {

        System.out.println();

        AzureResourceManager azureResourceManager = mock(AzureResourceManager.class);

        Response<RoleAssignmentInner> simulatedResponse = mock(Response.class);
        when(simulatedResponse.getStatusCode()).thenReturn(201);

        AccessManagement accessManagement = mock(AccessManagement.class);
        RoleAssignments roleAssignments = mock(RoleAssignments.class);
        AuthorizationManager authorizationManager = mock(AuthorizationManager.class);
        AuthorizationManagementClient authorizationManagementClient = mock(AuthorizationManagementClient.class);
        RoleAssignmentsClient roleAssignmentsClient = mock(RoleAssignmentsClient.class);

        when(azureResourceManager.accessManagement()).thenReturn(accessManagement);
        when(accessManagement.roleAssignments()).thenReturn(roleAssignments);
        when(roleAssignments.manager()).thenReturn(authorizationManager);
        when(authorizationManager.roleServiceClient()).thenReturn(authorizationManagementClient);
        when(authorizationManagementClient.getRoleAssignments()).thenReturn(roleAssignmentsClient);

        when(roleAssignmentsClient.createWithResponse(
                        anyString(), anyString(), any(RoleAssignmentCreateParameters.class), any(Context.class)))
                .thenReturn(simulatedResponse);

        AzurePermissionsManager azurePermissionsManager = new AzurePermissionsManager();

        Either<FailedOperation, Void> result = azurePermissionsManager.assignPermissions(
                azureResourceManager,
                azurePermissionsConfig.getResourceId(),
                azurePermissionsConfig.getRoleAssignmentName(),
                azurePermissionsConfig.getRoleDefinitionId(),
                azurePermissionsConfig.getPrincipalId(),
                PrincipalType.fromString(azurePermissionsConfig.getPrincipalType()));

        assertTrue(result.isRight());
    }

    @Test
    void testAssignPermissions_Failure() {

        AzureResourceManager azureResourceManager = mock(AzureResourceManager.class);

        String errorMessage = "Error performing operations";

        AccessManagement accessManagement = mock(AccessManagement.class);
        RoleAssignments roleAssignments = mock(RoleAssignments.class);
        AuthorizationManager authorizationManager = mock(AuthorizationManager.class);
        AuthorizationManagementClient authorizationManagementClient = mock(AuthorizationManagementClient.class);
        RoleAssignmentsClient roleAssignmentsClient = mock(RoleAssignmentsClient.class);

        when(azureResourceManager.accessManagement()).thenReturn(accessManagement);
        when(accessManagement.roleAssignments()).thenReturn(roleAssignments);
        when(roleAssignments.manager()).thenReturn(authorizationManager);
        when(authorizationManager.roleServiceClient()).thenReturn(authorizationManagementClient);
        when(authorizationManagementClient.getRoleAssignments()).thenReturn(roleAssignmentsClient);

        when(roleAssignmentsClient.createWithResponse(
                        anyString(), anyString(), any(RoleAssignmentCreateParameters.class), any(Context.class)))
                .thenThrow(new RuntimeException(errorMessage));

        AzurePermissionsManager azurePermissionsManager = new AzurePermissionsManager();

        Either<FailedOperation, Void> result = azurePermissionsManager.assignPermissions(
                azureResourceManager,
                azurePermissionsConfig.getResourceId(),
                azurePermissionsConfig.getRoleAssignmentName(),
                azurePermissionsConfig.getRoleDefinitionId(),
                azurePermissionsConfig.getPrincipalId(),
                PrincipalType.fromString(azurePermissionsConfig.getPrincipalType()));

        assertEquals(Either.left(new FailedOperation(Collections.singletonList(new Problem(errorMessage)))), result);
    }
}
