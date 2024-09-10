package it.agilelab.witboost.provisioning.databricks.permissions;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.core.http.HttpResponse;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.core.management.exception.ManagementException;
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
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import java.util.Collections;
import java.util.List;
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
                "resId",
                "GUID",
                azurePermissionsConfig.getDpOwnerRoleDefinitionId(),
                "principalId",
                PrincipalType.USER);

        assertTrue(result.isRight());
    }

    @Test
    void testAssignPermissions_Failure() {
        String errorMessage = "Error performing operations";

        when(roleAssignmentsClient.createWithResponse(
                        anyString(), anyString(), any(RoleAssignmentCreateParameters.class), any(Context.class)))
                .thenThrow(new RuntimeException(errorMessage));

        Either<FailedOperation, Void> result = azurePermissionsManager.assignPermissions(
                "resId",
                "GUID",
                azurePermissionsConfig.getDpOwnerRoleDefinitionId(),
                "principalId",
                PrincipalType.USER);

        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    void testAssignPermissions_RoleAlreadyExists() {
        String errorMessage =
                "Status code 409, \"{\"error\":{\"code\":\"RoleAssignmentExists\",\"message\":\"The role assignment already exists.\"}}\"";

        HttpResponse mockResponse = mock(HttpResponse.class);

        when(roleAssignmentsClient.createWithResponse(
                        anyString(), anyString(), any(RoleAssignmentCreateParameters.class), any(Context.class)))
                .thenThrow(new ManagementException(errorMessage, mockResponse));

        Either<FailedOperation, Void> result = azurePermissionsManager.assignPermissions(
                "resId",
                "GUID",
                azurePermissionsConfig.getDpOwnerRoleDefinitionId(),
                "principalId",
                PrincipalType.USER);

        assertTrue(result.isRight());
    }

    @Test
    void testAssignPermissions_ErrorWithManagementException() {
        String errorMessage = "This is a management exception";

        HttpResponse mockResponse = mock(HttpResponse.class);

        when(roleAssignmentsClient.createWithResponse(
                        anyString(), anyString(), any(RoleAssignmentCreateParameters.class), any(Context.class)))
                .thenThrow(new ManagementException(errorMessage, mockResponse));

        Either<FailedOperation, Void> result = azurePermissionsManager.assignPermissions(
                "resId",
                "GUID",
                azurePermissionsConfig.getDpOwnerRoleDefinitionId(),
                "principalId",
                PrincipalType.USER);

        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    void testGetPrincipalRoleAssignmentsOnResource_Success() {
        PagedIterable<RoleAssignmentInner> mockPagedIterable = mock(PagedIterable.class);
        RoleAssignmentInner mockRoleAssignment = mock(RoleAssignmentInner.class);

        when(mockRoleAssignment.principalId()).thenReturn("principalId");
        when(mockPagedIterable.spliterator())
                .thenReturn(Collections.singleton(mockRoleAssignment).spliterator());

        when(roleAssignmentsClient.listForResource(
                        anyString(), anyString(), anyString(), anyString(), isNull(), isNull(), any(Context.class)))
                .thenReturn(mockPagedIterable);

        Either<FailedOperation, List<RoleAssignmentInner>> result =
                azurePermissionsManager.getPrincipalRoleAssignmentsOnResource(
                        "resourceGroupName",
                        "resourceProviderNamespace",
                        "resourceType",
                        "resourceName",
                        "principalId");

        assertTrue(result.isRight());
        assertEquals(1, result.get().size());
        assertEquals("principalId", result.get().get(0).principalId());
    }

    @Test
    void testGetPrincipalRoleAssignmentsOnResource_Failure() {
        String errorMessage = "Error retrieving role assignments";

        when(roleAssignmentsClient.listForResource(
                        anyString(), anyString(), anyString(), anyString(), isNull(), isNull(), any(Context.class)))
                .thenThrow(new RuntimeException(errorMessage));

        Either<FailedOperation, List<RoleAssignmentInner>> result =
                azurePermissionsManager.getPrincipalRoleAssignmentsOnResource(
                        "resourceGroupName",
                        "resourceProviderNamespace",
                        "resourceType",
                        "resourceName",
                        "principalId");
        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    void testDeleteRoleAssignment_Success() {
        Either<FailedOperation, Void> result =
                azurePermissionsManager.deleteRoleAssignment("resourceName", "roleAssignmentId");

        assertTrue(result.isRight());
    }

    @Test
    void testDeleteRoleAssignment_Failure() {
        String errorMessage = "Error deleting role assignment";

        doThrow(new RuntimeException(errorMessage)).when(roleAssignmentsClient).deleteById(anyString());

        Either<FailedOperation, Void> result =
                azurePermissionsManager.deleteRoleAssignment("resourceName", "roleAssignmentId");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }
}
