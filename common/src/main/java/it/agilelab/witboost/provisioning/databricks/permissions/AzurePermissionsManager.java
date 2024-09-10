package it.agilelab.witboost.provisioning.databricks.permissions;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.Response;
import com.azure.core.management.exception.ManagementException;
import com.azure.core.util.Context;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.authorization.fluent.models.RoleAssignmentInner;
import com.azure.resourcemanager.authorization.models.PrincipalType;
import com.azure.resourcemanager.authorization.models.RoleAssignmentCreateParameters;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

public class AzurePermissionsManager {
    private final Logger logger = LoggerFactory.getLogger(AzurePermissionsManager.class);

    @Autowired
    AzureResourceManager azureResourceManager;

    public AzurePermissionsManager(AzureResourceManager azureResourceManager) {
        this.azureResourceManager = azureResourceManager;
    }

    /**
     * Assigns permissions to a resource in Azure.
     *
     * @param resourceId            The scope of the operation or resource. Valid scopes are: subscription (format: '/subscriptions/{subscriptionId}'), resource group (format: '/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}', or resource (format: '/subscriptions/{subscriptionId}/resourceGroups/{resourceGroupName}/providers/{resourceProviderNamespace}/[{parentResourcePath}/]{resourceType}/{resourceName}'.
     * @param roleAssignmentName    The name of the role assignment. It can be any valid GUID.
     * @param roleDefinitionId      The ID of the role definition to assign.
     * @param principalId           The ID of the principal to which the role is assigned.
     * @param principalType         The type of the principal (e.g., User, Group).
     * @return                      Either a success or a failure indication.
     */
    public Either<FailedOperation, Void> assignPermissions(
            String resourceId,
            String roleAssignmentName,
            String roleDefinitionId,
            String principalId,
            PrincipalType principalType) {

        try {
            Response<RoleAssignmentInner> response = azureResourceManager
                    .accessManagement()
                    .roleAssignments()
                    .manager()
                    .roleServiceClient()
                    .getRoleAssignments()
                    .createWithResponse(
                            resourceId,
                            roleAssignmentName,
                            new RoleAssignmentCreateParameters()
                                    .withRoleDefinitionId(roleDefinitionId)
                                    .withPrincipalId(principalId)
                                    .withPrincipalType(principalType),
                            Context.NONE);

            if (response.getStatusCode() == 201) {
                logger.info(String.format(
                        "Successfully assigned permissions for principal [ID: %s, Type: %s] on resource [ID: %s]. Role: %s. Azure role assignment ID: %s",
                        principalId, principalType, resourceId, roleDefinitionId, roleAssignmentName));

                return right(null);
            } else
                return left(new FailedOperation(Collections.singletonList(new Problem(String.format(
                        "Error assigning permissions for principal [ID: %s, Type: %s] on resource [ID: %s]. Azure response status code: %d",
                        principalId, principalType, resourceId, response.getStatusCode())))));

        } catch (ManagementException e) {
            if (e.getMessage()
                    .equalsIgnoreCase(
                            "Status code 409, \"{\"error\":{\"code\":\"RoleAssignmentExists\",\"message\":\"The role assignment already exists.\"}}\"")) {

                logger.info(String.format(
                        "Role assignment already exists for principal [ID: %s, Type: %s] on resource [ID: %s]. Skipping creation.",
                        principalId, principalType, resourceId));
                return right(null);
            }

            String errorMessage = String.format(
                    "Error while assigning permissions for principal [ID: %s, Type: %s] on resource [ID: %s]. Please try again and if the error persists contact the platform team. Azure response message: %s",
                    principalId, principalType, resourceId, e.getMessage());

            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));

        } catch (Exception e) {
            String errorMessage = String.format(
                    "Unexpected error while assigning permissions for principal [ID: %s, Type: %s] on resource [ID: %s]. Please try again and if the error persists contact the platform team. Details: %s",
                    principalId, principalType, resourceId, e.getMessage());

            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Retrieves the role assignments of a specific principal on a given Azure resource.
     *
     * @param resourceGroupName           The name of the resource group containing the resource.
     * @param resourceProviderNamespace   The provider namespace of the resource (e.g., 'Microsoft.Databricks').
     * @param resourceType                The type of the resource (e.g., 'workspaces').
     * @param resourceName                The name of the resource.
     * @param principalId                 The ID of the principal (user or group) whose role assignments are being retrieved.
     * @return                            Either a list of role assignments (right) or a failure (left) with the error details.
     */
    public Either<FailedOperation, List<RoleAssignmentInner>> getPrincipalRoleAssignmentsOnResource(
            String resourceGroupName,
            String resourceProviderNamespace,
            String resourceType,
            String resourceName,
            String principalId) {

        try {

            logger.info(String.format(
                    "Retrieving role assignments for principal [ID: %s] on resource [Name: %s, Group: %s, Type: %s].",
                    principalId, resourceName, resourceGroupName, resourceType));

            PagedIterable<RoleAssignmentInner> listPermissions = azureResourceManager
                    .accessManagement()
                    .roleAssignments()
                    .manager()
                    .roleServiceClient()
                    .getRoleAssignments()
                    .listForResource(
                            resourceGroupName,
                            resourceProviderNamespace,
                            resourceType,
                            resourceName,
                            null,
                            null,
                            Context.NONE);

            List<RoleAssignmentInner> existingPermissions = Optional.ofNullable(listPermissions)
                    .map(Iterable::spliterator)
                    .map(spliterator -> StreamSupport.stream(spliterator, false))
                    .orElseGet(Stream::empty)
                    .filter(permission -> permission.principalId().equalsIgnoreCase(principalId))
                    .collect(Collectors.toList());

            return right(existingPermissions);

        } catch (Exception e) {

            String errorMessage = String.format(
                    "Error retrieving role assignments for principal [ID: %s] on resource [Name: %s, Group: %s, Type: %s]. Please try again, and if the issue persists, contact the platform team. Details: %s",
                    principalId, resourceName, resourceGroupName, resourceType, e.getMessage());

            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Deletes a specific role assignment on an Azure resource.
     *
     * @param resourceName      The name of the resource where the role assignment exists.
     * @param roleAssignmentId  The ID of the role assignment to be deleted.
     * @return                  Either success (right) if the role assignment is successfully deleted or failure (left) with the error details.
     */
    public Either<FailedOperation, Void> deleteRoleAssignment(String resourceName, String roleAssignmentId) {

        try {
            azureResourceManager
                    .accessManagement()
                    .roleAssignments()
                    .manager()
                    .roleServiceClient()
                    .getRoleAssignments()
                    .deleteById(roleAssignmentId);

            logger.info(String.format(
                    "Successfully deleted role assignment [ID: %s] on resource [Name: %s].",
                    roleAssignmentId, resourceName));

            return right(null);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error deleting role assignment [ID: %s] on resource [Name: %s]. Please try again and if the error persists contact the platform team. Details: %s",
                    roleAssignmentId, resourceName, e.getMessage());

            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
