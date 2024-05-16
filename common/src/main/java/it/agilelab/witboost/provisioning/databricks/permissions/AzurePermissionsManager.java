package it.agilelab.witboost.provisioning.databricks.permissions;

import static io.vavr.control.Either.left;

import com.azure.core.http.rest.Response;
import com.azure.core.util.Context;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.authorization.fluent.models.RoleAssignmentInner;
import com.azure.resourcemanager.authorization.models.PrincipalType;
import com.azure.resourcemanager.authorization.models.RoleAssignmentCreateParameters;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

public class AzurePermissionsManager {
    private static final Logger logger = Logger.getLogger(AzurePermissionsManager.class.getName());

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
    Either<FailedOperation, Void> assignPermissions(
            AzureResourceManager azureResourceManager,
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

            if (response.getStatusCode() == 201) return Either.right(null);
            else
                return left(new FailedOperation(Collections.singletonList(new Problem(String.format(
                        "Error assigning permissions to the resource %s. Azure response status code: %d",
                        resourceId, response.getStatusCode())))));

        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }
}
