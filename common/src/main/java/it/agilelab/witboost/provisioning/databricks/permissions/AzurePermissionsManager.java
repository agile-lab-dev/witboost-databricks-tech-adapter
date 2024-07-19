package it.agilelab.witboost.provisioning.databricks.permissions;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

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

            if (response.getStatusCode() == 201) return right(null);
            else
                return left(new FailedOperation(Collections.singletonList(new Problem(String.format(
                        "Error assigning permissions to the resource %s. Azure response status code: %d",
                        resourceId, response.getStatusCode())))));

        } catch (ManagementException e) {
            if (e.getMessage()
                    .equalsIgnoreCase(
                            "Status code 409, \"{\"error\":{\"code\":\"RoleAssignmentExists\",\"message\":\"The role assignment already exists.\"}}\"")) {
                logger.info("Role assignment already exists. Creation skipped");
                return right(null);
            }
            String errorMessage = String.format(
                    "An error occurred while assigning permissions to the Azure resource %s. Please try again and if the error persists contact the platform team. Details: %s",
                    resourceId, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while assigning permissions to the Azure resource %s. Please try again and if the error persists contact the platform team. Details: %s",
                    resourceId, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
