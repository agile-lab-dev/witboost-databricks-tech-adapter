package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.azure.core.http.rest.PagedIterable;
import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import com.azure.resourcemanager.databricks.models.Sku;
import com.azure.resourcemanager.databricks.models.Workspace;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Manages operations on Azure Databricks workspaces.
 */
@Service
public class AzureWorkspaceManager {

    private final AzureDatabricksManager azureDatabricksManager;
    private final AzurePermissionsConfig azurePermissionsConfig;
    private final Logger logger = LoggerFactory.getLogger(AzureWorkspaceManager.class);

    @Autowired
    public AzureWorkspaceManager(
            AzureDatabricksManager azureDatabricksManager, AzurePermissionsConfig azurePermissionsConfig) {
        this.azureDatabricksManager = azureDatabricksManager;
        this.azurePermissionsConfig = azurePermissionsConfig;
    }

    /**
     * Deletes a workspace.
     *
     * @param resourceGroupName The name of the resource group containing the workspace.
     * @param workspaceName     The name of the workspace to delete.
     * @return Either a FailedOperation if an exception occurs during deletion, or Void if successful.
     */
    public Either<FailedOperation, Void> deleteWorkspace(String resourceGroupName, String workspaceName) {
        try {
            azureDatabricksManager
                    .workspaces()
                    .delete(resourceGroupName, workspaceName, com.azure.core.util.Context.NONE);
            return right(null);
        } catch (Exception e) {
            String error = String.format(
                    "An error occurred deleting the workspace: %s. Please try again and if the error persists contact the platform team. Details: %s",
                    workspaceName, e.getMessage());
            logger.error(error, e);

            return left(new FailedOperation(Collections.singletonList(new Problem(error, e))));
        }
    }

    /**
     * Creates a new Azure Databricks workspace.
     *
     * @param workspaceName             The name of the new workspace.
     * @param region                    The Azure region for the workspace.
     * @param existingResourceGroupName The name of an existing resource group to use for the workspace.
     * @param managedResourceGroupId    The managed resource group ID for the workspace.
     * @param skuType                   The SKU type for the workspace.
     * @return Either a DatabricksWorkspaceInfo if the operation is successful, or a FailedOperation.
     */
    public synchronized Either<FailedOperation, DatabricksWorkspaceInfo> createIfNotExistsWorkspace(
            String workspaceName,
            String region,
            String existingResourceGroupName,
            String managedResourceGroupId,
            SkuType skuType) {
        try {
            Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> workspace =
                    getWorkspace(workspaceName, managedResourceGroupId);

            if (workspace.isLeft()) return left(workspace.getLeft());

            if (workspace.get().isPresent()) {
                logger.info(String.format(
                        "Workspace %s already exists", workspace.get().get().getName()));
                return right(workspace.get().get());
            }

            logger.info(String.format("Creating workspace %s", workspaceName));
            Workspace w = azureDatabricksManager
                    .workspaces()
                    .define(workspaceName)
                    .withRegion(region)
                    .withExistingResourceGroup(existingResourceGroupName)
                    .withManagedResourceGroupId(managedResourceGroupId)
                    .withSku(new Sku().withName(skuType.getValue()))
                    .create();

            String resourceId = String.format(
                    "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Databricks/workspaces/%s",
                    azurePermissionsConfig.getSubscriptionId(), azurePermissionsConfig.getResourceGroup(), w.name());

            String azureUrl = String.format(
                    "https://portal.azure.com/#@%s/resource/%s", azurePermissionsConfig.getAuth_tenantId(), resourceId);

            var workspaceInfo = new DatabricksWorkspaceInfo(
                    w.name(), w.workspaceId(), w.workspaceUrl(), w.id(), azureUrl, w.provisioningState());
            return right(workspaceInfo);

        } catch (Exception e) {

            if (e.getMessage().contains("\"code\": \"ApplianceBeingCreated\"")) {
                String error = String.format(
                        "The workspace %s is currently being created. Please wait a few minutes and try again.",
                        workspaceName);
                logger.error(error, e);
                return left(new FailedOperation(Collections.singletonList(new Problem(error, e))));
            }

            String error = String.format(
                    "An error occurred creating the workspace: %s. Please try again and if the error persists contact the platform team. Details: %s",
                    workspaceName, e.getMessage());
            logger.error(error, e);

            return left(new FailedOperation(Collections.singletonList(new Problem(error, e))));
        }
    }

    /**
     * Retrieves information about an existing workspace.
     *
     * @param workspaceName          The name of the workspace to retrieve.
     * @param managedResourceGroupId The managed resource group ID for the workspace.
     * @return Either a DatabricksWorkspaceInfo if the workspace exists, or an empty Optional if not found,
     *         or a FailedOperation in case of errors.
     */
    public Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> getWorkspace(
            String workspaceName, String managedResourceGroupId) {
        try {

            DatabricksWorkspaceInfo workspaceInfo;
            Workspace w;

            PagedIterable<Workspace> listWorkspaces =
                    azureDatabricksManager.workspaces().list();

            Optional<Workspace> existingWorkspace = Optional.ofNullable(listWorkspaces)
                    .map(Iterable::spliterator)
                    .map(spliterator -> StreamSupport.stream(spliterator, false))
                    .orElseGet(Stream::empty)
                    .filter(workspace -> workspace.name().equalsIgnoreCase(workspaceName)
                            && workspace.managedResourceGroupId().equalsIgnoreCase(managedResourceGroupId))
                    .findFirst();

            if (existingWorkspace.isPresent()) {
                w = existingWorkspace.get();

                String resourceId = String.format(
                        "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Databricks/workspaces/%s",
                        azurePermissionsConfig.getSubscriptionId(),
                        azurePermissionsConfig.getResourceGroup(),
                        w.name());

                String azureUrl = String.format(
                        "https://portal.azure.com/#@%s/resource/%s",
                        azurePermissionsConfig.getAuth_tenantId(), resourceId);

                workspaceInfo = new DatabricksWorkspaceInfo(
                        w.name(), w.workspaceId(), w.workspaceUrl(), w.id(), azureUrl, w.provisioningState());
                return right(Optional.of(workspaceInfo));
            } else return right(Optional.empty());

        } catch (Exception e) {
            String error = String.format(
                    "An error occurred getting info of the workspace: %s. Please try again and if the error persists contact the platform team. Details: %s",
                    workspaceName, e.getMessage());
            logger.error(error, e);

            return left(new FailedOperation(Collections.singletonList(new Problem(error, e))));
        }
    }
}
