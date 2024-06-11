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
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import java.util.Collections;
import java.util.Optional;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Manages operations on Azure Databricks workspaces.
 */
@Service
public class AzureWorkspaceManager {

    private final AzureDatabricksManager azureDatabricksManager;
    private static Logger logger = Logger.getLogger(AzureWorkspaceManager.class.getName());

    @Autowired
    public AzureWorkspaceManager(AzureDatabricksManager azureDatabricksManager) {
        this.azureDatabricksManager = azureDatabricksManager;
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
            logger.severe(e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
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
    public Either<FailedOperation, DatabricksWorkspaceInfo> createWorkspace(
            String workspaceName,
            String region,
            String existingResourceGroupName,
            String managedResourceGroupId,
            SkuType skuType) {
        try {

            // Check if the workspace already exists
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
            var workspaceInfo = new DatabricksWorkspaceInfo(w.name(), w.workspaceId(), w.workspaceUrl(), w.id());
            return right(workspaceInfo);

        } catch (Exception e) {
            logger.severe(e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
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
                workspaceInfo = new DatabricksWorkspaceInfo(w.name(), w.workspaceId(), w.workspaceUrl(), w.id());
                return right(Optional.of(workspaceInfo));
            } else return right(Optional.empty());

        } catch (Exception e) {
            logger.severe(e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }
}
