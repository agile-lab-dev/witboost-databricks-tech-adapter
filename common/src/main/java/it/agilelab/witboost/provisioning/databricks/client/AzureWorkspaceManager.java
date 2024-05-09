package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import com.azure.resourcemanager.databricks.models.Sku;
import com.azure.resourcemanager.databricks.models.Workspace;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import java.util.Collections;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Manages operations on Azure Databricks workspaces.
 */
public class AzureWorkspaceManager {

    private final AzureDatabricksManager manager;
    private static Logger logger = Logger.getLogger(AzureWorkspaceManager.class.getName());

    public AzureWorkspaceManager(AzureDatabricksManager manager) {
        this.manager = manager;
    }

    /**
     * Deletes a workspace.
     *
     * @param resourceGroupName The name of the resource group containing the workspace.
     * @param workspaceName     The name of the workspace to delete.
     * @return                  Either a list of failed operations if an exception occurs during deletion, or void if successful.
     */
    public Either<FailedOperation, Void> deleteWorkspace(String resourceGroupName, String workspaceName) {
        try {
            manager.workspaces().delete(resourceGroupName, workspaceName, com.azure.core.util.Context.NONE);
            return right(null);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
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
     * @return                          Either a workspace ID if the operation is successful, or a list of failed operations.
     */
    public Either<FailedOperation, DatabricksWorkspaceInfo> createWorkspace(
            String workspaceName,
            String region,
            String existingResourceGroupName,
            String managedResourceGroupId,
            SkuType skuType) {
        try {
            Workspace w = manager.workspaces()
                    .define(workspaceName)
                    .withRegion(region)
                    .withExistingResourceGroup(existingResourceGroupName)
                    .withManagedResourceGroupId(managedResourceGroupId)
                    .withSku(new Sku().withName(skuType.getValue()))
                    .create();

            DatabricksWorkspaceInfo workspaceInfo =
                    new DatabricksWorkspaceInfo(w.name(), w.workspaceId(), w.workspaceUrl());
            return right(workspaceInfo);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }
}
