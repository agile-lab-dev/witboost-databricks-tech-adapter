package it.agilelab.witboost.provisioning.databricks.service;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.authorization.models.PrincipalType;
import com.databricks.sdk.WorkspaceClient;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.DatabricksWorkspaceClientBean;
import it.agilelab.witboost.provisioning.databricks.client.AzureWorkspaceManager;
import it.agilelab.witboost.provisioning.databricks.client.SkuType;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.common.ProvisioningException;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.DatabricksJobWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.permissions.AzurePermissionsManager;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureMapper;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WorkspaceHandler {

    private final Logger logger = LoggerFactory.getLogger(WorkspaceHandler.class);
    private final AzurePermissionsConfig azurePermissionsConfig;
    private final DatabricksAuthConfig databricksAuthConfig;
    private final AzureAuthConfig azureAuthConfig;
    private final GitCredentialsConfig gitCredentialsConfig;
    private final AzureMapper azureMapper;
    private final AzureResourceManager azureResourceManager;
    private final AzurePermissionsManager azurePermissionsManager;
    private final AzureWorkspaceManager azureWorkspaceManager;
    private final DatabricksWorkspaceClientBean databricksWorkspaceClientBean;

    @Autowired
    public WorkspaceHandler(
            AzureWorkspaceManager azureWorkspaceManager,
            AzurePermissionsConfig azurePermissionsConfig,
            GitCredentialsConfig gitCredentialsConfig,
            DatabricksAuthConfig databricksAuthConfig,
            AzureAuthConfig azureAuthConfig,
            AzureMapper azureMapper,
            AzurePermissionsManager azurePermissionsManager,
            DatabricksWorkspaceClientBean databricksWorkspaceClientBean,
            AzureResourceManager azureResourceManager) {
        this.azureWorkspaceManager = azureWorkspaceManager;
        this.azurePermissionsConfig = azurePermissionsConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
        this.databricksAuthConfig = databricksAuthConfig;
        this.azureAuthConfig = azureAuthConfig;
        this.azureMapper = azureMapper;
        this.azurePermissionsManager = azurePermissionsManager;
        this.databricksWorkspaceClientBean = databricksWorkspaceClientBean;
        this.azureResourceManager = azureResourceManager;
    }

    public <T extends Specific> Either<FailedOperation, DatabricksWorkspaceInfo> provisionWorkspace(
            ProvisionRequest<T> provisionRequest) {

        Either<FailedOperation, DatabricksWorkspaceInfo> eitherNewWorkspace =
                createDatabricksWorkspace(provisionRequest);

        if (eitherNewWorkspace.isLeft()) return left(eitherNewWorkspace.getLeft());

        DatabricksWorkspaceInfo databricksWorkspaceInfo = eitherNewWorkspace.get();

        Either<FailedOperation, Void> eitherAssignPermissions =
                assignAzurePermissions(provisionRequest, databricksWorkspaceInfo);
        if (eitherAssignPermissions.isLeft()) return left(eitherAssignPermissions.getLeft());

        return right(eitherNewWorkspace.get());
    }

    public <T extends Specific> Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> getWorkspaceInfo(
            ProvisionRequest<T> provisionRequest) {

        String workspaceName = getWorkspaceName(provisionRequest)
                .getOrElseThrow(() -> new ProvisioningException("Failed to get workspace name"));

        String managedResourceGroupId = String.format(
                "/subscriptions/%s/resourceGroups/%s-rg", azurePermissionsConfig.getSubscriptionId(), workspaceName);

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherGetWorkspace =
                azureWorkspaceManager.getWorkspace(workspaceName, managedResourceGroupId);

        return eitherGetWorkspace;
    }

    public Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> getWorkspaceInfo(String workspaceName) {

        String managedResourceGroupId = String.format(
                "/subscriptions/%s/resourceGroups/%s-rg", azurePermissionsConfig.getSubscriptionId(), workspaceName);

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherGetWorkspace =
                azureWorkspaceManager.getWorkspace(workspaceName, managedResourceGroupId);

        return eitherGetWorkspace;
    }

    public Either<FailedOperation, String> getWorkspaceHost(String workspaceName) {

        try {
            String managedResourceGroupId = String.format(
                    "/subscriptions/%s/resourceGroups/%s-rg",
                    azurePermissionsConfig.getSubscriptionId(), workspaceName);

            Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherGetWorkspace =
                    azureWorkspaceManager.getWorkspace(workspaceName, managedResourceGroupId);

            return right(eitherGetWorkspace.get().get().getDatabricksHost());

        } catch (Exception e) {
            String errorMessage =
                    String.format("An error occurred while retrieving workspace host. Details: %s", e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    public Either<FailedOperation, WorkspaceClient> getWorkspaceClient(
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {
        try {
            return right(databricksWorkspaceClientBean.getObject(
                    databricksWorkspaceInfo.getDatabricksHost(), databricksWorkspaceInfo.getName()));
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while getting Databricks workspaceClient for workspace %s. Please try again and if the error persists contact the platform team. Details: %s",
                    databricksWorkspaceInfo.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private <T extends Specific> Either<FailedOperation, DatabricksWorkspaceInfo> createDatabricksWorkspace(
            ProvisionRequest<T> provisionRequest) {
        try {

            Either<FailedOperation, String> eitherWorkspaceName = getWorkspaceName(provisionRequest);
            if (eitherWorkspaceName.isLeft()) return left(eitherWorkspaceName.getLeft());

            String workspaceName = eitherWorkspaceName.get();
            String managedResourceGroupId = String.format(
                    "/subscriptions/%s/resourceGroups/%s-rg",
                    azurePermissionsConfig.getSubscriptionId(), workspaceName);

            SkuType skuType;
            var skuTypeConfig = azureAuthConfig.getSkuType();

            if (skuTypeConfig.equalsIgnoreCase(SkuType.TRIAL.getValue())) {
                skuType = SkuType.TRIAL;
            } else skuType = SkuType.PREMIUM;

            Either<FailedOperation, DatabricksWorkspaceInfo> eitherNewWorkspace = azureWorkspaceManager.createWorkspace(
                    workspaceName,
                    "westeurope",
                    azurePermissionsConfig.getResourceGroup(),
                    managedResourceGroupId,
                    skuType);

            if (eitherNewWorkspace.isRight())
                logger.info(String.format(
                        "Workspace available at: %s", eitherNewWorkspace.get().getDatabricksHost()));

            return eitherNewWorkspace;

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating workspace for component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private <T extends Specific> Either<FailedOperation, Void> assignAzurePermissions(
            ProvisionRequest<T> provisionRequest, DatabricksWorkspaceInfo databricksWorkspaceInfo) {
        try {

            // TODO: Assign permissions to the group!? Now are assigned to the DP owner

            String mail = provisionRequest.dataProduct().getDataProductOwner();
            Set<String> inputUser = Set.of(mail);

            String message = String.format(
                    "Assigning permissions to %s for workspace %s", mail, databricksWorkspaceInfo.getName());
            logger.info(message);

            Map<String, Either<Throwable, String>> res = azureMapper.map(inputUser);
            Either<Throwable, String> userMap = res.get(mail);

            if (userMap.isLeft()) {
                String errorMessage = String.format(
                        "Failed to get AzureID of: %s. Details:",
                        mail, userMap.getLeft().getMessage());
                logger.error(errorMessage, userMap.getLeft());
                return left(
                        new FailedOperation(Collections.singletonList(new Problem(errorMessage, userMap.getLeft()))));
            }

            String resourceId = String.format(
                    "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Databricks/workspaces/%s",
                    azurePermissionsConfig.getSubscriptionId(),
                    azurePermissionsConfig.getResourceGroup(),
                    databricksWorkspaceInfo.getName());

            return azurePermissionsManager.assignPermissions(
                    resourceId,
                    UUID.randomUUID().toString(),
                    azurePermissionsConfig.getRoleDefinitionId(),
                    userMap.get(),
                    PrincipalType.USER);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while assigning permissions to %s at %s. Details: %s",
                    provisionRequest.dataProduct().getDataProductOwner(),
                    databricksWorkspaceInfo.getName(),
                    e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    public <T extends Specific> Either<FailedOperation, String> getWorkspaceName(ProvisionRequest<T> provisionRequest) {
        try {
            Specific specific = provisionRequest.component().getSpecific();
            String workspaceName;

            if (specific instanceof DatabricksJobWorkloadSpecific) {
                workspaceName = ((DatabricksJobWorkloadSpecific) specific).getWorkspace();
            } else if (specific instanceof DatabricksDLTWorkloadSpecific) {
                workspaceName = ((DatabricksDLTWorkloadSpecific) specific).getWorkspace();
            } else if (specific instanceof DatabricksOutputPortSpecific) {
                workspaceName = ((DatabricksOutputPortSpecific) specific).getWorkspaceOP();
            } else {
                String errorMessage = String.format(
                        "The specific section of the component %s is not of type DatabricksJobWorkloadSpecific or DatabricksDLTWorkloadSpecific",
                        provisionRequest.component().getName());
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }
            return right(workspaceName);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while retrieving the workspace name for component %s. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
