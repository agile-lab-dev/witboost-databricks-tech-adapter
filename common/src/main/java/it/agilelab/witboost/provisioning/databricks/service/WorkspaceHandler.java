package it.agilelab.witboost.provisioning.databricks.service;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.authorization.fluent.models.RoleAssignmentInner;
import com.azure.resourcemanager.authorization.models.PrincipalType;
import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.WorkspaceClient;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.WorkspaceClientConfig;
import it.agilelab.witboost.provisioning.databricks.client.AzureWorkspaceManager;
import it.agilelab.witboost.provisioning.databricks.client.SkuType;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.*;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.outputport.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.DatabricksJobWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.permissions.AzurePermissionsManager;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureMapper;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
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
    private final Function<WorkspaceClientConfig.WorkspaceClientConfigParams, WorkspaceClient> workspaceClientFactory;
    private static final String RESOURCE_ID_FORMAT =
            "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Databricks/workspaces/%s";

    private final Pattern databricksUrlPattern =
            Pattern.compile("(?:https://)?adb-(\\d+)\\.\\d+\\.azuredatabricks\\.net");

    @Autowired
    public WorkspaceHandler(
            AzureWorkspaceManager azureWorkspaceManager,
            AzurePermissionsConfig azurePermissionsConfig,
            GitCredentialsConfig gitCredentialsConfig,
            DatabricksAuthConfig databricksAuthConfig,
            AzureAuthConfig azureAuthConfig,
            AzureMapper azureMapper,
            AzurePermissionsManager azurePermissionsManager,
            Function<WorkspaceClientConfig.WorkspaceClientConfigParams, WorkspaceClient> workspaceClientFactory,
            AzureResourceManager azureResourceManager) {
        this.azureWorkspaceManager = azureWorkspaceManager;
        this.azurePermissionsConfig = azurePermissionsConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
        this.databricksAuthConfig = databricksAuthConfig;
        this.azureAuthConfig = azureAuthConfig;
        this.azureMapper = azureMapper;
        this.azurePermissionsManager = azurePermissionsManager;
        this.workspaceClientFactory = workspaceClientFactory;
        this.azureResourceManager = azureResourceManager;
    }

    public <T extends Specific> Either<FailedOperation, DatabricksWorkspaceInfo> provisionWorkspace(
            ProvisionRequest<T> provisionRequest) {

        var eitherWorkspaceInfo = getWorkspaceInfo(provisionRequest);

        // If the workspace information exists and is set to not be managed by the tech adapter, we immediately return
        // and avoid provisioning the workspace
        if (eitherWorkspaceInfo.isRight()
                && eitherWorkspaceInfo.get().isPresent()
                && !eitherWorkspaceInfo.get().get().isManaged()) {
            return right(eitherWorkspaceInfo.get().get());
        }

        Either<FailedOperation, DatabricksWorkspaceInfo> eitherNewWorkspace =
                createIfNotExistsDatabricksWorkspace(provisionRequest);

        if (eitherNewWorkspace.isLeft()) return left(eitherNewWorkspace.getLeft());

        DatabricksWorkspaceInfo databricksWorkspaceInfo = eitherNewWorkspace.get();

        Either<FailedOperation, Void> dpOwnerAzurePermissions = manageAzurePermissions(
                databricksWorkspaceInfo,
                provisionRequest.dataProduct().getDataProductOwner(),
                azurePermissionsConfig.getDpOwnerRoleDefinitionId(),
                PrincipalType.USER);
        if (dpOwnerAzurePermissions.isLeft()) return left(dpOwnerAzurePermissions.getLeft());

        // TODO: This is a temporary solution. Remove or update this logic in the future.
        String devGroup = provisionRequest.dataProduct().getDevGroup();
        if (!devGroup.startsWith("group:")) {
            devGroup = "group:" + devGroup;
        }

        Either<FailedOperation, Void> devGroupAzurePermissions = manageAzurePermissions(
                databricksWorkspaceInfo,
                devGroup,
                azurePermissionsConfig.getDevGroupRoleDefinitionId(),
                PrincipalType.GROUP);
        if (devGroupAzurePermissions.isLeft()) return left(devGroupAzurePermissions.getLeft());

        return right(databricksWorkspaceInfo);
    }

    /**
     * Returns the information of a Databricks Workspace from a ProvisionRequest
     * @param provisionRequest ProvisionRequest with the specific field.
     *                         If the `workspace` field in the specific class contains a valid Databricks Workspace URL, the `isManaged` property will be set to false, signaling the workspace must not be managed by the Tech Adapter.
     * @return An Optional containing the databricks workspace information if existent, or a FailedOperation if the method failed
     */
    public <T extends Specific> Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> getWorkspaceInfo(
            ProvisionRequest<T> provisionRequest) {
        return getWorkspaceName(provisionRequest).flatMap(this::getWorkspaceInfo);
    }

    /**
     * Returns the information of a Databricks Workspace
     * @param workspaceInfo Either the workspace name to be created or the workspace URL of an existent workspace
     * @return An Optional containing the databricks workspace information if existent, or a FailedOperation if the method failed
     */
    public Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> getWorkspaceInfo(String workspaceInfo) {
        var matcher = databricksUrlPattern.matcher(workspaceInfo);
        if (matcher.find()) {
            var workspaceID = matcher.group(1);
            return right(Optional.of(new DatabricksWorkspaceInfo(
                    workspaceInfo, workspaceID, null, workspaceInfo, ProvisioningState.SUCCEEDED)));
        } else {
            String managedResourceGroupId = String.format(
                    "/subscriptions/%s/resourceGroups/%s-rg",
                    azurePermissionsConfig.getSubscriptionId(), workspaceInfo);

            return azureWorkspaceManager.getWorkspace(workspaceInfo, managedResourceGroupId);
        }
    }

    public Either<FailedOperation, WorkspaceClient> getWorkspaceClient(
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {
        try {

            WorkspaceClientConfig.WorkspaceClientConfigParams workspaceClientAzureConfigParams =
                    new WorkspaceClientConfig.WorkspaceClientConfigParams(
                            WorkspaceClientConfig.WorkspaceClientConfigParams.AuthType.AZURE,
                            databricksAuthConfig,
                            azureAuthConfig,
                            databricksWorkspaceInfo.getDatabricksHost(),
                            databricksWorkspaceInfo.getName());

            return right(workspaceClientFactory.apply(workspaceClientAzureConfigParams));

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while getting Databricks workspaceClient for workspace %s. Please try again and if the error persists contact the platform team. Details: %s",
                    databricksWorkspaceInfo.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    <T extends Specific> Either<FailedOperation, DatabricksWorkspaceInfo> createIfNotExistsDatabricksWorkspace(
            ProvisionRequest<T> provisionRequest) {
        try {

            Either<FailedOperation, String> eitherWorkspaceName = getWorkspaceName(provisionRequest);
            if (eitherWorkspaceName.isLeft()) return left(eitherWorkspaceName.getLeft());

            String workspaceName = eitherWorkspaceName.get();
            String managedResourceGroupId = String.format(
                    "/subscriptions/%s/resourceGroups/%s-rg",
                    azurePermissionsConfig.getSubscriptionId(), workspaceName);

            SkuType skuType = azureAuthConfig.getSkuType().equalsIgnoreCase(SkuType.TRIAL.getValue())
                    ? SkuType.TRIAL
                    : SkuType.PREMIUM;

            Either<FailedOperation, DatabricksWorkspaceInfo> eitherNewWorkspace =
                    azureWorkspaceManager.createIfNotExistsWorkspace(
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

    protected Either<FailedOperation, Void> manageAzurePermissions(
            DatabricksWorkspaceInfo databricksWorkspaceInfo,
            String entity,
            String roleDefinitionId,
            PrincipalType principalType) {

        if (roleDefinitionId == null) return right(null);

        try {
            Set<String> inputEntity = Set.of(entity);

            String message = String.format(
                    "Managing permissions of %s for workspace %s. Assigning role definition %s",
                    entity, databricksWorkspaceInfo.getName(), roleDefinitionId);
            logger.info(message);

            Map<String, Either<Throwable, String>> res = azureMapper.map(inputEntity);
            Either<Throwable, String> entityMap = res.get(entity);

            if (entityMap.isLeft()) {
                String errorMessage = String.format(
                        "Failed to get AzureID of: %s. Details: %s",
                        entity, entityMap.getLeft().getMessage());
                logger.error(errorMessage, entityMap.getLeft());
                return left(
                        new FailedOperation(Collections.singletonList(new Problem(errorMessage, entityMap.getLeft()))));
            }

            if (roleDefinitionId.equalsIgnoreCase("no_permissions")) {
                return handleNoPermissions(databricksWorkspaceInfo, entityMap.get());
            } else {
                return assignPermissionsToEntity(
                        databricksWorkspaceInfo, entityMap.get(), roleDefinitionId, principalType);
            }
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while handling permissions of %s for the Azure resource %s. Details: %s",
                    entity, databricksWorkspaceInfo.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    protected Either<FailedOperation, Void> handleNoPermissions(
            DatabricksWorkspaceInfo databricksWorkspaceInfo, String entityId) {
        Either<FailedOperation, List<RoleAssignmentInner>> permissions =
                azurePermissionsManager.getPrincipalRoleAssignmentsOnResource(
                        azurePermissionsConfig.getResourceGroup(),
                        "Microsoft.Databricks",
                        "workspaces",
                        databricksWorkspaceInfo.getName(),
                        entityId);

        if (permissions.isLeft()) {
            return left(permissions.getLeft());
        }

        List<Problem> problems = new ArrayList<>();

        permissions.get().forEach(roleAssignmentInner -> {
            if (roleAssignmentInner.id().contains(databricksWorkspaceInfo.getName())) {
                Either<FailedOperation, Void> result = azurePermissionsManager.deleteRoleAssignment(
                        databricksWorkspaceInfo.getName(), roleAssignmentInner.id());
                if (result.isLeft()) problems.addAll(result.getLeft().problems());
            }
        });

        if (!problems.isEmpty()) {
            return Either.left(new FailedOperation(problems));
        }

        logger.info(String.format(
                "Removed permissions of %s on the Azure resource %s", entityId, databricksWorkspaceInfo.getName()));

        return right(null);
    }

    private Either<FailedOperation, Void> assignPermissionsToEntity(
            DatabricksWorkspaceInfo databricksWorkspaceInfo,
            String entityId,
            String roleDefinitionId,
            PrincipalType principalType) {

        String resourceId = String.format(
                RESOURCE_ID_FORMAT,
                azurePermissionsConfig.getSubscriptionId(),
                azurePermissionsConfig.getResourceGroup(),
                databricksWorkspaceInfo.getName());

        String permissionId = UUID.randomUUID().toString();
        return azurePermissionsManager.assignPermissions(
                resourceId, permissionId, roleDefinitionId, entityId, principalType);
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
            } else if (specific instanceof DatabricksWorkflowWorkloadSpecific) {
                workspaceName = ((DatabricksWorkflowWorkloadSpecific) specific).getWorkspace();
            } else {
                String errorMessage = String.format(
                        "The specific section of the component '%s' is not a valid type. Only the following types are accepted: DatabricksJobWorkloadSpecific, DatabricksDLTWorkloadSpecific, DatabricksOutputPortSpecific, DatabricksWorkflowWorkloadSpecific",
                        provisionRequest.component().getName());
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }

            if (workspaceName == null || workspaceName.isEmpty()) {
                String errorMessage = String.format(
                        "The provided specific section of the component '%s' doesn't contain a workspace name",
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
