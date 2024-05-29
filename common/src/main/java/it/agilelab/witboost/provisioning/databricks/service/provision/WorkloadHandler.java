package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;

import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.authorization.models.PrincipalType;
import com.databricks.sdk.WorkspaceClient;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.DatabricksWorkspaceClientBean;
import it.agilelab.witboost.provisioning.databricks.client.AzureWorkspaceManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
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
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitProvider;
import it.agilelab.witboost.provisioning.databricks.permissions.AzurePermissionsManager;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureMapper;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WorkloadHandler {

    private final Logger logger = LoggerFactory.getLogger(WorkloadHandler.class);

    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @Autowired
    private DatabricksAuthConfig databricksAuthConfig;

    @Autowired
    private AzureAuthConfig azureAuthConfig;

    @Autowired
    private GitCredentialsConfig gitCredentialsConfig;

    private RepoManager repoManager;

    private AzureMapper azureMapper;

    private AzureResourceManager azureResourceManager;
    private AzurePermissionsManager azurePermissionsManager;

    @Autowired
    private final AzureWorkspaceManager azureWorkspaceManager;

    @Autowired
    private final DatabricksWorkspaceClientBean databricksWorkspaceClientBean;

    public WorkloadHandler(
            AzureWorkspaceManager azureWorkspaceManager,
            AzurePermissionsConfig azurePermissionsConfig,
            GitCredentialsConfig gitCredentialsConfig,
            DatabricksAuthConfig databricksAuthConfig,
            AzureAuthConfig azureAuthConfig,
            DatabricksWorkspaceClientBean databricksWorkspaceClientBean,
            AzureMapper azureMapper,
            AzurePermissionsManager azurePermissionsManager,
            AzureResourceManager azureResourceManager) {
        this.azureWorkspaceManager = azureWorkspaceManager;
        this.azurePermissionsConfig = azurePermissionsConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
        this.databricksAuthConfig = databricksAuthConfig;
        this.azureAuthConfig = azureAuthConfig;
        this.databricksWorkspaceClientBean = databricksWorkspaceClientBean;
        this.azureMapper = azureMapper;
        this.azurePermissionsManager = azurePermissionsManager;
        this.azureResourceManager = azureResourceManager;
    }

    public <T extends Specific> Either<FailedOperation, String> createNewWorkspaceWithPermissions(
            ProvisionRequest<T> provisionRequest) {

        try {

            String workspaceName = getWorkspaceName(provisionRequest)
                    .getOrElseThrow(() -> new ProvisioningException("Failed to get workspace name"));

            String managedResourceGroupId = String.format(
                    "/subscriptions/%s/resourceGroups/%s-rg",
                    azurePermissionsConfig.getSubscriptionId(), workspaceName);

            Either<FailedOperation, DatabricksWorkspaceInfo> eitherNewWorkspace = azureWorkspaceManager.createWorkspace(
                    workspaceName,
                    "westeurope",
                    azurePermissionsConfig.getResourceGroup(),
                    managedResourceGroupId,
                    SkuType.TRIAL);

            if (eitherNewWorkspace.isLeft()) {
                return left(eitherNewWorkspace.getLeft());
            }

            DatabricksWorkspaceInfo databricksWorkspaceInfo = eitherNewWorkspace.get();

            Either<FailedOperation, Void> eitherAssignPermissions =
                    assignPermissions(provisionRequest, databricksWorkspaceInfo);
            if (eitherAssignPermissions.isLeft()) {
                return left(eitherAssignPermissions.getLeft());
            }

            Either<FailedOperation, Void> eitherCreatedRepo =
                    createRepository(provisionRequest, databricksWorkspaceInfo);
            if (eitherCreatedRepo.isLeft()) {
                return left(eitherCreatedRepo.getLeft());
            }

            logger.info(String.format("New workspace created available at: %s", databricksWorkspaceInfo.getUrl()));
            return Either.right(databricksWorkspaceInfo.getUrl());

        } catch (Exception e) {
            logger.error("Failed to create workspace", e);
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }

    private <T extends Specific> Either<FailedOperation, String> getWorkspaceName(
            ProvisionRequest<T> provisionRequest) {
        try {
            DatabricksWorkloadSpecific databricksWorkloadSpecific =
                    (DatabricksWorkloadSpecific) provisionRequest.component().getSpecific();

            return Either.right(databricksWorkloadSpecific.getWorkspace());

        } catch (ClassCastException | NullPointerException e) {
            String errorMessage = String.format(
                    "The specific section of the component %s is not of type DatabricksWorkloadSpecific",
                    provisionRequest.component().getId());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
    }

    private <T extends Specific> Either<FailedOperation, Void> createRepository(
            ProvisionRequest<T> provisionRequest, DatabricksWorkspaceInfo workspaceInfo) {
        try {
            DatabricksWorkloadSpecific databricksWorkloadSpecific =
                    (DatabricksWorkloadSpecific) provisionRequest.component().getSpecific();

            String gitRepo = databricksWorkloadSpecific.getGit().getGitRepoUrl();

            databricksWorkspaceClientBean.setWorkspaceHost(workspaceInfo.getUrl());
            WorkspaceClient workspaceClient = databricksWorkspaceClientBean.getObject();

            repoManager = new RepoManager(workspaceClient);
            return repoManager.createRepo(gitRepo, GitProvider.GITLAB);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }

    private <T extends Specific> Either<FailedOperation, Void> assignPermissions(
            ProvisionRequest<T> provisionRequest, DatabricksWorkspaceInfo databricksWorkspaceInfo) {

        logger.info(String.format("Assigning permissions to workspace %s", databricksWorkspaceInfo.getId()));

        // TODO: Assign permissions to the group!? Now are assigned to the DP owner

        String mail = provisionRequest.dataProduct().getDataProductOwner();
        Set<String> inputUser = Set.of(mail);

        Map<String, Either<Throwable, String>> res = azureMapper.map(inputUser);
        Either<Throwable, String> userMap = res.get(mail);

        if (userMap.isLeft()) {
            return left(new FailedOperation(Collections.singletonList(
                    new Problem("Failed to map user: " + userMap.getLeft().getMessage()))));
        }

        String resourceId = String.format(
                "/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Databricks/workspaces/%s",
                azurePermissionsConfig.getSubscriptionId(),
                azurePermissionsConfig.getResourceGroup(),
                databricksWorkspaceInfo.getName());

        try {
            Either<FailedOperation, Void> eitherAssignPermissions = azurePermissionsManager.assignPermissions(
                    resourceId,
                    UUID.randomUUID().toString(),
                    azurePermissionsConfig.getRoleDefinitionId(),
                    userMap.get(),
                    PrincipalType.USER);

            return eitherAssignPermissions;
        } catch (Exception e) {
            logger.error("Failed to assign permissions", e);
            return left(new FailedOperation(
                    Collections.singletonList(new Problem("Failed to assign permissions: " + e.getMessage()))));
        }
    }
}
