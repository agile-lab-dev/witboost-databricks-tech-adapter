package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.iam.ServicePrincipal;
import com.databricks.sdk.service.workspace.RepoPermissionLevel;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.WorkspaceClientConfig;
import it.agilelab.witboost.provisioning.databricks.client.IdentityManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManagerFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksPermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.DatabricksWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.databricks.DatabricksMapper;
import java.util.*;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
@Slf4j
public class BaseWorkloadHandler {

    protected final AzureAuthConfig azureAuthConfig;
    protected final GitCredentialsConfig gitCredentialsConfig;
    protected final DatabricksPermissionsConfig databricksPermissionsConfig;
    protected final AccountClient accountClient;
    protected final WorkspaceLevelManagerFactory workspaceLevelManagerFactory;
    protected final Function<WorkspaceClientConfig.WorkspaceClientConfigParams, WorkspaceClient> workspaceClientFactory;

    /**
     * Creates a repository in a Databricks workspace and assigns appropriate permissions to
     * users and groups if the workspace is managed. This method also ensures that proper
     * directories are created, Git credentials are configured, and user/group permissions
     * are applied or removed based on the configuration.
     *
     * @param provisionRequest the provisioning request containing component-specific configuration
     * @param workspaceClient the client for interacting with Databricks workspace APIs
     * @param databricksWorkspaceInfo details of the Databricks workspace where the repository is to be created
     * @param ownerName the name of the user who will be assigned owner permissions for the repository
     * @param developerGroupName the name of the group that will be assigned developer permissions for the repository
     * @return an Either containing a FailedOperation if the process fails, or Void if the operation is successful
     */
    protected synchronized Either<FailedOperation, Void> createRepositoryWithPermissions(
            ProvisionRequest<? extends Specific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo,
            String ownerName,
            String developerGroupName) {
        try {
            DatabricksWorkloadSpecific databricksWorkloadSpecific =
                    (DatabricksWorkloadSpecific) provisionRequest.component().getSpecific();
            String gitRepo = databricksWorkloadSpecific.getGit().getGitRepoUrl();
            String repoPath = databricksWorkloadSpecific.getRepoPath();

            WorkspaceLevelManager workspaceLevelManager =
                    workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(workspaceClient);

            Either<FailedOperation, Void> eitherSetGitCredentials =
                    workspaceLevelManager.setGitCredentials(workspaceClient, gitCredentialsConfig);
            if (eitherSetGitCredentials.isLeft()) return left(eitherSetGitCredentials.getLeft());

            int lastIndex = repoPath.lastIndexOf('/');
            String folderPath = repoPath.substring(0, lastIndex);
            workspaceClient.workspace().mkdirs(String.format("/%s", folderPath));

            repoPath = String.format("/%s", repoPath);

            log.info(
                    "Creating repository in workspace '{}' synchronizing git repository '{}{}'",
                    databricksWorkspaceInfo.getName(),
                    gitRepo,
                    repoPath);
            var repoManager = new RepoManager(workspaceClient, databricksWorkspaceInfo.getName());
            Either<FailedOperation, Long> eitherCreatedRepo =
                    repoManager.createRepo(gitRepo, gitCredentialsConfig.getProvider(), repoPath);
            if (eitherCreatedRepo.isLeft()) return left(eitherCreatedRepo.getLeft());

            // If the workspace is set to not be managed by the tech adapter, we don't add the users and groups to the
            // workspace
            if (databricksWorkspaceInfo.isManaged()) {
                log.info(
                        "Adding project owner '{}' and development group '{}' to workspace '{}' as workspace admins",
                        ownerName,
                        developerGroupName,
                        databricksWorkspaceInfo.getName());
                IdentityManager identityManager = new IdentityManager(accountClient, databricksWorkspaceInfo);
                Either<FailedOperation, Void> eitherUpdateUser =
                        identityManager.createOrUpdateUserWithAdminPrivileges(ownerName);
                if (eitherUpdateUser.isLeft()) return eitherUpdateUser;

                Either<FailedOperation, Void> eitherUpdateGroup =
                        identityManager.createOrUpdateGroupWithUserPrivileges(developerGroupName);
                if (eitherUpdateGroup.isLeft()) return eitherUpdateGroup;
            } else
                log.info(
                        "Skipping upsert of project owner and development group to workspace since workspace is not set to be managed by the Tech Adapter.");

            String repoId = eitherCreatedRepo.get().toString();
            String ownerPermissionLevelConfig =
                    databricksPermissionsConfig.getWorkload().getOwner();

            Either<FailedOperation, Void> eitherPermissionsToDPOwner;

            log.info("Updating permissions to project owner '{}' for repository with id '{}'", ownerName, repoId);
            if (ownerPermissionLevelConfig.equalsIgnoreCase("NO_PERMISSIONS")) {
                log.info(
                        "Configuration set as '{}', removing all permissions on repository '{}' for owner '{}'",
                        ownerPermissionLevelConfig,
                        repoId,
                        ownerName);
                eitherPermissionsToDPOwner = repoManager.removePermissionsToUser(repoId, ownerName);
            } else {
                RepoPermissionLevel ownerPermissionLevel = RepoPermissionLevel.valueOf(ownerPermissionLevelConfig);
                log.info(
                        "Assigning permission '{}' on repository '{}' for owner '{}'",
                        ownerPermissionLevelConfig,
                        repoId,
                        developerGroupName);
                eitherPermissionsToDPOwner =
                        repoManager.assignPermissionsToUser(repoId, ownerName, ownerPermissionLevel);
            }

            if (eitherPermissionsToDPOwner.isLeft()) {
                return left(eitherPermissionsToDPOwner.getLeft());
            }

            String devGroupPermissionLevelConfig =
                    databricksPermissionsConfig.getWorkload().getDeveloper();
            Either<FailedOperation, Void> eitherPermissionsToDevelopers;

            log.info(
                    "Updating permissions to development group '{}' for repository with id '{}'",
                    developerGroupName,
                    repoId);
            if (devGroupPermissionLevelConfig.equalsIgnoreCase("NO_PERMISSIONS")) {
                log.info(
                        "Configuration set as '{}', removing all permissions on repository '{}' for development group '{}'",
                        ownerPermissionLevelConfig,
                        repoId,
                        developerGroupName);
                eitherPermissionsToDevelopers = repoManager.removePermissionsToGroup(repoId, developerGroupName);
            } else {
                RepoPermissionLevel devGroupPermissionLevel =
                        RepoPermissionLevel.valueOf(devGroupPermissionLevelConfig);
                log.info(
                        "Assigning permission '{}' on repository '{}' for development group '{}'",
                        devGroupPermissionLevelConfig,
                        repoId,
                        developerGroupName);
                eitherPermissionsToDevelopers =
                        repoManager.assignPermissionsToGroup(repoId, developerGroupName, devGroupPermissionLevel);
            }

            if (eitherPermissionsToDevelopers.isLeft()) {
                return left(eitherPermissionsToDevelopers.getLeft());
            }

            return eitherPermissionsToDevelopers;

        } catch (Exception e) {

            DatabricksWorkloadSpecific databricksWorkloadSpecific =
                    (DatabricksWorkloadSpecific) provisionRequest.component().getSpecific();
            String gitRepo = databricksWorkloadSpecific.getGit().getGitRepoUrl();

            String errorMessage = String.format(
                    "An error occurred while creating repository %s in %s for component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    gitRepo,
                    databricksWorkspaceInfo.getName(),
                    provisionRequest.component().getName(),
                    e.getMessage());
            log.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Maps the principals (e.g., data product owner and development group) provided in the provision request
     * to representations recognized by Databricks. This process ensures that principals are formatted and validated
     * as expected for further operations within the Databricks environment.
     *
     * @param provisionRequest the provisioning request containing information about the data product and its associated properties
     * @return an Either instance containing:
     *         - a {@code FailedOperation} if an error occurs during the mapping process
     *         - a map of principal identifiers to their validated and formatted representations if the mapping is successful
     */
    protected Either<FailedOperation, Map<String, String>> mapPrincipals(ProvisionRequest<?> provisionRequest) {
        try {
            DatabricksMapper databricksMapper = new DatabricksMapper(accountClient);

            // TODO: This is a temporary solution. Remove or update this logic in the future.
            String devGroup = provisionRequest.dataProduct().getDevGroup();
            if (!devGroup.startsWith("group:")) devGroup = "group:" + devGroup;

            Map<String, Either<Throwable, String>> eitherMap =
                    databricksMapper.map(Set.of(provisionRequest.dataProduct().getDataProductOwner(), devGroup));

            Map<String, String> result = new HashMap<>();
            for (var entry : eitherMap.entrySet()) {
                if (entry.getValue().isLeft()) {
                    var error = entry.getValue().getLeft();
                    return left(new FailedOperation(Collections.singletonList(new Problem(error.getMessage(), error))));
                }
                result.put(entry.getKey(), entry.getValue().get());
            }
            return right(result);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while mapping dpOwner and devGroup for component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            log.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Creates and returns a WorkspaceClient for interacting with a Databricks workspace using a service principal.
     * If the client creation process encounters an error, a FailedOperation is returned with details of the issue.
     *
     * @param workspaceUrl the URL of the Databricks workspace
     * @param spClientId the client ID of the service principal
     * @param spClientSecret the client secret of the service principal
     * @param workspaceName the name of the workspace for which the client is being created
     * @return an Either instance containing:
     *         - a {@code WorkspaceClient} if the client is created successfully
     *         - a {@code FailedOperation} if an error occurs during the creation process
     */
    private Either<FailedOperation, WorkspaceClient> getServicePrincipalWorkspaceClient(
            String workspaceUrl, String spClientId, String spClientSecret, String workspaceName) {
        try {

            log.info("Creating workspace client for workspace '{}' for principal '{}' ", workspaceName, spClientId);

            WorkspaceClientConfig.WorkspaceClientConfigParams params =
                    new WorkspaceClientConfig.WorkspaceClientConfigParams(
                            WorkspaceClientConfig.WorkspaceClientConfigParams.AuthType.OAUTH,
                            spClientId,
                            spClientSecret,
                            workspaceUrl,
                            workspaceName);

            WorkspaceClient wsClient = workspaceClientFactory.apply(params);
            log.info("Successfully created workspace client for principal {}", spClientId);
            return right(wsClient);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An unexpected error occurred while creating workspace client for workspace '%s' for principal '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    workspaceName, spClientId, e.getMessage());
            log.error(errorMessage, e);
            return left(FailedOperation.singleProblemFailedOperation(errorMessage));
        }
    }

    /**
     * The method sets Git credentials for a service principal in a specified Databricks workspace.
     * NOTE: Currently, using the Databricks sdk, it is not possible to set Git credentials for a Service Principal
     * different from the microservice one, even with workspace admin privileges.
     * To address this, a temporary secret is generated and used to create a WorkspaceClient authenticated as the
     * target Service Principal. Git credentials are then set, and the secret is deleted right after the operation
     * (the secret is valid for at most 15 minutes).
     *
     * @param workspaceClient the WorkspaceClient instance, authenticated with the user or service principal configured for the microservice,
     *  *                        used for managing resources and operations in the Databricks workspace
     * @param workspaceHost the host URL of the Databricks workspace
     * @param workspaceName the display name of the target Databricks workspace where the Git credentials should be set
     * @param principalName the unique display name of the Service Principal for which the Git credentials are to be configured
     * @return an Either instance containing:
     *         - {@code Void} if the Git credentials are successfully set
     *         - {@code FailedOperation} if any part of the process fails
     */
    protected Either<FailedOperation, Void> setServicePrincipalGitCredentials(
            WorkspaceClient workspaceClient, String workspaceHost, String workspaceName, String principalName) {

        WorkspaceLevelManager workspaceLevelManager =
                workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(workspaceClient);

        log.info("Setting Git credentials for service principal {} in workspace {}", principalName, workspaceName);

        Either<FailedOperation, ServicePrincipal> principal =
                workspaceLevelManager.getServicePrincipalFromName(principalName);
        if (principal.isLeft()) return left(principal.getLeft());

        String principalID = principal.get().getId();
        String principalApplicationID = principal.get().getApplicationId();

        log.info("Generating temporary secret for principal {}", principalName);
        Either<FailedOperation, Map.Entry<String, String>> spSecret =
                workspaceLevelManager.generateSecretForServicePrincipal(Long.parseLong(principalID), "900s");
        if (spSecret.isLeft()) return left(spSecret.getLeft());

        Either<FailedOperation, WorkspaceClient> runAsWorkspaceClient = getServicePrincipalWorkspaceClient(
                workspaceHost, principalApplicationID, spSecret.get().getValue(), workspaceName);
        if (runAsWorkspaceClient.isLeft()) return left(runAsWorkspaceClient.getLeft());
        Either<FailedOperation, Void> setGitCredentials =
                workspaceLevelManager.setGitCredentials(runAsWorkspaceClient.get(), gitCredentialsConfig);
        if (setGitCredentials.isLeft()) return left(setGitCredentials.getLeft());

        log.info("Cleaning up temporary secret for principal {}", principalName);
        Either<FailedOperation, Void> deleteSpSecret = workspaceLevelManager.deleteServicePrincipalSecret(
                Long.parseLong(principalID), spSecret.get().getKey());
        if (deleteSpSecret.isLeft()) return left(deleteSpSecret.getLeft());

        log.info(
                "Successfully set Git credentials for service principal {} in workspace {}",
                principalName,
                workspaceName);

        return right(null);
    }
}
