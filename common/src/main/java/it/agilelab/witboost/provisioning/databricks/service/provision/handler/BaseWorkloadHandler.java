package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

import static io.vavr.control.Either.left;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.workspace.RepoPermissionLevel;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.IdentityManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManagerFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksPermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.DatabricksJobWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.databricks.DatabricksMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class BaseWorkloadHandler {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final AzureAuthConfig azureAuthConfig;
    protected final GitCredentialsConfig gitCredentialsConfig;
    protected final AccountClient accountClient;
    protected final WorkspaceLevelManagerFactory workspaceLevelManagerFactory;
    protected final DatabricksPermissionsConfig databricksPermissionsConfig;

    @Autowired
    public BaseWorkloadHandler(
            AzureAuthConfig azureAuthConfig,
            GitCredentialsConfig gitCredentialsConfig,
            DatabricksPermissionsConfig databricksPermissionsConfig,
            AccountClient accountClient,
            WorkspaceLevelManagerFactory workspaceLevelManagerFactory) {
        this.azureAuthConfig = azureAuthConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
        this.accountClient = accountClient;
        this.databricksPermissionsConfig = databricksPermissionsConfig;
        this.workspaceLevelManagerFactory = workspaceLevelManagerFactory;
    }

    /**
     * Creates a repository in the Databricks workspace and assigns permissions to the repository.
     *
     * This method performs the following operations:
     * 1. Extracts the Git repository URL and repository path from the provision request.
     * 2. Creates the necessary directory structure in the Databricks workspace.
     * 3. Creates the repository in the specified path using the provided Git repository URL.
     * 4. Uses the IdentityManager to create or update the owner and developer group.
     * 5. Assigns or removes permissions to the repository for the owner and the developer group based on configuration settings.
     *
     * @param provisionRequest the request containing the details for creating the repository, including the Git repository URL and the repository path.
     * @param workspaceClient the Databricks workspace client used to interact with the workspace.
     * @param databricksWorkspaceInfo information about the Databricks workspace, including its name.
     * @param ownerName the name of the owner to whom specific permissions will be assigned.
     * @param developerGroupName the name of the developer group to whom specific permissions will be assigned.
     * @return Either a FailedOperation or Void if the operation is successful. If the operation fails, the method returns a FailedOperation with detailed error information.
     */
    protected synchronized Either<FailedOperation, Void> createRepositoryWithPermissions(
            ProvisionRequest<? extends Specific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo,
            String ownerName,
            String developerGroupName) {
        try {
            String gitRepo = null;
            String repoPath = null;

            Specific specific = provisionRequest.component().getSpecific();
            if (specific instanceof DatabricksDLTWorkloadSpecific) {
                DatabricksDLTWorkloadSpecific dltSpecific = (DatabricksDLTWorkloadSpecific) specific;
                gitRepo = dltSpecific.getGit().getGitRepoUrl();
                repoPath = dltSpecific.getRepoPath();
            } else if (specific instanceof DatabricksJobWorkloadSpecific) {
                DatabricksJobWorkloadSpecific jobSpecific = (DatabricksJobWorkloadSpecific) specific;
                gitRepo = jobSpecific.getGit().getGitRepoUrl();
                repoPath = jobSpecific.getRepoPath();
            } else if (specific instanceof DatabricksWorkflowWorkloadSpecific) {
                DatabricksWorkflowWorkloadSpecific workflowSpecific = (DatabricksWorkflowWorkloadSpecific) specific;
                gitRepo = workflowSpecific.getGit().getGitRepoUrl();
                repoPath = workflowSpecific.getRepoPath();
            }

            int lastIndex = repoPath.lastIndexOf('/');
            String folderPath = repoPath.substring(0, lastIndex);
            workspaceClient.workspace().mkdirs(String.format("/%s", folderPath));

            repoPath = String.format("/%s", repoPath);

            logger.info(
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
                logger.info(
                        "Adding project owner '{}' and development group '{}' to workspace '{}' as admins",
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
                logger.info(
                        "Skipping upsert of project owner and development group to workspace since workspace is not set to be managed by the Tech Adapter.");

            String repoId = eitherCreatedRepo.get().toString();
            String ownerPermissionLevelConfig =
                    databricksPermissionsConfig.getWorkload().getOwner();

            Either<FailedOperation, Void> eitherPermissionsToDPOwner;

            logger.info("Updating permissions to project owner '{}' for repository with id '{}'", ownerName, repoId);
            if (ownerPermissionLevelConfig.equalsIgnoreCase("NO_PERMISSIONS")) {
                logger.info(
                        "Configuration set as '{}', removing all permissions on repository '{}' for owner '{}'",
                        ownerPermissionLevelConfig,
                        repoId,
                        ownerName);
                eitherPermissionsToDPOwner = repoManager.removePermissionsToUser(repoId, ownerName);
            } else {
                RepoPermissionLevel ownerPermissionLevel = RepoPermissionLevel.valueOf(ownerPermissionLevelConfig);
                logger.info(
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

            logger.info(
                    "Updating permissions to development group '{}' for repository with id '{}'",
                    developerGroupName,
                    repoId);
            if (devGroupPermissionLevelConfig.equalsIgnoreCase("NO_PERMISSIONS")) {
                logger.info(
                        "Configuration set as '{}', removing all permissions on repository '{}' for development group '{}'",
                        ownerPermissionLevelConfig,
                        repoId,
                        developerGroupName);
                eitherPermissionsToDevelopers = repoManager.removePermissionsToGroup(repoId, developerGroupName);
            } else {
                RepoPermissionLevel devGroupPermissionLevel =
                        RepoPermissionLevel.valueOf(devGroupPermissionLevelConfig);
                logger.info(
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
            String gitRepo = null;

            Specific specific = provisionRequest.component().getSpecific();

            if (specific instanceof DatabricksDLTWorkloadSpecific) {
                DatabricksDLTWorkloadSpecific dltSpecific = (DatabricksDLTWorkloadSpecific) specific;
                gitRepo = dltSpecific.getGit().getGitRepoUrl();
            } else if (specific instanceof DatabricksJobWorkloadSpecific) {
                DatabricksJobWorkloadSpecific jobSpecific = (DatabricksJobWorkloadSpecific) specific;
                gitRepo = jobSpecific.getGit().getGitRepoUrl();
            } else if (specific instanceof DatabricksWorkflowWorkloadSpecific) {
                DatabricksWorkflowWorkloadSpecific workflowSpecific = (DatabricksWorkflowWorkloadSpecific) specific;
                gitRepo = workflowSpecific.getGit().getGitRepoUrl();
            }

            String errorMessage = String.format(
                    "An error occurred while creating repository %s in %s for component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    gitRepo,
                    databricksWorkspaceInfo.getName(),
                    provisionRequest.component().getName(),
                    e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

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
                    return Either.left(
                            new FailedOperation(Collections.singletonList(new Problem(error.getMessage(), error))));
                }
                result.put(entry.getKey(), entry.getValue().get());
            }
            return Either.right(result);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while mapping dpOwner and devGroup for component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return Either.left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
