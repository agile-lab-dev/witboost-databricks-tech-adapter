package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.workspace.RepoPermissionLevel;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.IdentityManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksPermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.DatabricksJobWorkloadSpecific;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BaseWorkloadHandler {

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());
    protected final AzureAuthConfig azureAuthConfig;
    protected final GitCredentialsConfig gitCredentialsConfig;
    protected final DatabricksPermissionsConfig databricksPermissionsConfig;
    protected final AccountClient accountClient;

    @Autowired
    public BaseWorkloadHandler(
            AzureAuthConfig azureAuthConfig,
            GitCredentialsConfig gitCredentialsConfig,
            DatabricksPermissionsConfig databricksPermissionsConfig,
            AccountClient accountClient) {
        this.azureAuthConfig = azureAuthConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
        this.databricksPermissionsConfig = databricksPermissionsConfig;
        this.accountClient = accountClient;
    }

    /**
     * Creates a repository in the Databricks workspace.
     *
     * @param provisionRequest the request containing the details for creating the repository
     * @param workspaceClient the Databricks workspace client
     * @param workspaceName the name of the Databricks workspace
     * @return Either a FailedOperation or Void if successful
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
            }
            int lastIndex = repoPath.lastIndexOf('/');
            String folderPath = repoPath.substring(0, lastIndex);
            workspaceClient
                    .workspace()
                    .mkdirs(String.format("/Users/%s/%s", azureAuthConfig.getClientId(), folderPath));

            repoPath = String.format("/Users/%s/%s", azureAuthConfig.getClientId(), repoPath);

            var repoManager = new RepoManager(workspaceClient, databricksWorkspaceInfo.getName());
            Either<FailedOperation, Long> eitherCreatedRepo =
                    repoManager.createRepo(gitRepo, gitCredentialsConfig.getProvider(), repoPath);
            if (eitherCreatedRepo.isLeft()) return left(eitherCreatedRepo.getLeft());

            IdentityManager identityManager = new IdentityManager(accountClient, databricksWorkspaceInfo);
            Either<FailedOperation, Void> eitherUpdateUser =
                    identityManager.createOrUpdateUserWithAdminPrivileges(ownerName);
            if (eitherUpdateUser.isLeft()) return eitherUpdateUser;

            Either<FailedOperation, Void> eitherUpdateGroup =
                    identityManager.createOrUpdateGroupWithUserPrivileges(developerGroupName);
            if (eitherUpdateGroup.isLeft()) return eitherUpdateGroup;

            String repoId = eitherCreatedRepo.get().toString();
            String ownerPermissionLevelConfig =
                    databricksPermissionsConfig.getWorkload().getOwner();

            Either<FailedOperation, Void> eitherPermissionsToDPOwner;

            if (ownerPermissionLevelConfig.equalsIgnoreCase("NO_PERMISSIONS")) {
                eitherPermissionsToDPOwner = repoManager.removePermissionsToUser(repoId, ownerName);
            } else {
                RepoPermissionLevel ownerPermissionLevel = RepoPermissionLevel.valueOf(ownerPermissionLevelConfig);
                eitherPermissionsToDPOwner =
                        repoManager.assignPermissionsToUser(repoId, ownerName, ownerPermissionLevel);
            }

            if (eitherPermissionsToDPOwner.isLeft()) {
                return left(eitherPermissionsToDPOwner.getLeft());
            }

            String devGroupPermissionLevelConfig =
                    databricksPermissionsConfig.getWorkload().getDeveloper();
            Either<FailedOperation, Void> eitherPermissionsToDevelopers;

            if (devGroupPermissionLevelConfig.equalsIgnoreCase("NO_PERMISSIONS")) {
                eitherPermissionsToDevelopers = repoManager.removePermissionsToGroup(repoId, developerGroupName);
            } else {
                RepoPermissionLevel devGroupPermissionLevel =
                        RepoPermissionLevel.valueOf(devGroupPermissionLevelConfig);
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
}
