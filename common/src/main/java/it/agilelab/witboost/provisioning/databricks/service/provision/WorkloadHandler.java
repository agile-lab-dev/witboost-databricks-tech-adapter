package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.authorization.models.PrincipalType;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.BaseJob;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.DatabricksWorkspaceClientBean;
import it.agilelab.witboost.provisioning.databricks.client.AzureWorkspaceManager;
import it.agilelab.witboost.provisioning.databricks.client.JobManager;
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
import it.agilelab.witboost.provisioning.databricks.model.databricks.*;
import it.agilelab.witboost.provisioning.databricks.permissions.AzurePermissionsManager;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureMapper;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WorkloadHandler {

    private final Logger logger = LoggerFactory.getLogger(WorkloadHandler.class);
    private AzurePermissionsConfig azurePermissionsConfig;
    private DatabricksAuthConfig databricksAuthConfig;
    private AzureAuthConfig azureAuthConfig;
    private GitCredentialsConfig gitCredentialsConfig;
    private AzureMapper azureMapper;
    private AzureResourceManager azureResourceManager;
    private AzurePermissionsManager azurePermissionsManager;
    private final AzureWorkspaceManager azureWorkspaceManager;
    private final DatabricksWorkspaceClientBean databricksWorkspaceClientBean;

    @Autowired
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

    public <T extends Specific> Either<FailedOperation, String> provisionWorkload(
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
            WorkspaceClient workspaceClient =
                    databricksWorkspaceClientBean.getObject(databricksWorkspaceInfo.getUrl(), workspaceName);

            Either<FailedOperation, Void> eitherAssignPermissions =
                    assignAzurePermissions(provisionRequest, databricksWorkspaceInfo);
            if (eitherAssignPermissions.isLeft()) {
                return left(eitherAssignPermissions.getLeft());
            }

            Either<FailedOperation, Void> eitherCreatedRepo =
                    createRepository(provisionRequest, workspaceClient, workspaceName);
            if (eitherCreatedRepo.isLeft()) {
                return left(eitherCreatedRepo.getLeft());
            }

            Either<FailedOperation, Long> eitherCreatedJob =
                    createJob(provisionRequest, workspaceClient, workspaceName);
            if (eitherCreatedJob.isLeft()) {
                return left(eitherCreatedJob.getLeft());
            }

            logger.info(String.format("New workspace created available at: %s", databricksWorkspaceInfo.getUrl()));
            return right(databricksWorkspaceInfo.getUrl());

        } catch (Exception e) {
            logger.error("Failed to create workspace", e);
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }

    public <T extends Specific> Either<FailedOperation, Void> unprovisionWorkload(
            ProvisionRequest<T> provisionRequest) {
        try {
            DatabricksWorkloadSpecific databricksWorkloadSpecific =
                    (DatabricksWorkloadSpecific) provisionRequest.component().getSpecific();

            String workspaceName = getWorkspaceName(provisionRequest)
                    .getOrElseThrow(() -> new ProvisioningException("Failed to get workspace name"));

            String managedResourceGroupId = String.format(
                    "/subscriptions/%s/resourceGroups/%s-rg",
                    azurePermissionsConfig.getSubscriptionId(), workspaceName);

            Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherGetWorkspace =
                    azureWorkspaceManager.getWorkspace(workspaceName, managedResourceGroupId);

            if (eitherGetWorkspace.isLeft()) return left(eitherGetWorkspace.getLeft());

            Optional<DatabricksWorkspaceInfo> optionalDatabricksWorkspaceInfo = eitherGetWorkspace.get();
            if (optionalDatabricksWorkspaceInfo.isEmpty())
                return left(new FailedOperation(Collections.singletonList(
                        new Problem(String.format("Workspace %s does not exists", workspaceName)))));

            DatabricksWorkspaceInfo workspaceInfo = optionalDatabricksWorkspaceInfo.get();

            WorkspaceClient workspaceClient =
                    databricksWorkspaceClientBean.getObject(workspaceInfo.getUrl(), workspaceName);

            var jobManager = new JobManager(workspaceClient, workspaceName);

            Either<FailedOperation, Iterable<BaseJob>> eitherGetJobs =
                    jobManager.listJobsWithGivenName(databricksWorkloadSpecific.getJobName());

            if (eitherGetJobs.isLeft()) return left(eitherGetJobs.getLeft());

            Iterable<BaseJob> jobs = eitherGetJobs.get();
            List<FailedOperation> errors = new ArrayList<>();

            jobs.forEach(job -> {
                Either<FailedOperation, Void> result = jobManager.deleteJob(job.getJobId());
                if (result.isLeft()) {
                    errors.add(result.getLeft());
                }
            });

            if (!errors.isEmpty()) {
                return Either.left(errors.get(0));
            }

            if (provisionRequest.removeData()) {
                var repoManager = new RepoManager(workspaceClient, workspaceName);

                Either<FailedOperation, Void> eitherDeletedRepo = repoManager.deleteRepo(
                        ((DatabricksWorkloadSpecific)
                                        provisionRequest.component().getSpecific())
                                .getGit()
                                .getGitRepoUrl(),
                        azureAuthConfig.getClientId());

                if (eitherDeletedRepo.isLeft()) return left(eitherDeletedRepo.getLeft());

            } else {
                String repoUrl = ((DatabricksWorkloadSpecific)
                                provisionRequest.component().getSpecific())
                        .getGit()
                        .getGitRepoUrl();

                logger.info(
                        "The repo with URL {} will not be removed from the Workspace because removeData is set to ‘false’.",
                        repoUrl);
            }

            return right(null);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
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
            ProvisionRequest<T> provisionRequest, WorkspaceClient workspaceClient, String workspaceName) {
        try {
            DatabricksWorkloadSpecific databricksWorkloadSpecific =
                    (DatabricksWorkloadSpecific) provisionRequest.component().getSpecific();

            String gitRepo = databricksWorkloadSpecific.getGit().getGitRepoUrl();

            var repoManager = new RepoManager(workspaceClient, workspaceName);
            return repoManager.createRepo(gitRepo, GitProvider.GITLAB);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }

    private <T extends Specific> Either<FailedOperation, Long> createJob(
            ProvisionRequest<T> provisionRequest, WorkspaceClient workspaceClient, String workspaceName) {
        try {
            DatabricksWorkloadSpecific databricksWorkloadSpecific =
                    (DatabricksWorkloadSpecific) provisionRequest.component().getSpecific();

            var jobManager = new JobManager(workspaceClient, workspaceName);

            Either<FailedOperation, Long> j = jobManager.createJobWithNewCluster(
                    databricksWorkloadSpecific.getJobName(),
                    databricksWorkloadSpecific.getDescription(),
                    UUID.randomUUID().toString(),
                    databricksWorkloadSpecific.getCluster(),
                    databricksWorkloadSpecific.getScheduling(),
                    databricksWorkloadSpecific.getGit());

            return j;

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }

    private <T extends Specific> Either<FailedOperation, Void> assignAzurePermissions(
            ProvisionRequest<T> provisionRequest, DatabricksWorkspaceInfo databricksWorkspaceInfo) {
        try {
            logger.info("Assigning Azure permissions to {}", databricksWorkspaceInfo.getName());

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

            return azurePermissionsManager.assignPermissions(
                    resourceId,
                    UUID.randomUUID().toString(),
                    azurePermissionsConfig.getRoleDefinitionId(),
                    userMap.get(),
                    PrincipalType.USER);

        } catch (Exception e) {
            logger.error(
                    "Failed to assign permissions to {} at {} ",
                    provisionRequest.dataProduct().getDataProductOwner(),
                    databricksWorkspaceInfo.getName(),
                    e);
            return left(new FailedOperation(
                    Collections.singletonList(new Problem("Failed to assign permissions: " + e.getMessage()))));
        }
    }
}
