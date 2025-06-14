package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.iam.ServicePrincipal;
import com.databricks.sdk.service.jobs.BaseJob;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.WorkspaceClientConfig;
import it.agilelab.witboost.provisioning.databricks.client.JobManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManagerFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksPermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.DatabricksJobWorkloadSpecific;
import java.util.*;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class JobWorkloadHandler extends BaseWorkloadHandler {

    private final Logger logger = LoggerFactory.getLogger(JobWorkloadHandler.class);

    @Autowired
    public JobWorkloadHandler(
            AzureAuthConfig azureAuthConfig,
            GitCredentialsConfig gitCredentialsConfig,
            DatabricksPermissionsConfig databricksPermissionsConfig,
            AccountClient accountClient,
            WorkspaceLevelManagerFactory workspaceLevelManagerFactory,
            Function<WorkspaceClientConfig.WorkspaceClientConfigParams, WorkspaceClient> workspaceClientFactory) {
        super(
                azureAuthConfig,
                gitCredentialsConfig,
                databricksPermissionsConfig,
                accountClient,
                workspaceLevelManagerFactory,
                workspaceClientFactory);
    }

    /**
     * Provisions a new Databricks job for the given component.
     *
     * @param provisionRequest the request containing the specifics for the job to be provisioned
     * @param workspaceClient the Databricks workspace client
     * @param databricksWorkspaceInfo information about the Databricks workspace
     * @return Either a failed operation or the ID of the provisioned job as a String
     */
    public Either<FailedOperation, String> provisionWorkload(
            ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {

        try {
            Either<FailedOperation, Map<String, String>> eitherPrincipalsMapping = mapPrincipals(provisionRequest);
            if (eitherPrincipalsMapping.isLeft()) return Either.left(eitherPrincipalsMapping.getLeft());

            if (eitherPrincipalsMapping.isLeft()) return Either.left(eitherPrincipalsMapping.getLeft());

            Map<String, String> principalsMapping = eitherPrincipalsMapping.get();
            String dpOwnerDatabricksId =
                    principalsMapping.get(provisionRequest.dataProduct().getDataProductOwner());

            // TODO: This is a temporary solution. Remove or update this logic in the future.
            String devGroup = provisionRequest.dataProduct().getDevGroup();

            if (!devGroup.startsWith("group:")) devGroup = "group:" + devGroup;

            String dpDevGroupDatabricksId = principalsMapping.get(devGroup);

            Either<FailedOperation, Void> eitherCreatedRepo = createRepositoryWithPermissions(
                    provisionRequest,
                    workspaceClient,
                    databricksWorkspaceInfo,
                    dpOwnerDatabricksId,
                    dpDevGroupDatabricksId);
            if (eitherCreatedRepo.isLeft()) return left(eitherCreatedRepo.getLeft());

            String runAsPrincipalName =
                    provisionRequest.component().getSpecific().getRunAsPrincipalName();
            if (runAsPrincipalName != null && !runAsPrincipalName.isBlank()) {
                var setGitCred = setServicePrincipalGitCredentials(
                        workspaceClient,
                        databricksWorkspaceInfo.getDatabricksHost(),
                        databricksWorkspaceInfo.getName(),
                        runAsPrincipalName);

                if (setGitCred.isLeft()) return left(setGitCred.getLeft());
            }

            Either<FailedOperation, Long> eitherCreatedJob =
                    createJob(provisionRequest, workspaceClient, databricksWorkspaceInfo.getName());
            if (eitherCreatedJob.isLeft()) return left(eitherCreatedJob.getLeft());

            logger.info(String.format("Workspace available at: %s", databricksWorkspaceInfo.getDatabricksHost()));

            String jobUrl =
                    "https://" + databricksWorkspaceInfo.getDatabricksHost() + "/jobs/" + eitherCreatedJob.get();
            logger.info(String.format(
                    "New job linked to component %s available at %s",
                    provisionRequest.component().getName(), jobUrl));

            return right(eitherCreatedJob.get().toString());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while provisioning component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Unprovisions a previously provisioned Databricks job, deleting the associated job and repository if requested.
     *
     * @param provisionRequest the request containing the specifics for the job to be unprovisioned
     * @param workspaceClient the Databricks workspace client
     * @param databricksWorkspaceInfo information about the Databricks workspace
     * @return Either a failed operation or void if successful
     */
    public Either<FailedOperation, Void> unprovisionWorkload(
            ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {
        try {
            DatabricksJobWorkloadSpecific databricksJobWorkloadSpecific =
                    provisionRequest.component().getSpecific();

            var jobManager = new JobManager(workspaceClient, databricksWorkspaceInfo.getName());

            Either<FailedOperation, Iterable<BaseJob>> eitherGetJobs =
                    jobManager.listJobsWithGivenName(databricksJobWorkloadSpecific.getJobName());
            if (eitherGetJobs.isLeft()) return left(eitherGetJobs.getLeft());

            Iterable<BaseJob> jobs = eitherGetJobs.get();
            List<Problem> problems = new ArrayList<>();

            jobs.forEach(job -> {
                Either<FailedOperation, Void> result = jobManager.deleteJob(job.getJobId());
                if (result.isLeft()) problems.addAll(result.getLeft().problems());
            });

            if (!problems.isEmpty()) return left(new FailedOperation(problems));

            if (provisionRequest.removeData()) {
                var repoManager = new RepoManager(workspaceClient, databricksWorkspaceInfo.getName());

                String repoPath = databricksJobWorkloadSpecific.getRepoPath();
                if (!repoPath.startsWith("/")) repoPath = "/" + repoPath;

                Either<FailedOperation, Void> eitherDeletedRepo = repoManager.deleteRepo(
                        provisionRequest.component().getSpecific().getGit().getGitRepoUrl(), repoPath);
                if (eitherDeletedRepo.isLeft()) return left(eitherDeletedRepo.getLeft());

            } else
                logger.info(
                        "The repository with URL '{}' associated with component '{}' will not be removed from the Workspace because the 'removeData' flag is set to 'false'. Provision request details: [Component: {}, Repo URL: {}, Remove Data: {}].",
                        provisionRequest.component().getSpecific().getGit().getGitRepoUrl(),
                        provisionRequest.component().getName(),
                        provisionRequest.component().getName(),
                        provisionRequest.component().getSpecific().getGit().getGitRepoUrl(),
                        provisionRequest.removeData());

            return right(null);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while unprovisioning component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Creates a new job in the Databricks workspace.
     *
     * @param provisionRequest the request containing the specifics for the job to be created
     * @param workspaceClient the Databricks workspace client
     * @param workspaceName the name of the Databricks workspace
     * @return Either a failed operation or the ID of the created job as a Long
     */
    private Either<FailedOperation, Long> createJob(
            ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            String workspaceName) {
        try {
            DatabricksJobWorkloadSpecific databricksJobWorkloadSpecific =
                    provisionRequest.component().getSpecific();

            var jobManager = new JobManager(workspaceClient, workspaceName);

            String runAsPrincipalApplicationID = null;

            if (databricksJobWorkloadSpecific.getRunAsPrincipalName() != null
                    && !databricksJobWorkloadSpecific.getRunAsPrincipalName().isBlank()) {

                WorkspaceLevelManager workspaceLevelManager =
                        workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(workspaceClient);

                Either<FailedOperation, ServicePrincipal> eitherRunAsPrincipal =
                        workspaceLevelManager.getServicePrincipalFromName(
                                databricksJobWorkloadSpecific.getRunAsPrincipalName());
                if (eitherRunAsPrincipal.isLeft()) return left(eitherRunAsPrincipal.getLeft());
                runAsPrincipalApplicationID = eitherRunAsPrincipal.get().getApplicationId();
            }

            return jobManager.createOrUpdateJobWithNewCluster(
                    databricksJobWorkloadSpecific.getJobName(),
                    databricksJobWorkloadSpecific.getDescription(),
                    "Task1",
                    runAsPrincipalApplicationID,
                    databricksJobWorkloadSpecific.getCluster(),
                    databricksJobWorkloadSpecific.getScheduling(),
                    databricksJobWorkloadSpecific.getGit(),
                    provisionRequest.dataProduct().getEnvironment());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating the new Databricks job for component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
