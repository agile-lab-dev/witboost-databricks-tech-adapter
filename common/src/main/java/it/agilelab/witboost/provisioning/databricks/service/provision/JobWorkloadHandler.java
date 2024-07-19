package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.BaseJob;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.JobManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.DatabricksJobWorkloadSpecific;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class JobWorkloadHandler {

    private final Logger logger = LoggerFactory.getLogger(JobWorkloadHandler.class);

    private final AzureAuthConfig azureAuthConfig;
    private final GitCredentialsConfig gitCredentialsConfig;

    @Autowired
    public JobWorkloadHandler(AzureAuthConfig azureAuthConfig, GitCredentialsConfig gitCredentialsConfig) {
        this.azureAuthConfig = azureAuthConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
    }

    public Either<FailedOperation, String> provisionWorkload(
            ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {

        try {

            Either<FailedOperation, Void> eitherCreatedRepo =
                    createRepository(provisionRequest, workspaceClient, databricksWorkspaceInfo.getName());
            if (eitherCreatedRepo.isLeft()) {
                return left(eitherCreatedRepo.getLeft());
            }

            Either<FailedOperation, Long> eitherCreatedJob =
                    createJob(provisionRequest, workspaceClient, databricksWorkspaceInfo.getName());
            if (eitherCreatedJob.isLeft()) {
                return left(eitherCreatedJob.getLeft());
            }

            logger.info(String.format(
                    "New workspace created available at: %s", databricksWorkspaceInfo.getDatabricksHost()));
            return right(eitherCreatedJob.get().toString());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while provisioning component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

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
                if (result.isLeft()) {
                    problems.addAll(result.getLeft().problems());
                }
            });

            if (!problems.isEmpty()) {
                return Either.left(new FailedOperation(problems));
            }

            if (provisionRequest.removeData()) {
                var repoManager = new RepoManager(workspaceClient, databricksWorkspaceInfo.getName());

                Either<FailedOperation, Void> eitherDeletedRepo = repoManager.deleteRepo(
                        (provisionRequest.component().getSpecific()).getGit().getGitRepoUrl(),
                        azureAuthConfig.getClientId());

                if (eitherDeletedRepo.isLeft()) return left(eitherDeletedRepo.getLeft());

            } else {
                String repoUrl =
                        (provisionRequest.component().getSpecific()).getGit().getGitRepoUrl();

                logger.info(
                        "The repo with URL {} will not be removed from the Workspace because removeData is set to ‘false’.",
                        repoUrl);
            }

            return right(null);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while unprovisioning component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private Either<FailedOperation, Void> createRepository(
            ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            String workspaceName) {
        try {
            DatabricksJobWorkloadSpecific databricksJobWorkloadSpecific =
                    provisionRequest.component().getSpecific();

            String gitRepo = databricksJobWorkloadSpecific.getGit().getGitRepoUrl();

            var repoManager = new RepoManager(workspaceClient, workspaceName);
            return repoManager.createRepo(gitRepo, gitCredentialsConfig.getProvider());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating the new Databricks repo for component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private Either<FailedOperation, Long> createJob(
            ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            String workspaceName) {
        try {
            DatabricksJobWorkloadSpecific databricksJobWorkloadSpecific =
                    provisionRequest.component().getSpecific();

            var jobManager = new JobManager(workspaceClient, workspaceName);

            return jobManager.createJobWithNewCluster(
                    databricksJobWorkloadSpecific.getJobName(),
                    databricksJobWorkloadSpecific.getDescription(),
                    UUID.randomUUID().toString(),
                    databricksJobWorkloadSpecific.getCluster(),
                    databricksJobWorkloadSpecific.getScheduling(),
                    databricksJobWorkloadSpecific.getGit());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating the new Databricks job for component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
