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
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WorkloadHandler {

    private final Logger logger = LoggerFactory.getLogger(WorkloadHandler.class);

    private AzureAuthConfig azureAuthConfig;

    @Autowired
    public WorkloadHandler(AzureAuthConfig azureAuthConfig) {
        this.azureAuthConfig = azureAuthConfig;
    }

    public <T extends Specific> Either<FailedOperation, String> provisionWorkload(
            ProvisionRequest<T> provisionRequest,
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
            logger.error("Failed to create workspace", e);
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }

    public <T extends Specific> Either<FailedOperation, Void> unprovisionWorkload(
            ProvisionRequest<T> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {
        try {
            DatabricksWorkloadSpecific databricksWorkloadSpecific =
                    (DatabricksWorkloadSpecific) provisionRequest.component().getSpecific();

            var jobManager = new JobManager(workspaceClient, databricksWorkspaceInfo.getName());

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
                var repoManager = new RepoManager(workspaceClient, databricksWorkspaceInfo.getName());

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
}
