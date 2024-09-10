package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.BaseJob;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.JobManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksPermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.DatabricksJobWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.databricks.DatabricksMapper;
import java.util.*;
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
            AccountClient accountClient) {
        super(azureAuthConfig, gitCredentialsConfig, databricksPermissionsConfig, accountClient);
    }

    public Either<FailedOperation, String> provisionWorkload(
            ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {

        try {

            DatabricksMapper databricksMapper = new DatabricksMapper();

            // TODO: This is a temporary solution. Remove or update this logic in the future.
            String devGroup = provisionRequest.dataProduct().getDevGroup();
            if (!devGroup.startsWith("group:")) devGroup = "group:" + devGroup;

            Map<String, Either<Throwable, String>> eitherMap =
                    databricksMapper.map(Set.of(provisionRequest.dataProduct().getDataProductOwner(), devGroup));

            Either<Throwable, String> eitherDpOwnerDatabricksId =
                    eitherMap.get(provisionRequest.dataProduct().getDataProductOwner());
            if (eitherDpOwnerDatabricksId.isLeft()) {
                var error = eitherDpOwnerDatabricksId.getLeft();
                return left(new FailedOperation(Collections.singletonList(new Problem(error.getMessage(), error))));
            }
            String dpOwnerDatabricksId = eitherDpOwnerDatabricksId.get();

            Either<Throwable, String> eitherDpDevGroupDatabricksId = eitherMap.get(devGroup);
            if (eitherDpDevGroupDatabricksId.isLeft()) {
                var error = eitherDpDevGroupDatabricksId.getLeft();
                return left(new FailedOperation(Collections.singletonList(new Problem(error.getMessage(), error))));
            }
            String dpDevGroupDatabricksId = eitherDpDevGroupDatabricksId.get();

            var unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

            DatabricksJobWorkloadSpecific databricksJobWorkloadSpecific =
                    provisionRequest.component().getSpecific();

            Either<FailedOperation, Void> eitherAttachedMetastore =
                    unityCatalogManager.attachMetastore(databricksJobWorkloadSpecific.getMetastore());
            if (eitherAttachedMetastore.isLeft()) return left(eitherAttachedMetastore.getLeft());

            Either<FailedOperation, Void> eitherCreatedRepo = createRepositoryWithPermissions(
                    provisionRequest,
                    workspaceClient,
                    databricksWorkspaceInfo,
                    dpOwnerDatabricksId,
                    dpDevGroupDatabricksId);
            if (eitherCreatedRepo.isLeft()) return left(eitherCreatedRepo.getLeft());

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

                String repoPath = databricksJobWorkloadSpecific.getRepoPath();
                repoPath = String.format("/%s", repoPath);

                Either<FailedOperation, Void> eitherDeletedRepo = repoManager.deleteRepo(
                        provisionRequest.component().getSpecific().getGit().getGitRepoUrl(), repoPath);

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
