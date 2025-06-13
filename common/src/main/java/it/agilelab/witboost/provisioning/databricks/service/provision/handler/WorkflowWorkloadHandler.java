package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.BaseJob;
import com.databricks.sdk.service.jobs.Job;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.WorkspaceClientConfig;
import it.agilelab.witboost.provisioning.databricks.client.JobManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkflowManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManagerFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksPermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import java.util.*;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class WorkflowWorkloadHandler extends BaseWorkloadHandler {

    private final Logger logger = LoggerFactory.getLogger(WorkflowWorkloadHandler.class);

    @Autowired
    public WorkflowWorkloadHandler(
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
     * Provisions a new Databricks workflow for the given component.
     *
     * @param provisionRequest the request containing the specifics for the workflow to be provisioned
     * @param workspaceClient the Databricks workspace client
     * @param databricksWorkspaceInfo information about the Databricks workspace
     * @return Either a failed operation or the ID of the provisioned workflow as a String
     */
    public Either<FailedOperation, String> provisionWorkflow(
            ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {

        try {
            Either<FailedOperation, Map<String, String>> eitherPrincipalsMapping = mapPrincipals(provisionRequest);
            if (eitherPrincipalsMapping.isLeft()) return Either.left(eitherPrincipalsMapping.getLeft());

            Map<String, String> principalsMapping = eitherPrincipalsMapping.get();
            String dpOwnerDatabricksId =
                    principalsMapping.get(provisionRequest.dataProduct().getDataProductOwner());

            String devGroup = provisionRequest.dataProduct().getDevGroup();
            if (!devGroup.startsWith("group:"))
                devGroup = "group:"
                        + devGroup; // TODO: This is a temporary solution. Remove or update this logic in the future.
            String dpDevGroupDatabricksId = principalsMapping.get(devGroup);

            Either<FailedOperation, Void> eitherCreatedRepo = createRepositoryWithPermissions(
                    provisionRequest,
                    workspaceClient,
                    databricksWorkspaceInfo,
                    dpOwnerDatabricksId,
                    dpDevGroupDatabricksId);
            if (eitherCreatedRepo.isLeft()) return left(eitherCreatedRepo.getLeft());

            Either<FailedOperation, Long> eitherCreatedWorkflow =
                    provisionWorkflow(provisionRequest, workspaceClient, databricksWorkspaceInfo.getName());
            if (eitherCreatedWorkflow.isLeft()) return left(eitherCreatedWorkflow.getLeft());

            logger.info(String.format("Workspace available at: %s", databricksWorkspaceInfo.getDatabricksHost()));

            String workflowUrl =
                    "https://" + databricksWorkspaceInfo.getDatabricksHost() + "/jobs/" + eitherCreatedWorkflow.get();
            logger.info(String.format(
                    "New workflow linked to component %s available at %s",
                    provisionRequest.component().getName(), workflowUrl));

            return right(eitherCreatedWorkflow.get().toString());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while provisioning component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Creates a new workflow in the Databricks workspace.
     *
     * @param provisionRequest the request containing the specifics for the workflow to be created
     * @param workspaceClient the Databricks workspace client
     * @param workspaceName the name of the Databricks workspace
     * @return Either a failed operation or the ID of the created workflow as a Long
     */
    protected Either<FailedOperation, Long> provisionWorkflow(
            ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            String workspaceName) {
        try {
            DatabricksWorkflowWorkloadSpecific databricksWorkflowWorkloadSpecific =
                    provisionRequest.component().getSpecific();

            var workflowManager = new WorkflowManager(workspaceClient, workspaceName, workspaceLevelManagerFactory);

            Job originalWorkflow = databricksWorkflowWorkloadSpecific.getWorkflow();

            Either<FailedOperation, Job> eitherUpdatedWorkflow = workflowManager.reconstructJobWithCorrectIds(
                    originalWorkflow, databricksWorkflowWorkloadSpecific.getWorkflowTasksInfoList());
            if (eitherUpdatedWorkflow.isLeft()) return left(eitherUpdatedWorkflow.getLeft());

            Job updatedWorkflow = eitherUpdatedWorkflow.get();

            logger.info(String.format(
                    "(%s) Original workflow definition: %s",
                    provisionRequest.component().getName(), originalWorkflow));
            logger.info(String.format(
                    "(%s) Updated workflow definition: %s",
                    provisionRequest.component().getName(), updatedWorkflow));

            return workflowManager.createOrUpdateWorkflow(updatedWorkflow);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating the new Databricks workflow for component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Unprovisions a previously provisioned Databricks workflow, deleting the associated job and repository if requested.
     *
     * @param provisionRequest the request containing the specifics for the workflow to be unprovisioned
     * @param workspaceClient the Databricks workspace client
     * @param databricksWorkspaceInfo information about the Databricks workspace
     * @return Either a failed operation or void if successful
     */
    public Either<FailedOperation, Void> unprovisionWorkload(
            ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {
        try {
            DatabricksWorkflowWorkloadSpecific databricksWorkflowWorkloadSpecific =
                    provisionRequest.component().getSpecific();

            var jobManager = new JobManager(workspaceClient, databricksWorkspaceInfo.getName());

            Either<FailedOperation, Iterable<BaseJob>> eitherGetWorkflows =
                    jobManager.listJobsWithGivenName(databricksWorkflowWorkloadSpecific
                            .getWorkflow()
                            .getSettings()
                            .getName());

            if (eitherGetWorkflows.isLeft()) return left(eitherGetWorkflows.getLeft());

            Iterable<BaseJob> workflows = eitherGetWorkflows.get();
            List<Problem> problems = new ArrayList<>();

            workflows.forEach(workflow -> {
                Either<FailedOperation, Void> result = jobManager.deleteJob(workflow.getJobId());
                if (result.isLeft()) problems.addAll(result.getLeft().problems());
            });

            if (!problems.isEmpty()) return Either.left(new FailedOperation(problems));

            if (provisionRequest.removeData()) {
                var repoManager = new RepoManager(workspaceClient, databricksWorkspaceInfo.getName());

                String repoPath = databricksWorkflowWorkloadSpecific.getRepoPath();
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
}
