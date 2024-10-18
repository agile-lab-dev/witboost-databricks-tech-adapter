package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowManager {
    private final Logger logger = LoggerFactory.getLogger(WorkflowManager.class);

    private final WorkspaceClient workspaceClient;
    private final String workspaceName;

    public WorkflowManager(WorkspaceClient workspaceClient, String workspaceName) {
        this.workspaceClient = workspaceClient;
        this.workspaceName = workspaceName;
    }

    public Either<FailedOperation, Long> createOrUpdateWorkflow(Job job) {

        JobManager jobManager = new JobManager(workspaceClient, workspaceName);
        Either<FailedOperation, Iterable<BaseJob>> eitherGetWorkflows =
                jobManager.listJobsWithGivenName(job.getSettings().getName());

        if (eitherGetWorkflows.isLeft()) return left(eitherGetWorkflows.getLeft());

        Iterable<BaseJob> jobs = eitherGetWorkflows.get();
        List<BaseJob> jobList = new ArrayList<>();
        jobs.forEach(jobList::add);

        if (jobList.isEmpty()) return createWorkflow(job);

        if (jobList.size() != 1) {
            String errorMessage = String.format(
                    "Error trying to update the workflow '%s'. The workflow name is not unique in %s.",
                    job.getSettings().getName(), workspaceName);
            FailedOperation failedOperation = new FailedOperation(Collections.singletonList(new Problem(errorMessage)));
            return left(failedOperation);
        }

        job.setJobId(jobList.get(0).getJobId());
        return updateWorkflow(job);
    }

    public Either<FailedOperation, Long> createWorkflow(Job workflow) {

        try {

            logger.info(String.format(
                    "Creating workflow [name: %s, workspace %s].",
                    workflow.getSettings().getName(), workspaceName));

            CreateResponse wf = workspaceClient
                    .jobs()
                    .create(new CreateJob()
                            .setContinuous(workflow.getSettings().getContinuous())
                            .setDeployment(workflow.getSettings().getDeployment())
                            .setDescription(workflow.getSettings().getDescription())
                            .setEmailNotifications(workflow.getSettings().getEmailNotifications())
                            .setEnvironments(workflow.getSettings().getEnvironments())
                            .setFormat(workflow.getSettings().getFormat())
                            .setGitSource(workflow.getSettings().getGitSource())
                            .setJobClusters(workflow.getSettings().getJobClusters())
                            .setMaxConcurrentRuns(workflow.getSettings().getMaxConcurrentRuns())
                            .setName(workflow.getSettings().getName())
                            .setNotificationSettings(workflow.getSettings().getNotificationSettings())
                            .setParameters(workflow.getSettings().getParameters())
                            .setQueue(workflow.getSettings().getQueue())
                            .setRunAs(workflow.getSettings().getRunAs())
                            .setSchedule(workflow.getSettings().getSchedule())
                            .setTags(workflow.getSettings().getTags())
                            .setTasks(workflow.getSettings().getTasks())
                            .setTimeoutSeconds(workflow.getSettings().getTimeoutSeconds())
                            .setTrigger(workflow.getSettings().getTrigger())
                            .setWebhookNotifications(workflow.getSettings().getWebhookNotifications())
                            .setEditMode(workflow.getSettings().getEditMode())
                            .setHealth(workflow.getSettings().getHealth()));
            return right(wf.getJobId());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating the workflow %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    workflow.getSettings().getName(), workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    public Either<FailedOperation, Long> updateWorkflow(Job workflow) {

        try {
            logger.info(String.format(
                    "Updating workflow [name: %s, workspace %s].",
                    workflow.getSettings().getName(), workspaceName));

            workspaceClient.jobs().reset(workflow.getJobId(), workflow.getSettings());

            return right(workflow.getJobId());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while updating the workflow %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    workflow.getSettings().getName(), workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
