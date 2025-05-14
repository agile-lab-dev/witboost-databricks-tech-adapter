package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowManager {
    private final Logger logger = LoggerFactory.getLogger(WorkflowManager.class);

    private final WorkspaceClient workspaceClient;
    private final String workspaceName;
    private final WorkspaceLevelManagerFactory workspaceLevelManagerFactory;

    public WorkflowManager(
            WorkspaceClient workspaceClient,
            String workspaceName,
            WorkspaceLevelManagerFactory workspaceLevelManagerFactory) {
        this.workspaceClient = workspaceClient;
        this.workspaceName = workspaceName;
        this.workspaceLevelManagerFactory = workspaceLevelManagerFactory;
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

    /***
     * This method brings as input a Databricks Job Task.
     * For some specific task types (job/pipelines/notebook) it stores in a custom object (DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo) the infos that are workspace's dependent.
     * In fact Databricks in the Job definition saves the relation with workspace's entities with the entity's id (and not the name), but entities' ids are different from workspace to workspace, whereas the name is the same
     * @param taskObject
     * @return WorkflowTasksInfo object
     */
    public Either<FailedOperation, Optional<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo>>
            getWorkflowTaskInfoFromId(Task taskObject) {

        try {

            DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo workflowTaskInfo =
                    new DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo();

            workflowTaskInfo.setTaskKey(taskObject.getTaskKey());

            Optional<String> optionalTaskType = retrieveTaskType(taskObject);
            if (optionalTaskType.isPresent()) {
                String taskType = optionalTaskType.get();
                workflowTaskInfo.setReferencedTaskType(taskType);

                switch (taskType) {
                    case "pipeline" -> {
                        String pipelineId = taskObject.getPipelineTask().getPipelineId();
                        String pipelineName =
                                workspaceClient.pipelines().get(pipelineId).getName();
                        workflowTaskInfo.setReferencedTaskName(pipelineName);
                        workflowTaskInfo.setReferencedTaskId(pipelineId);
                        return right(Optional.of(workflowTaskInfo));
                    }
                    case "job" -> {
                        Long jobId = (taskObject).getRunJobTask().getJobId();
                        String jobName =
                                workspaceClient.jobs().get(jobId).getSettings().getName();
                        workflowTaskInfo.setReferencedTaskName(jobName);
                        workflowTaskInfo.setReferencedTaskId(jobId.toString());
                        return right(Optional.of(workflowTaskInfo));
                    }
                    case "notebook_warehouse" -> {
                        String warehouseId = taskObject.getNotebookTask().getWarehouseId();
                        String warehouseName =
                                workspaceClient.warehouses().get(warehouseId).getName();
                        workflowTaskInfo.setReferencedClusterName(warehouseName);
                        workflowTaskInfo.setReferencedClusterId(warehouseId);
                        return right(Optional.of(workflowTaskInfo));
                    }
                    case "notebook_compute" -> {
                        String clusterId = taskObject.getExistingClusterId();
                        String clusterName =
                                workspaceClient.clusters().get(clusterId).getClusterName();
                        workflowTaskInfo.setReferencedClusterName(clusterName);
                        workflowTaskInfo.setReferencedClusterId(clusterId);
                        return right(Optional.of(workflowTaskInfo));
                    }
                }
            }
            return right(Optional.empty());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while retrieving workflow tasks info for task '%s' in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    taskObject.getTaskKey(), workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    public Either<FailedOperation, Task> createTaskFromWorkflowTaskInfo(
            DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo wfInfo, Task task) {

        String referencedTaskName = wfInfo.getReferencedTaskName();
        String originalTaskId = wfInfo.getReferencedTaskId();
        String originalTaskName = wfInfo.getReferencedTaskName();

        var dltManager = new DLTManager(workspaceClient, workspaceName);
        var jobManager = new JobManager(workspaceClient, workspaceName);

        WorkspaceLevelManager workspaceLevelManager =
                workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(workspaceClient);

        switch (wfInfo.getReferencedTaskType()) {
            case "pipeline" -> {
                Either<FailedOperation, String> eitherReferencedTaskIdCorrect =
                        dltManager.retrievePipelineIdFromName(referencedTaskName);
                if (eitherReferencedTaskIdCorrect.isLeft()) return left(eitherReferencedTaskIdCorrect.getLeft());

                logger.info(String.format(
                        "Changing id of pipeline '%s' from '%s' to '%s'",
                        originalTaskName, originalTaskId, eitherReferencedTaskIdCorrect.get()));
                task.getPipelineTask().setPipelineId(eitherReferencedTaskIdCorrect.get());
            }
            case "job" -> {
                Either<FailedOperation, String> eitherReferencedTaskIdCorrect =
                        jobManager.retrieveJobIdFromName(referencedTaskName);
                if (eitherReferencedTaskIdCorrect.isLeft()) return left(eitherReferencedTaskIdCorrect.getLeft());
                logger.info(String.format(
                        "Changing id of job '%s' from '%s' to '%s'",
                        originalTaskName, originalTaskId, eitherReferencedTaskIdCorrect.get()));
                task.getRunJobTask().setJobId(Long.valueOf(eitherReferencedTaskIdCorrect.get()));
            }
            case "notebook_warehouse" -> {
                String originalWarehouseName = wfInfo.getReferencedClusterName();
                String originalWarehouseId = wfInfo.getReferencedClusterId();
                Either<FailedOperation, String> eitherReferencedWarehouseIdCorrect =
                        workspaceLevelManager.getSqlWarehouseIdFromName(originalWarehouseName);
                if (eitherReferencedWarehouseIdCorrect.isLeft())
                    return left(eitherReferencedWarehouseIdCorrect.getLeft());
                logger.info(String.format(
                        "Changing id of warehouse '%s' from '%s' to '%s'",
                        originalWarehouseName, originalWarehouseId, eitherReferencedWarehouseIdCorrect.get()));
                task.getNotebookTask().setWarehouseId(eitherReferencedWarehouseIdCorrect.get());
            }
            case "notebook_compute" -> {
                String originalClusterName = wfInfo.getReferencedClusterName();
                String originaClusterId = wfInfo.getReferencedClusterId();
                Either<FailedOperation, String> eitherReferencedClusterIdCorrect =
                        workspaceLevelManager.getComputeClusterIdFromName(originalClusterName);
                if (eitherReferencedClusterIdCorrect.isLeft()) return left(eitherReferencedClusterIdCorrect.getLeft());
                logger.info(String.format(
                        "Changing id of compute cluster '%s' from '%s' to '%s'",
                        originalClusterName, originaClusterId, eitherReferencedClusterIdCorrect.get()));
                task.setExistingClusterId(eitherReferencedClusterIdCorrect.get());
            }
        }
        return right(task);
    }

    protected Optional<String> retrieveTaskType(Task taskObject) {
        if (taskObject.getPipelineTask() != null) return Optional.of("pipeline");
        else if (taskObject.getRunJobTask() != null) return Optional.of("job");
        else if (taskObject.getNotebookTask() != null & taskObject.getExistingClusterId() != null)
            return Optional.of("notebook_compute");
        else if (taskObject.getNotebookTask() != null
                && taskObject.getNotebookTask().getWarehouseId() != null) return Optional.of("notebook_warehouse");
        return Optional.empty();
    }

    public Either<FailedOperation, Job> reconstructJobWithCorrectIds(
            Job originalWorkflow, List<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo> workflowInfoList) {
        JobSettings originalWorkflowSettings = originalWorkflow.getSettings();

        Collection<Task> tasksList = originalWorkflowSettings.getTasks();

        ArrayList<Task> updatedTaskList = new ArrayList<>();

        for (Task task : Optional.ofNullable(tasksList).orElse(Collections.emptyList())) {
            String taskKey = task.getTaskKey();

            Optional<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo> matchingWorkflowInfo =
                    Optional.ofNullable(workflowInfoList).orElse(Collections.emptyList()).stream()
                            .filter(info -> info.getTaskKey().equals(taskKey))
                            .findFirst();

            if (matchingWorkflowInfo.isPresent()) {
                Either<FailedOperation, Task> eitherUpdatedTask =
                        createTaskFromWorkflowTaskInfo(matchingWorkflowInfo.get(), task);

                if (eitherUpdatedTask.isLeft()) return left(eitherUpdatedTask.getLeft());
                updatedTaskList.add(eitherUpdatedTask.get());
            } else {
                updatedTaskList.add(task);
            }
        }

        originalWorkflowSettings.setTasks(updatedTaskList);
        originalWorkflow.setSettings(originalWorkflowSettings);
        return right(originalWorkflow);
    }
}
