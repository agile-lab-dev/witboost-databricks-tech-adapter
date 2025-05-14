package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.compute.AzureAttributes;
import com.databricks.sdk.service.jobs.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.DatabricksJobWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.JobClusterSpecific;
import java.util.*;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to manage Databricks jobs.
 */
public class JobManager {
    private final Logger logger = LoggerFactory.getLogger(JobManager.class);

    private WorkspaceClient workspaceClient;
    private String workspaceName;

    public JobManager(WorkspaceClient workspaceClient, String workspaceName) {
        this.workspaceClient = workspaceClient;
        this.workspaceName = workspaceName;
    }

    /**
     * Creates or updates a Databricks job with a new dedicated cluster.
     * If a job with the specified name does not exist, a new job is created.
     * If the job exists and is unique, the existing job is updated.
     * Returns an error if more than one job with the same name exists.
     *
     * @param jobName            The name of the job.
     * @param description        The description of the job.
     * @param taskKey            The task key.
     * @param jobClusterSpecific The cluster configuration details.
     * @param schedulingSpecific The scheduling parameters.
     * @param jobGitSpecific     The Git-related parameters for the task.
     * @return Either a Long representing the job ID if successful, or a FailedOperation if an error occurs.
     */
    public Either<FailedOperation, Long> createOrUpdateJobWithNewCluster(
            String jobName,
            String description,
            String taskKey,
            JobClusterSpecific jobClusterSpecific,
            DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific,
            DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific) {

        Either<FailedOperation, Iterable<BaseJob>> eitherGetJobs = listJobsWithGivenName(jobName);

        if (eitherGetJobs.isLeft()) return left(eitherGetJobs.getLeft());

        Iterable<BaseJob> jobs = eitherGetJobs.get();
        List<BaseJob> jobList = new ArrayList<>();
        jobs.forEach(jobList::add);

        if (jobList.isEmpty())
            return createJobWithNewCluster(
                    jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, jobGitSpecific);

        if (jobList.size() != 1) {
            String errorMessage = String.format(
                    "Error trying to update the job '%s'. The job name is not unique in %s.", jobName, workspaceName);
            FailedOperation failedOperation = new FailedOperation(Collections.singletonList(new Problem(errorMessage)));
            return left(failedOperation);
        }

        BaseJob job = jobList.get(0);

        return updateJobWithNewCluster(
                job.getJobId(), jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, jobGitSpecific);
    }

    /**
     * Creates a Databricks job with a new dedicated cluster.
     *
     * @param jobName            The name of the job to be created.
     * @param description        The description of the job.
     * @param taskKey            The task key.
     * @param jobClusterSpecific    The parameters for creating a new cluster.
     * @param schedulingSpecific The parameters for scheduling the job.
     * @param jobGitSpecific        The parameters including the git details for the task.
     * @return Either a Long representing the job ID if successful, or a FailedOperation.
     */
    private Either<FailedOperation, Long> createJobWithNewCluster(
            String jobName,
            String description,
            String taskKey,
            JobClusterSpecific jobClusterSpecific,
            DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific,
            DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific) {

        try {

            logger.info(String.format("Creating job with name %s in %s", jobName, workspaceName));

            // Parameters for the notebook
            Map<String, String> map = Map.of("", "");

            // Creating a collection of tasks (only one task in this case)
            Collection<Task> tasks = Collections.singletonList(new Task()
                    .setDescription(description)
                    .setNotebookTask(new NotebookTask()
                            .setBaseParameters(map)
                            .setNotebookPath(jobGitSpecific.getGitPath())
                            .setSource(Source.GIT))
                    .setTaskKey(taskKey)
                    .setNewCluster(getClusterSpecFromSpecific(jobClusterSpecific)));

            GitSource gitLabSource = getGitSourceFromSpecific(jobGitSpecific);

            Collection<JobParameterDefinition> jobParameterDefinitions = new ArrayList<>();

            CreateJob createJob = new CreateJob()
                    .setName(jobName)
                    .setTasks(tasks)
                    .setParameters(jobParameterDefinitions)
                    .setGitSource(gitLabSource);

            if (schedulingSpecific != null)
                createJob.setSchedule(new CronSchedule()
                        .setTimezoneId(schedulingSpecific.getJavaTimezoneId())
                        .setQuartzCronExpression(schedulingSpecific.getCronExpression()));

            CreateResponse j = workspaceClient.jobs().create(createJob);

            logger.info(String.format(
                    "Created new job in %s with name: %s and ID: %d.", workspaceName, jobName, j.getJobId()));

            return right(j.getJobId());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating the job %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    jobName, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Updates an existing Databricks job with a new dedicated cluster.
     *
     * @param jobId              The ID of the job to update.
     * @param jobName            The new name of the job.
     * @param description        The updated description of the job.
     * @param taskKey            The task key.
     * @param jobClusterSpecific The updated cluster configuration details.
     * @param schedulingSpecific The updated scheduling parameters.
     * @param jobGitSpecific     The updated Git-related parameters for the task.
     * @return Either a Long representing the job ID if successful, or a FailedOperation if an error occurs.
     */
    private Either<FailedOperation, Long> updateJobWithNewCluster(
            Long jobId,
            String jobName,
            String description,
            String taskKey,
            JobClusterSpecific jobClusterSpecific,
            DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific,
            DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific) {

        try {
            logger.info(String.format("Updating job %s in %s", jobName, workspaceName));

            Map<String, String> map = Map.of("", "");

            Collection<Task> tasks = Collections.singletonList(new Task()
                    .setDescription(description)
                    .setNotebookTask(new NotebookTask()
                            .setBaseParameters(map)
                            .setNotebookPath(jobGitSpecific.getGitPath())
                            .setSource(Source.GIT))
                    .setTaskKey(taskKey)
                    .setNewCluster(getClusterSpecFromSpecific(jobClusterSpecific)));

            GitSource gitLabSource = getGitSourceFromSpecific(jobGitSpecific);

            Collection<JobParameterDefinition> jobParameterDefinitions = new ArrayList<>();

            JobSettings jobSettings = new JobSettings();
            jobSettings
                    .setName(jobName)
                    .setTasks(tasks)
                    .setParameters(jobParameterDefinitions)
                    .setGitSource(gitLabSource);

            if (schedulingSpecific != null)
                jobSettings.setSchedule(new CronSchedule()
                        .setTimezoneId(schedulingSpecific.getJavaTimezoneId())
                        .setQuartzCronExpression(schedulingSpecific.getCronExpression()));

            workspaceClient.jobs().update(new UpdateJob().setJobId(jobId).setNewSettings(jobSettings));

            logger.info(String.format("Updated job in %s with name: %s and ID: %d.", workspaceName, jobName, jobId));

            return right(jobId);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while updating the job %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    jobName, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Exports a job given its ID.
     *
     * @param jobId The ID of the job to export.
     * @return Either the exported Job if successful, or a FailedOperation.
     */
    public Either<FailedOperation, Job> exportJob(Long jobId) {
        try {
            Job job = workspaceClient.jobs().get(jobId);
            return right(job);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while exporting the job %d (workspace: %s). Please try again and if the error persists contact the platform team. Details: %s",
                    jobId, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Deletes a job given its ID.
     *
     * @param jobId The ID of the job to delete.
     * @return Either Void if successful, or a FailedOperation.
     */
    public Either<FailedOperation, Void> deleteJob(Long jobId) {
        try {
            logger.info(String.format("Deleting job with ID: %d in %s", jobId, workspaceName));
            workspaceClient.jobs().delete(jobId);
            return right(null);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while deleting the job with ID %d (workspace: %s). Please try again and if the error persists contact the platform team. Details: %s",
                    jobId, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Lists all jobs with a given name.
     *
     * @param jobName The name of the job(s) to list.
     * @return Either an Iterable of BaseJob if successful, or a FailedOperation.
     */
    public Either<FailedOperation, Iterable<BaseJob>> listJobsWithGivenName(String jobName) {
        try {
            Iterable<BaseJob> list = workspaceClient.jobs().list(new ListJobsRequest().setName(jobName));
            return right(list);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while listing the jobs named %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    jobName, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Creates a Databricks job using an existing cluster.
     *
     * @param jobName           The name of the job to be created.
     * @param description       The description of the job.
     * @param existingClusterId The ID of the existing cluster to run the job on.
     * @param notebookPath      The path to the notebook to be executed by the job.
     * @param taskKey           The task key.
     * @return Either a Long representing the job ID if successful, or a FailedOperation.
     */
    public Either<FailedOperation, Long> createJobWithExistingCluster(
            String jobName, String description, String existingClusterId, String notebookPath, String taskKey) {

        try {
            logger.info(String.format("Creating job with name %s in %s", jobName, workspaceName));

            // Parameters for the notebook
            Map<String, String> map = Map.of("", "");

            // Creating a collection of tasks (only one task in this case)
            Collection<Task> tasks = Collections.singletonList(new Task()
                    .setDescription(description)
                    .setExistingClusterId(existingClusterId)
                    .setNotebookTask(new NotebookTask()
                            .setBaseParameters(map)
                            .setNotebookPath(notebookPath)
                            .setSource(Source.WORKSPACE))
                    .setTaskKey(taskKey));

            CreateResponse j = workspaceClient
                    .jobs()
                    .create(new CreateJob().setName(jobName).setTasks(tasks));

            logger.info(String.format(
                    "Created new job in %s with name: %s and ID: %d.", workspaceName, jobName, j.getJobId()));
            return right(j.getJobId());
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating the job %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    jobName, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private GitSource getGitSourceFromSpecific(DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific) {
        GitSource gitLabSource =
                new GitSource().setGitUrl(jobGitSpecific.getGitRepoUrl()).setGitProvider(GitProvider.GIT_LAB);

        if (jobGitSpecific
                .getGitReferenceType()
                .toString()
                .equalsIgnoreCase(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH.toString()))
            gitLabSource.setGitBranch(jobGitSpecific.getGitReference());
        else if (jobGitSpecific
                .getGitReferenceType()
                .toString()
                .equalsIgnoreCase(DatabricksJobWorkloadSpecific.GitReferenceType.TAG.toString())) {
            gitLabSource.setGitTag(jobGitSpecific.getGitReference());
        }

        return gitLabSource;
    }

    private com.databricks.sdk.service.compute.ClusterSpec getClusterSpecFromSpecific(
            JobClusterSpecific jobClusterSpecific) {

        Map<String, String> sparkConfNew = new HashMap<>();
        if (jobClusterSpecific.getSparkConf() != null)
            jobClusterSpecific
                    .getSparkConf()
                    .forEach(sparkConf -> sparkConfNew.put(sparkConf.getName(), sparkConf.getValue()));

        Map<String, String> envVarNew = new HashMap<>();
        if (jobClusterSpecific.getSparkEnvVars() != null)
            jobClusterSpecific.getSparkEnvVars().forEach(envVar -> envVarNew.put(envVar.getName(), envVar.getValue()));

        return new com.databricks.sdk.service.compute.ClusterSpec()
                .setSparkVersion(jobClusterSpecific.getClusterSparkVersion())
                .setNodeTypeId(jobClusterSpecific.getNodeTypeId())
                .setNumWorkers(jobClusterSpecific.getNumWorkers())
                .setAzureAttributes(new AzureAttributes()
                        .setFirstOnDemand(jobClusterSpecific.getFirstOnDemand())
                        .setAvailability(jobClusterSpecific.getAvailability())
                        .setSpotBidMaxPrice(jobClusterSpecific.getSpotBidMaxPrice()))
                .setDriverNodeTypeId(jobClusterSpecific.getDriverNodeTypeId())
                .setSparkConf(sparkConfNew)
                .setSparkEnvVars(envVarNew)
                .setRuntimeEngine(jobClusterSpecific.getRuntimeEngine());
    }

    public Either<FailedOperation, String> retrieveJobIdFromName(String jobName) {

        try {
            Iterable<BaseJob> jobsIterable = workspaceClient.jobs().list(new ListJobsRequest());
            List<BaseJob> jobListFiltered = StreamSupport.stream(jobsIterable.spliterator(), false)
                    .filter(job -> job.getSettings().getName().equalsIgnoreCase(jobName))
                    .toList();

            if (jobListFiltered.isEmpty()) {
                String errorMessage = String.format(
                        "An error occurred while searching job '%s' in %s: no job found with that name.",
                        jobName, workspaceName);
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            } else if (jobListFiltered.size() > 1) {
                String errorMessage = String.format(
                        "An error occurred while searching job '%s' in %s: more than 1 job found with that name.",
                        jobName, workspaceName);
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            } else {
                return right(jobListFiltered.get(0).getJobId().toString());
            }

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while getting the list of Jobs named %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    jobName, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
