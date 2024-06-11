package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.compute.AzureAttributes;
import com.databricks.sdk.service.jobs.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.ClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitReferenceType;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.SchedulingSpecific;
import java.util.*;
import java.util.logging.Logger;

/**
 * Class to manage Databricks jobs.
 */
public class JobManager {
    static Logger logger = Logger.getLogger(JobManager.class.getName());

    private WorkspaceClient workspaceClient;
    private String workspaceName;

    public JobManager(WorkspaceClient workspaceClient, String workspaceName) {
        this.workspaceClient = workspaceClient;
        this.workspaceName = workspaceName;
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
            logger.severe(e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }

    /**
     * Creates a Databricks job with a new dedicated cluster.
     *
     * @param jobName            The name of the job to be created.
     * @param description        The description of the job.
     * @param taskKey            The task key.
     * @param clusterSpecific    The parameters for creating a new cluster.
     * @param schedulingSpecific The parameters for scheduling the job.
     * @param gitSpecific        The parameters including the git details for the task.
     * @return Either a Long representing the job ID if successful, or a FailedOperation.
     */
    public Either<FailedOperation, Long> createJobWithNewCluster(
            String jobName,
            String description,
            String taskKey,
            ClusterSpecific clusterSpecific,
            SchedulingSpecific schedulingSpecific,
            GitSpecific gitSpecific) {

        try {

            logger.info(String.format("Creating job with name %s in %s", jobName, workspaceName));

            // Parameters for the notebook
            Map<String, String> map = Map.of("", "");

            // Creating a collection of tasks (only one task in this case)
            Collection<Task> tasks = Collections.singletonList(new Task()
                    .setDescription(description)
                    .setNotebookTask(new NotebookTask()
                            .setBaseParameters(map)
                            .setNotebookPath(gitSpecific.getGitPath())
                            .setSource(Source.GIT))
                    .setTaskKey(taskKey)
                    .setNewCluster(getClusterSpecFromSpecific(clusterSpecific)));

            GitSource gitLabSource = getGitSourceFromSpecific(gitSpecific);

            Collection<JobParameterDefinition> jobParameterDefinitions = new ArrayList<>();

            // Create job with scheduling if enabled
            CreateJob createJob = new CreateJob()
                    .setName(jobName)
                    .setTasks(tasks)
                    .setParameters(jobParameterDefinitions)
                    .setGitSource(gitLabSource);

            if (schedulingSpecific != null)
                createJob.setSchedule(new CronSchedule()
                        .setTimezoneId(schedulingSpecific.getJavaTimezoneId())
                        .setQuartzCronExpression(schedulingSpecific.getCronExpression()));

            // Create the job with the specified parameters
            CreateResponse j = workspaceClient.jobs().create(createJob);

            logger.info(String.format(
                    "Created new job in %s with name: %s and ID: %d.", workspaceName, jobName, j.getJobId()));

            return right(j.getJobId());

        } catch (Exception e) {
            logger.severe(e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }

    private GitSource getGitSourceFromSpecific(GitSpecific gitSpecific) {
        GitSource gitLabSource =
                new GitSource().setGitUrl(gitSpecific.getGitRepoUrl()).setGitProvider(GitProvider.GIT_LAB);

        if (gitSpecific.getGitReferenceType().toString().equalsIgnoreCase(GitReferenceType.BRANCH.toString()))
            gitLabSource.setGitBranch(gitSpecific.getGitReference());
        else if (gitSpecific.getGitReferenceType().toString().equalsIgnoreCase(GitReferenceType.TAG.toString())) {
            gitLabSource.setGitTag(gitSpecific.getGitReference());
        }

        return gitLabSource;
    }

    private com.databricks.sdk.service.compute.ClusterSpec getClusterSpecFromSpecific(ClusterSpecific clusterSpecific) {
        return new com.databricks.sdk.service.compute.ClusterSpec()
                .setSparkVersion(clusterSpecific.getClusterSparkVersion())
                .setNodeTypeId(clusterSpecific.getNodeTypeId())
                .setNumWorkers(clusterSpecific.getNumWorkers())
                .setAzureAttributes(new AzureAttributes()
                        .setFirstOnDemand(clusterSpecific.getFirstOnDemand())
                        .setAvailability(clusterSpecific.getAvailability())
                        .setSpotBidMaxPrice(clusterSpecific.getSpotBidMaxPrice()))
                .setDriverNodeTypeId(clusterSpecific.getDriverNodeTypeId())
                .setSparkConf(clusterSpecific.getSparkConf())
                .setSparkEnvVars(clusterSpecific.getSparkEnvVars())
                .setRuntimeEngine(clusterSpecific.getRuntimeEngine());
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
            logger.severe(e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
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
            logger.severe(e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
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
            logger.severe(e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }
}
