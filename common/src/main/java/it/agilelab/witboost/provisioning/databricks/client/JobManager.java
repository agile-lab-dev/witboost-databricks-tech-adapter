package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.compute.AzureAttributes;
import com.databricks.sdk.service.compute.DataSecurityMode;
import com.databricks.sdk.service.jobs.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.Environment;
import it.agilelab.witboost.provisioning.databricks.model.databricks.SparkConf;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.SparkEnvVar;
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

    private final WorkspaceClient workspaceClient;
    private final String workspaceName;

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
     * @param environment        The Witboost environment
     * @return Either a Long representing the job ID if successful, or a FailedOperation if an error occurs.
     */
    public Either<FailedOperation, Long> createOrUpdateJobWithNewCluster(
            String jobName,
            String description,
            String taskKey,
            JobClusterSpecific jobClusterSpecific,
            DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific,
            DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific,
            String environment) {

        Either<FailedOperation, Iterable<BaseJob>> eitherGetJobs = listJobsWithGivenName(jobName);

        if (eitherGetJobs.isLeft()) return left(eitherGetJobs.getLeft());

        Iterable<BaseJob> jobs = eitherGetJobs.get();
        List<BaseJob> jobList = new ArrayList<>();
        jobs.forEach(jobList::add);

        if (jobList.isEmpty())
            return createJobWithNewCluster(
                    jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, jobGitSpecific, environment);

        if (jobList.size() != 1) {
            String errorMessage = String.format(
                    "Error trying to update the job '%s'. The job name is not unique in %s.", jobName, workspaceName);
            FailedOperation failedOperation = new FailedOperation(Collections.singletonList(new Problem(errorMessage)));
            return left(failedOperation);
        }

        BaseJob job = jobList.get(0);

        return updateJobWithNewCluster(
                job.getJobId(),
                jobName,
                description,
                taskKey,
                jobClusterSpecific,
                schedulingSpecific,
                jobGitSpecific,
                environment);
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
     * @param environment        The Witboost environment
     * @return Either a Long representing the job ID if successful, or a FailedOperation.
     */
    private Either<FailedOperation, Long> createJobWithNewCluster(
            String jobName,
            String description,
            String taskKey,
            JobClusterSpecific jobClusterSpecific,
            DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific,
            DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific,
            String environment) {

        try {

            logger.info("Creating job with name {} in {}", jobName, workspaceName);

            // Parameters for the notebook
            Map<String, String> map = Map.of("", "");
            Either<FailedOperation, Map<String, String>> sparkEnvVar =
                    getSparkEnvVarsForEnvironment(environment, jobClusterSpecific, jobName);
            if (sparkEnvVar.isLeft()) return left(sparkEnvVar.getLeft());

            // Creating a collection of tasks (only one task in this case)
            Collection<Task> tasks = Collections.singletonList(new Task()
                    .setDescription(description)
                    .setNotebookTask(new NotebookTask()
                            .setBaseParameters(map)
                            .setNotebookPath(jobGitSpecific.getGitPath())
                            .setSource(Source.GIT))
                    .setTaskKey(taskKey)
                    .setNewCluster(getClusterSpecFromSpecific(jobClusterSpecific, sparkEnvVar.get())));

            GitSource gitLabSource = getGitSourceFromSpecific(jobGitSpecific);

            Collection<JobParameterDefinition> jobParameterDefinitions = getJobParameters(sparkEnvVar.get());

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

            logger.info("Created new job in {} with name: {} and ID: {}.", workspaceName, jobName, j.getJobId());

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
     * @param jobGitSpecific     The updated Git-related parameters for the task
     * @param environment        The Witboost environment
     * @return Either a Long representing the job ID if successful, or a FailedOperation if an error occurs.
     */
    private Either<FailedOperation, Long> updateJobWithNewCluster(
            Long jobId,
            String jobName,
            String description,
            String taskKey,
            JobClusterSpecific jobClusterSpecific,
            DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific,
            DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific,
            String environment) {

        try {
            logger.info("Updating job {} in {}", jobName, workspaceName);

            Map<String, String> map = Map.of("", "");
            Either<FailedOperation, Map<String, String>> sparkEnvVar =
                    getSparkEnvVarsForEnvironment(environment, jobClusterSpecific, jobName);
            if (sparkEnvVar.isLeft()) return left(sparkEnvVar.getLeft());

            Collection<Task> tasks = Collections.singletonList(new Task()
                    .setDescription(description)
                    .setNotebookTask(new NotebookTask()
                            .setBaseParameters(map)
                            .setNotebookPath(jobGitSpecific.getGitPath())
                            .setSource(Source.GIT))
                    .setTaskKey(taskKey)
                    .setNewCluster(getClusterSpecFromSpecific(jobClusterSpecific, sparkEnvVar.get())));

            GitSource gitLabSource = getGitSourceFromSpecific(jobGitSpecific);

            Collection<JobParameterDefinition> jobParameterDefinitions = getJobParameters(sparkEnvVar.get());

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

            logger.info("Updated job in {} with name: {} and ID: {}.", workspaceName, jobName, jobId);

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
            logger.info("Deleting job with ID: {} in {}", jobId, workspaceName);
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
            logger.info("Creating job with name {} in {}", jobName, workspaceName);

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

            logger.info("Created new job in {} with name: {} and ID: {}.", workspaceName, jobName, j.getJobId());
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
     * Converts the provided JobGitSpecific object into a corresponding GitSource instance.
     * This method maps the Git repository URL and its reference type (branch or tag) into a GitSource object.
     *
     * @param jobGitSpecific The Git-related parameters containing repository URL and reference type (branch or tag).
     * @return A GitSource object that encapsulates the Git repository details and reference information.
     */
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

    /**
     * Creates a ClusterSpec object based on the provided job-cluster-specific configuration
     * and Spark environment variables.
     *
     * @param jobClusterSpecific The configuration details for the job cluster, including Spark version,
     *                           node types, number of workers, and other cluster-specific attributes.
     * @param sparkEnvVars       A map of Spark environment variables to be applied to the cluster.
     * @return A ClusterSpec object containing the final cluster configuration.
     */
    private com.databricks.sdk.service.compute.ClusterSpec getClusterSpecFromSpecific(
            JobClusterSpecific jobClusterSpecific, Map<String, String> sparkEnvVars) {

        Map<String, String> sparkConf = getSparkConfFromSpecific(jobClusterSpecific.getSparkConf());

        return new com.databricks.sdk.service.compute.ClusterSpec()
                .setSparkVersion(jobClusterSpecific.getClusterSparkVersion())
                .setNodeTypeId(jobClusterSpecific.getNodeTypeId())
                .setNumWorkers(jobClusterSpecific.getNumWorkers())
                .setAzureAttributes(new AzureAttributes()
                        .setFirstOnDemand(jobClusterSpecific.getFirstOnDemand())
                        .setAvailability(jobClusterSpecific.getAvailability())
                        .setSpotBidMaxPrice(jobClusterSpecific.getSpotBidMaxPrice()))
                .setDriverNodeTypeId(jobClusterSpecific.getDriverNodeTypeId())
                .setSparkConf(sparkConf)
                .setSparkEnvVars(sparkEnvVars)
                .setDataSecurityMode(DataSecurityMode.SINGLE_USER)
                .setRuntimeEngine(jobClusterSpecific.getRuntimeEngine());
    }

    /**
     * Retrieves the job ID associated with the specified job name from the Databricks workspace.
     * If no job with the given name exists, or if multiple jobs with the same name are found,
     * an error is returned.
     *
     * @param jobName The name of the job to search for.
     * @return Either a String representing the job ID if the operation is successful,
     *         or a FailedOperation if an error occurs (e.g., no job found, multiple jobs found, or an exception occurs).
     */
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

    /**
     * Converts the provided Spark configuration list into a map.
     *
     * @param inputSparkConf A list of Spark configuration objects containing key-value pairs.
     * @return A map where the key is the configuration name, and the value is the configuration value.
     */
    private Map<String, String> getSparkConfFromSpecific(List<SparkConf> inputSparkConf) {

        Map<String, String> sparkConfNew = new HashMap<>();
        if (inputSparkConf != null)
            inputSparkConf.forEach(sparkConf -> sparkConfNew.put(sparkConf.getName(), sparkConf.getValue()));

        return sparkConfNew;
    }

    /**
     * Retrieves Spark environment variables from the provided list.
     *
     * @param inputSparkEnvVars A list of Spark environment variables to process.
     * @return A map where the key is the variable name and the value is the variable value.
     */
    private Map<String, String> getSparkEnvVars(List<SparkEnvVar> inputSparkEnvVars) {
        Map<String, String> envVarNew = new HashMap<>();
        if (inputSparkEnvVars != null)
            inputSparkEnvVars.forEach(envVar -> envVarNew.put(envVar.getName(), envVar.getValue()));

        return envVarNew;
    }

    /**
     * Retrieves Spark environment variables based on the specified environment.
     * Validates the environment and returns the corresponding Spark environment variables for the job.
     * If the environment is invalid, it returns a failed operation.
     *
     * @param environment The target environment, which can be one of DEVELOPMENT, QA, or PRODUCTION.
     * @param jobClusterSpecific The cluster configuration details, which include environment-specific
     *                           Spark settings for job clusters.
     * @param jobName The name of the job for which environment variables are being retrieved.
     *
     * @return Either a failed operation indicating an error, or a map of Spark environment variables for the specified environment.
     */
    protected Either<FailedOperation, Map<String, String>> getSparkEnvVarsForEnvironment(
            String environment, JobClusterSpecific jobClusterSpecific, String jobName) {
        Environment env;
        try {
            env = Environment.valueOf(environment.toUpperCase());
        } catch (IllegalArgumentException | NullPointerException e) {
            String errorMessage = String.format(
                    "An error occurred while getting the Spark environment variables for the job '%s' in the environment '%s'. The specified environment is invalid. Available options are: DEVELOPMENT, QA, PRODUCTION. Details: %s",
                    jobName, environment, e.getMessage());
            logger.error(errorMessage, e);
            return left(FailedOperation.singleProblemFailedOperation(errorMessage, e));
        }

        List<SparkEnvVar> sparkEnvVarEnv =
                switch (env) {
                    case DEVELOPMENT -> jobClusterSpecific.getSparkEnvVarsDevelopment();
                    case QA -> jobClusterSpecific.getSparkEnvVarsQa();
                    case PRODUCTION -> jobClusterSpecific.getSparkEnvVarsProduction();
                };

        return right(getSparkEnvVars(sparkEnvVarEnv));
    }

    /**
     * Converts a list of Spark environment variables into job parameter definitions.
     *
     * @param sparkEnvVars A map of Spark environment variable names and their corresponding values.
     * @return A collection of `JobParameterDefinition`, each representing a Spark environment variable as a parameter.
     */
    private Collection<JobParameterDefinition> getJobParameters(Map<String, String> sparkEnvVars) {

        Collection<JobParameterDefinition> jobParameterDefinitions = new ArrayList<>();
        if (sparkEnvVars != null) {
            sparkEnvVars.forEach((key, value) -> {
                JobParameterDefinition jobParameter = new JobParameterDefinition();
                jobParameter.setName(key);
                jobParameter.setDefault(value);
                jobParameterDefinitions.add(jobParameter);
            });
        }
        return jobParameterDefinitions;
    }
}
