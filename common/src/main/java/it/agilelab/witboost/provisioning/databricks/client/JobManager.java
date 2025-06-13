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
 * Class responsible for managing Databricks jobs.
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
     * If exactly one job exists with the specified name, that job is updated.
     * If multiple jobs exist with the specified name, an error is returned.
     *
     * @param jobName            The name of the job
     * @param description        The description of the job
     * @param taskKey            The task key that uniquely identifies the task within the job
     * @param runAs              The service principal name to run the job as. If null or empty, defaults to the tech adapter's service principal used for the job creation or update
     * @param jobClusterSpecific The cluster configuration details for the new cluster
     * @param schedulingSpecific The scheduling parameters for the job
     * @param jobGitSpecific     The Git-related parameters for the job source
     * @param environment        The Witboost environment
     * @return Either a Long representing the job ID if successful, or a FailedOperation if an error occurs
     */
    public Either<FailedOperation, Long> createOrUpdateJobWithNewCluster(
            String jobName,
            String description,
            String taskKey,
            String runAs,
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
                    jobName,
                    description,
                    taskKey,
                    runAs,
                    jobClusterSpecific,
                    schedulingSpecific,
                    jobGitSpecific,
                    environment);

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
                runAs,
                jobClusterSpecific,
                schedulingSpecific,
                jobGitSpecific,
                environment);
    }

    /**
     * Creates a new Databricks job with a dedicated cluster.
     *
     * @param jobName            The name of the job to be created
     * @param description        The description of the job
     * @param taskKey            The task key that uniquely identifies the task within the job
     * @param runAs              The service principal name to run the job as. If null or empty, defaults to the tech adapter's service principal used for the job creation
     * @param jobClusterSpecific The cluster configuration details for the new cluster
     * @param schedulingSpecific The scheduling parameters for the job
     * @param jobGitSpecific     The Git repository details and reference information
     * @param environment        The Witboost environment
     * @return Either a Long representing the job ID if successful, or a FailedOperation
     */
    private Either<FailedOperation, Long> createJobWithNewCluster(
            String jobName,
            String description,
            String taskKey,
            String runAs,
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

            if (runAs != null && !runAs.isEmpty()) createJob.setRunAs(new JobRunAs().setServicePrincipalName(runAs));

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
     * Updates an existing Databricks job with a new dedicated cluster configuration.
     *
     * @param jobId              The ID of the job to update
     * @param jobName            The new name for the job
     * @param description        The updated description for the job
     * @param taskKey            The task key that uniquely identifies the task within the job
     * @param runAs              The service principal name to run the job as. If null or empty, defaults to the tech adapter's service principal used for the job update
     * @param jobClusterSpecific The updated cluster configuration details
     * @param schedulingSpecific The updated scheduling parameters
     * @param jobGitSpecific     The updated Git repository details and reference information
     * @param environment        The Witboost environment
     * @return Either a Long representing the job ID if successful, or a FailedOperation if an error occurs
     */
    private Either<FailedOperation, Long> updateJobWithNewCluster(
            Long jobId,
            String jobName,
            String description,
            String taskKey,
            String runAs,
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

            if (runAs != null && !runAs.isEmpty()) jobSettings.setRunAs(new JobRunAs().setServicePrincipalName(runAs));

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
     * Exports a Databricks job configuration by its ID.
     *
     * @param jobId The unique identifier of the job to export
     * @return Either the Job configuration if successful, or a FailedOperation if an error occurs
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
     * Deletes a Databricks job by its ID.
     *
     * @param jobId The unique identifier of the job to delete
     * @return Either Void if successful, or a FailedOperation if the deletion fails
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
     * Lists all Databricks jobs matching a given name.
     *
     * @param jobName The name of the jobs to search for
     * @return Either an Iterable of BaseJob containing matching jobs if successful, or a FailedOperation if an error occurs
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
     * Creates a new Databricks job that runs on an existing cluster.
     *
     * @param jobName           The name of the job to create
     * @param description       A description of the job's purpose
     * @param existingClusterId The ID of the existing cluster to use for running the job
     * @param notebookPath      The path to the notebook that will be executed by the job
     * @param taskKey           The task key that uniquely identifies the task within the job
     * @return Either a Long representing the job ID if successful, or a FailedOperation if creation fails
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

            logger.info(
                    "Successfully created new job in {} with name: {} and ID: {}.",
                    workspaceName,
                    jobName,
                    j.getJobId());
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
     * Converts a JobGitSpecific object into a Databricks GitSource configuration.
     * This method handles both branch and tag reference types.
     *
     * @param jobGitSpecific The Git configuration containing repository URL and reference information
     * @return A GitSource object configured with the provided Git repository details
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
     * Creates a Databricks cluster specification from job-specific cluster configuration.
     *
     * @param jobClusterSpecific The cluster configuration including Spark version, node types, and other settings
     * @param sparkEnvVars       A map of Spark environment variables to be set on the cluster
     * @return A ClusterSpec configured with the provided specifications
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
     * Retrieves a job's ID by searching for a job with the specified name.
     *
     * @param jobName The name of the job to search for
     * @return Either a String containing the job ID if exactly one matching job is found,
     * or a FailedOperation if no jobs or multiple jobs are found with the given name
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
     * Converts a List of Spark configuration into a Map format.
     *
     * @param inputSparkConf List of Spark configuration entries
     * @return Map containing Spark configuration name-value pairs
     */
    private Map<String, String> getSparkConfFromSpecific(List<SparkConf> inputSparkConf) {

        Map<String, String> sparkConfNew = new HashMap<>();
        if (inputSparkConf != null)
            inputSparkConf.forEach(sparkConf -> sparkConfNew.put(sparkConf.getName(), sparkConf.getValue()));

        return sparkConfNew;
    }

    /**
     * Converts a list of Spark environment variables into a map format.
     *
     * @param inputSparkEnvVars List of Spark environment variable entries
     * @return Map containing environment variable name-value pairs
     */
    private Map<String, String> getSparkEnvVars(List<SparkEnvVar> inputSparkEnvVars) {
        Map<String, String> envVarNew = new HashMap<>();
        if (inputSparkEnvVars != null)
            inputSparkEnvVars.forEach(envVar -> envVarNew.put(envVar.getName(), envVar.getValue()));

        return envVarNew;
    }

    /**
     * Retrieves and validates environment-specific Spark environment variables.
     *
     * @param environment        The Witboost environment
     * @param jobClusterSpecific Cluster configuration containing environment-specific variables
     * @param jobName            Name of the job requesting the environment variables
     * @return Either a map of environment variables if successful, or a FailedOperation if validation fails
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
     * Converts Spark environment variables into Databricks job parameters.
     *
     * @param sparkEnvVars Map of environment variable names and values
     * @return Collection of JobParameterDefinition objects representing the environment variables
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
