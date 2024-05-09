package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.compute.AzureAttributes;
import com.databricks.sdk.service.compute.AzureAvailability;
import com.databricks.sdk.service.compute.RuntimeEngine;
import com.databricks.sdk.service.jobs.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import lombok.Getter;
import lombok.Setter;

/**
 * Class to manage Databricks jobs.
 */
public class JobManager {
    static Logger logger = Logger.getLogger(JobManager.class.getName());

    /**
     * Creates a Databricks job.
     *
     * @param workspaceClient   The WorkspaceClient instance.
     * @param jobName           The name of the job to be created.
     * @param description       The description of the job.
     * @param existingClusterId The ID of the existing cluster to run the job on.
     * @param notebookPath      The path to the notebook to be executed by the job.
     * @param taskKey           The task key.
     * @return                  Either a Long representing the job ID if successful, or a FailedOperation.
     */
    public Either<FailedOperation, Long> createJobWithExistingCluster(
            WorkspaceClient workspaceClient,
            String jobName,
            String description,
            String existingClusterId,
            String notebookPath,
            String taskKey) {

        try {
            logger.log(Level.INFO, "Attempting to create the job. Please wait...");

            // Parameters for the notebook
            Map<String, String> map = Map.of("", "");

            // Creating a collection of tasks (only one task in this case)
            Collection<Task> tasks = Arrays.asList(new Task()
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

            logger.log(
                    Level.INFO, "View  the job at " + workspaceClient.config().getHost() + "/#job/" + j.getJobId());
            return right(j.getJobId());
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }

    /**
     * Creates a Databricks job with a new dedicated cluster.
     *
     * @param workspaceClient   The WorkspaceClient instance.
     * @param jobName           The name of the job to be created.
     * @param description       The description of the job.
     * @param taskKey           The task key.
     * @param newClusterParams  The parameters for creating a new cluster.
     * @param scheduleParams    The parameters for scheduling the job.
     * @param gitSource         The parameters for accessing the Git repository.
     * @param notebookPath      The relative path to the notebook to be executed by the job.
     * @return                  Either a Long representing the job ID if successful, or a FailedOperation.
     */
    public Either<FailedOperation, Long> createJobWithNewCluster(
            WorkspaceClient workspaceClient,
            String jobName,
            String description,
            String taskKey,
            NewClusterParams newClusterParams,
            ScheduleParams scheduleParams,
            GitJobSource gitSource,
            String notebookPath) {

        try {

            // Parameters for the notebook
            Map<String, String> map = Map.of("", "");

            // Creating a collection of tasks (only one task in this case)
            Collection<Task> tasks = Arrays.asList(new Task()
                    .setDescription(description)
                    .setNotebookTask(new NotebookTask()
                            .setBaseParameters(map)
                            .setNotebookPath(notebookPath)
                            .setSource(Source.GIT))
                    .setTaskKey(taskKey)
                    .setNewCluster(new com.databricks.sdk.service.compute.ClusterSpec()
                            .setSparkVersion(newClusterParams.getSparkVersion())
                            .setNodeTypeId(newClusterParams.getNodeTypeId())
                            .setNumWorkers(newClusterParams.getNumWorkers())
                            .setAzureAttributes(new AzureAttributes()
                                    .setFirstOnDemand(newClusterParams.getFirstOnDemand())
                                    .setAvailability(AzureAvailability.valueOf(newClusterParams.getAvailability()))
                                    .setSpotBidMaxPrice(newClusterParams.getSpotBidMaxPrice()))
                            .setDriverNodeTypeId(newClusterParams.getDriverNodeTypeId())
                            .setSparkConf(newClusterParams.getSparkConf())
                            .setSparkEnvVars(newClusterParams.getSparkEnvVars())
                            .setRuntimeEngine(newClusterParams.getRuntimeEngine())));

            GitSource gitLabSource =
                    new GitSource().setGitUrl(gitSource.getGitUrl()).setGitProvider(GitProvider.GIT_LAB);

            if (gitSource.getGitReferenceType().equals(GitReferenceType.BRANCH))
                gitSource.setGitBranch(gitSource.getGitBranch());
            else if (gitSource.getGitReferenceType().equals(GitReferenceType.TAG)) {
                gitSource.setGitTag(gitSource.getGitTag());
            }

            CreateResponse j = workspaceClient
                    .jobs()
                    .create(new CreateJob()
                            .setName(jobName)
                            .setTasks(tasks)
                            .setSchedule(new CronSchedule()
                                    .setTimezoneId(scheduleParams.getTimeZoneId())
                                    .setQuartzCronExpression(scheduleParams.getCronExpression()))
                            .setGitSource(gitLabSource));

            return right(j.getJobId());

        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }

    /**
     * Export a job given its ID.
     *
     * @param workspaceClient   The WorkspaceClient instance.
     * @param jobId             The ID of the job to export.
     * @return                  Either the exported Job if successful, or a FailedOperation.
     */
    public Either<FailedOperation, Job> exportJob(WorkspaceClient workspaceClient, Long jobId) {
        try {
            Job job = workspaceClient.jobs().get(jobId);

            System.out.println(job.toString());
            return right(job);
        } catch (Exception e) {
            logger.log(Level.SEVERE, e.getMessage());
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }
}

/**
 * Class representing parameters for creating a job cluster.
 */
@Getter
@Setter
class NewClusterParams {
    private String sparkVersion;
    private String nodeTypeId;
    private Long numWorkers;
    private Long firstOnDemand;
    private Double spotBidMaxPrice;
    private String availability;
    private String driverNodeTypeId;
    private Map<String, String> sparkConf;
    private Map<String, String> sparkEnvVars;
    private RuntimeEngine runtimeEngine;
}

/**
 * Class representing parameters for scheduling a job.
 */
@Getter
@Setter
class ScheduleParams {
    private String timeZoneId;
    private String cronExpression;
}

/**
 * Class representing parameters for accessing a Git repository.
 */
@Getter
@Setter
class GitJobSource {
    private String gitUrl;
    private GitProvider gitProvider;
    private String gitBranch;
    private String gitTag;
    private GitReferenceType gitReferenceType;
}

@Getter
enum GitReferenceType {
    BRANCH,
    TAG
}
