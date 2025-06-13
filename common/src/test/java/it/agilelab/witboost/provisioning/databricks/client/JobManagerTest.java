package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.compute.AzureAvailability;
import com.databricks.sdk.service.compute.RuntimeEngine;
import com.databricks.sdk.service.jobs.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.model.databricks.SparkConf;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.SparkEnvVar;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.DatabricksJobWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.JobClusterSpecific;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
@ExtendWith(MockitoExtension.class)
public class JobManagerTest {

    @Mock
    WorkspaceClient workspaceClient;

    @InjectMocks
    JobManager jobManager;

    private static final String jobName = "MyJob";
    private static final String description = "Description of the job";
    private static final String taskKey = "task123";
    private static final String runAs = "test-principal";
    private static final String gitRepoUrl = "https://github.com/user/repo.git";
    private static final String notebookPath = "/path/to/notebook";

    private JobClusterSpecific createJobClusterSpecific() {
        JobClusterSpecific jobClusterSpecific = new JobClusterSpecific();
        jobClusterSpecific.setClusterSparkVersion("3.0.1");
        jobClusterSpecific.setNodeTypeId("node123");
        jobClusterSpecific.setNumWorkers(2L);
        jobClusterSpecific.setFirstOnDemand(1L);
        jobClusterSpecific.setSpotBidMaxPrice(0.5);
        jobClusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        jobClusterSpecific.setDriverNodeTypeId("driver123");
        SparkConf sparkConf = new SparkConf();
        sparkConf.setName("spark.conf");
        sparkConf.setValue("value");
        jobClusterSpecific.setSparkConf(List.of(sparkConf));
        SparkEnvVar sparkEnvVar = new SparkEnvVar("spark.env.var", "value");
        jobClusterSpecific.setSparkEnvVarsDevelopment(List.of(sparkEnvVar));
        jobClusterSpecific.setSparkEnvVarsQa(List.of(sparkEnvVar));
        jobClusterSpecific.setSparkEnvVarsProduction(List.of(sparkEnvVar));
        jobClusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        return jobClusterSpecific;
    }

    private DatabricksJobWorkloadSpecific.SchedulingSpecific createSchedulingSpecific() {
        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific =
                new DatabricksJobWorkloadSpecific.SchedulingSpecific();
        schedulingSpecific.setJavaTimezoneId("UTC");
        schedulingSpecific.setCronExpression("0 0 12 * * ?");
        return schedulingSpecific;
    }

    private DatabricksJobWorkloadSpecific.JobGitSpecific createJobGitSpecific(
            DatabricksJobWorkloadSpecific.GitReferenceType referenceType) {
        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                new DatabricksJobWorkloadSpecific.JobGitSpecific();
        jobGitSpecific.setGitRepoUrl(gitRepoUrl);
        jobGitSpecific.setGitProvider(GitProvider.GIT_LAB);
        jobGitSpecific.setGitReference("main");
        jobGitSpecific.setGitReferenceType(referenceType);
        return jobGitSpecific;
    }

    @BeforeEach
    public void setUp() {
        jobManager = new JobManager(workspaceClient, "workspace");

        JobsAPI mockJobs = mock(JobsAPI.class);
        lenient().when(workspaceClient.jobs()).thenReturn(mockJobs);
    }

    @Test
    public void testCreateJobWithExistingCluster() {
        String existingClusterId = "existingCluster123";
        String taskKey = "task123";
        CreateResponse createResponse = new CreateResponse().setJobId(123L);
        when(workspaceClient.jobs().create(any())).thenReturn(createResponse);
        Either<FailedOperation, Long> result =
                jobManager.createJobWithExistingCluster(jobName, description, existingClusterId, notebookPath, taskKey);

        verify(workspaceClient.jobs(), times(1)).create(any());
        assertTrue(result.isRight());
        assertEquals(123L, result.get());
    }

    @Test
    public void testCreateJobWithExistingCluster_Failure() {
        String existingClusterId = "existingCluster123";
        String taskKey = "task123";

        when(workspaceClient.jobs().create(any())).thenThrow(new RuntimeException("Failed to create job"));

        Either<FailedOperation, Long> result =
                jobManager.createJobWithExistingCluster(jobName, description, existingClusterId, notebookPath, taskKey);

        assertTrue(result.isLeft());
    }

    @Test
    public void testCreateJobWithNewCluster_GitBRANCH() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                createJobGitSpecific(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);

        CreateResponse createResponse = new CreateResponse().setJobId(123L);
        when(workspaceClient.jobs().create(any())).thenReturn(createResponse);

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName,
                description,
                taskKey,
                runAs,
                jobClusterSpecific,
                schedulingSpecific,
                jobGitSpecific,
                "development");

        assertTrue(result.isRight());
        assertEquals(123L, result.get().longValue());

        verify(workspaceClient.jobs(), times(1)).create(any());
    }

    @Test
    public void testCreateJobWithNewCluster_GitTAG() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                createJobGitSpecific(DatabricksJobWorkloadSpecific.GitReferenceType.TAG);

        CreateResponse createResponse = new CreateResponse().setJobId(123L);
        when(workspaceClient.jobs().create(any())).thenReturn(createResponse);

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName,
                description,
                taskKey,
                runAs,
                jobClusterSpecific,
                schedulingSpecific,
                jobGitSpecific,
                "development");
        assertTrue(result.isRight());
        assertEquals(123L, result.get().longValue());

        verify(workspaceClient.jobs(), times(1)).create(any());
    }

    @Test
    public void testUpdateJobWithNewCluster_GitTAG() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                createJobGitSpecific(DatabricksJobWorkloadSpecific.GitReferenceType.TAG);

        List<BaseJob> baseJobList = Collections.singletonList(
                new BaseJob().setSettings(new JobSettings().setName("MyJob")).setJobId(123L));

        when(workspaceClient.jobs().list(any())).thenReturn(baseJobList);

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName,
                description,
                taskKey,
                runAs,
                jobClusterSpecific,
                schedulingSpecific,
                jobGitSpecific,
                "development");
        assertTrue(result.isRight());
        assertEquals(123L, result.get().longValue());

        verify(workspaceClient.jobs(), times(1)).update(any());
    }

    @Test
    public void testUpdateJobWithNewCluster_GitBRANCH() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                createJobGitSpecific(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);

        List<BaseJob> baseJobList = Collections.singletonList(
                new BaseJob().setSettings(new JobSettings().setName("MyJob")).setJobId(123L));

        when(workspaceClient.jobs().list(any())).thenReturn(baseJobList);

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName,
                description,
                taskKey,
                runAs,
                jobClusterSpecific,
                schedulingSpecific,
                jobGitSpecific,
                "development");
        assertTrue(result.isRight());
        assertEquals(123L, result.get().longValue());

        verify(workspaceClient.jobs(), times(1)).update(any());
    }

    @Test
    public void testUpdateJobWithNewCluster_JobNotUnique() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                createJobGitSpecific(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);

        List<BaseJob> baseJobList = Arrays.asList(
                new BaseJob().setSettings(new JobSettings().setName("MyJob")).setJobId(123L),
                new BaseJob().setSettings(new JobSettings().setName("MyJob")).setJobId(456L));

        when(workspaceClient.jobs().list(any())).thenReturn(baseJobList);

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName,
                description,
                taskKey,
                runAs,
                jobClusterSpecific,
                schedulingSpecific,
                jobGitSpecific,
                "development");
        assertTrue(result.isLeft());
        assertEquals(
                "Error trying to update the job 'MyJob'. The job name is not unique in workspace.",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testCreateJobWithNewCluster_Failure() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                createJobGitSpecific(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);

        when(workspaceClient.jobs().create(any())).thenThrow(new RuntimeException("Failed to create job"));

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName,
                description,
                taskKey,
                runAs,
                jobClusterSpecific,
                schedulingSpecific,
                jobGitSpecific,
                "development");
        assertTrue(result.isLeft());
    }

    @Test
    public void testUpdateJobWithNewCluster_Exception() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                createJobGitSpecific(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);

        List<BaseJob> baseJobList = Collections.singletonList(
                new BaseJob().setSettings(new JobSettings().setName("MyJob")).setJobId(123L));

        JobsAPI mockJobs = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(mockJobs);
        when(mockJobs.list(any())).thenReturn(baseJobList);
        doThrow(new DatabricksException("Exception")).when(mockJobs).update(any(UpdateJob.class));

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName,
                description,
                taskKey,
                runAs,
                jobClusterSpecific,
                schedulingSpecific,
                jobGitSpecific,
                "development");
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while updating the job MyJob in workspace. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testExportJob() {
        long jobId = 123L;
        Job job = new Job().setJobId(jobId);

        when(workspaceClient.jobs().get(jobId)).thenReturn(job);
        Either<FailedOperation, Job> result = jobManager.exportJob(jobId);

        assertTrue(result.isRight());
        verify(workspaceClient.jobs(), times(1)).get(jobId);
    }

    @Test
    public void testExportJob_Failure() {
        long jobId = 123L;
        when(workspaceClient.jobs().get(jobId)).thenThrow(new RuntimeException("Failed to export job"));

        Either<FailedOperation, Job> result = jobManager.exportJob(jobId);
        assertTrue(result.isLeft());
    }

    @Test
    public void deleteJob_Success() {
        long jobId = 123L;
        JobsAPI mockJobs = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(mockJobs);

        doNothing().when(mockJobs).delete(jobId);
        Either<FailedOperation, Void> result = jobManager.deleteJob(jobId);
        assertTrue(result.isRight());
    }

    @Test
    public void testDeleteJob_Exception() {
        Long jobId = 123L;
        when(workspaceClient.jobs()).thenReturn(null);
        Either<FailedOperation, Void> result = jobManager.deleteJob(jobId);

        assertTrue(result.isLeft());
        String errorMessage =
                "Cannot invoke \"com.databricks.sdk.service.jobs.JobsAPI.delete(long)\" because the return value of \"com.databricks.sdk.WorkspaceClient.jobs()\" is null";
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void testListJob_Exception() {
        when(workspaceClient.jobs()).thenReturn(null);
        Either<FailedOperation, Iterable<BaseJob>> result = jobManager.listJobsWithGivenName("name");

        assertTrue(result.isLeft());
        String errorMessage =
                "Cannot invoke \"com.databricks.sdk.service.jobs.JobsAPI.list(com.databricks.sdk.service.jobs.ListJobsRequest)\" because the return value of \"com.databricks.sdk.WorkspaceClient.jobs()\" is null";
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void testGetSparkEnvVarsForEnvironment_Development() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        Either<FailedOperation, Map<String, String>> result =
                jobManager.getSparkEnvVarsForEnvironment("development", jobClusterSpecific, jobName);

        assertTrue(result.isRight());
        assertNotNull(result.get().get("spark.env.var"));
        assertEquals("value", result.get().get("spark.env.var"));
    }

    @Test
    public void testGetSparkEnvVarsForEnvironment_QA() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        Either<FailedOperation, Map<String, String>> result =
                jobManager.getSparkEnvVarsForEnvironment("qa", jobClusterSpecific, jobName);

        assertTrue(result.isRight());
        assertNotNull(result.get().get("spark.env.var"));
        assertEquals("value", result.get().get("spark.env.var"));
    }

    @Test
    public void testGetSparkEnvVarsForEnvironment_Production() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        Either<FailedOperation, Map<String, String>> result =
                jobManager.getSparkEnvVarsForEnvironment("production", jobClusterSpecific, jobName);

        assertTrue(result.isRight());
        assertNotNull(result.get().get("spark.env.var"));
        assertEquals("value", result.get().get("spark.env.var"));
    }

    @Test
    public void testGetSparkEnvVarsForEnvironment_InvalidEnvironment() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        Either<FailedOperation, Map<String, String>> result =
                jobManager.getSparkEnvVarsForEnvironment("invalid_env", jobClusterSpecific, jobName);

        assertTrue(result.isLeft());
        assertEquals(
                String.format(
                        "An error occurred while getting the Spark environment variables for the job '%s' in the environment '%s'. The specified environment is invalid. Available options are: DEVELOPMENT, QA, PRODUCTION. Details: No enum constant it.agilelab.witboost.provisioning.databricks.model.Environment.INVALID_ENV",
                        jobName, "invalid_env"),
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testGetSparkEnvVarsForEnvironment_NullEnvironment() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        Either<FailedOperation, Map<String, String>> result =
                jobManager.getSparkEnvVarsForEnvironment(null, jobClusterSpecific, jobName);

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains("The specified environment is invalid"));
    }
}
