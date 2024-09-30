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
import it.agilelab.witboost.provisioning.databricks.model.databricks.SparkEnvVar;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.GitReferenceType;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.JobClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.JobGitSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.SchedulingSpecific;
import java.util.Arrays;
import java.util.List;
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
        SparkEnvVar sparkEnvVar = new SparkEnvVar();
        sparkEnvVar.setName("spark.env.var");
        sparkEnvVar.setValue("value");
        jobClusterSpecific.setSparkEnvVars(List.of(sparkEnvVar));
        jobClusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        return jobClusterSpecific;
    }

    private SchedulingSpecific createSchedulingSpecific() {
        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setJavaTimezoneId("UTC");
        schedulingSpecific.setCronExpression("0 0 12 * * ?");
        return schedulingSpecific;
    }

    private JobGitSpecific createJobGitSpecific(GitReferenceType referenceType) {
        JobGitSpecific jobGitSpecific = new JobGitSpecific();
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
        when(workspaceClient.jobs()).thenReturn(mockJobs);
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
        assertEquals(123l, result.get());
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
        SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        JobGitSpecific jobGitSpecific = createJobGitSpecific(GitReferenceType.BRANCH);

        CreateResponse createResponse = new CreateResponse().setJobId(123L);
        when(workspaceClient.jobs().create(any())).thenReturn(createResponse);

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, jobGitSpecific);

        assertTrue(result.isRight());
        assertEquals(123L, result.get().longValue());

        verify(workspaceClient.jobs(), times(1)).create(any());
    }

    @Test
    public void testCreateJobWithNewCluster_GitTAG() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        JobGitSpecific jobGitSpecific = createJobGitSpecific(GitReferenceType.TAG);

        CreateResponse createResponse = new CreateResponse().setJobId(123L);
        when(workspaceClient.jobs().create(any())).thenReturn(createResponse);

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, jobGitSpecific);

        assertTrue(result.isRight());
        assertEquals(123L, result.get().longValue());

        verify(workspaceClient.jobs(), times(1)).create(any());
    }

    @Test
    public void testUpdateJobWithNewCluster_GitTAG() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        JobGitSpecific jobGitSpecific = createJobGitSpecific(GitReferenceType.TAG);

        List<BaseJob> baseJobList = Arrays.asList(
                new BaseJob().setSettings(new JobSettings().setName("MyJob")).setJobId(123l));

        when(workspaceClient.jobs().list(any())).thenReturn(baseJobList);

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, jobGitSpecific);

        assertTrue(result.isRight());
        assertEquals(123L, result.get().longValue());

        verify(workspaceClient.jobs(), times(1)).update(any());
    }

    @Test
    public void testUpdateJobWithNewCluster_GitBRANCH() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        JobGitSpecific jobGitSpecific = createJobGitSpecific(GitReferenceType.BRANCH);

        List<BaseJob> baseJobList = Arrays.asList(
                new BaseJob().setSettings(new JobSettings().setName("MyJob")).setJobId(123l));

        when(workspaceClient.jobs().list(any())).thenReturn(baseJobList);

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, jobGitSpecific);

        assertTrue(result.isRight());
        assertEquals(123L, result.get().longValue());

        verify(workspaceClient.jobs(), times(1)).update(any());
    }

    @Test
    public void testUpdateJobWithNewCluster_JobNotUnique() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        JobGitSpecific jobGitSpecific = createJobGitSpecific(GitReferenceType.BRANCH);

        List<BaseJob> baseJobList = Arrays.asList(
                new BaseJob().setSettings(new JobSettings().setName("MyJob")).setJobId(123l),
                new BaseJob().setSettings(new JobSettings().setName("MyJob")).setJobId(456l));

        when(workspaceClient.jobs().list(any())).thenReturn(baseJobList);

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, jobGitSpecific);

        assertTrue(result.isLeft());
        assertEquals(
                "Error trying to update the job 'MyJob'. The job name is not unique in workspace.",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testCreateJobWithNewCluster_Failure() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        JobGitSpecific jobGitSpecific = createJobGitSpecific(GitReferenceType.BRANCH);

        when(workspaceClient.jobs().create(any())).thenThrow(new RuntimeException("Failed to create job"));

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, jobGitSpecific);

        assertTrue(result.isLeft());
    }

    @Test
    public void testUpdateJobWithNewCluster_Exception() {
        JobClusterSpecific jobClusterSpecific = createJobClusterSpecific();
        SchedulingSpecific schedulingSpecific = createSchedulingSpecific();
        JobGitSpecific jobGitSpecific = createJobGitSpecific(GitReferenceType.BRANCH);

        List<BaseJob> baseJobList = Arrays.asList(
                new BaseJob().setSettings(new JobSettings().setName("MyJob")).setJobId(123l));

        JobsAPI mockJobs = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(mockJobs);
        when(mockJobs.list(any())).thenReturn(baseJobList);
        doThrow(new DatabricksException("Exception")).when(mockJobs).update(any(UpdateJob.class));

        Either<FailedOperation, Long> result = jobManager.createOrUpdateJobWithNewCluster(
                jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, jobGitSpecific);

        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while updating the job MyJob in workspace. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testExportJob() {
        Long jobId = 123L;
        Job job = new Job().setJobId(jobId);

        when(workspaceClient.jobs().get(jobId)).thenReturn(job);
        Either<FailedOperation, Job> result = jobManager.exportJob(jobId);

        assertTrue(result.isRight());
        verify(workspaceClient.jobs(), times(1)).get(jobId);
    }

    @Test
    public void testExportJob_Failure() {
        Long jobId = 123L;
        when(workspaceClient.jobs().get(jobId)).thenThrow(new RuntimeException("Failed to export job"));

        Either<FailedOperation, Job> result = jobManager.exportJob(jobId);
        assertTrue(result.isLeft());
    }

    @Test
    public void testdeleteJob_Success() {
        Long jobId = 123L;
        JobsAPI mockJobs = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(mockJobs);

        doNothing().when(mockJobs).delete(jobId);
        Either<FailedOperation, Void> result = jobManager.deleteJob(jobId);
        assertTrue(result.isRight());
    }

    @Test
    public void testdeleteJob_Exception() {
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
}
