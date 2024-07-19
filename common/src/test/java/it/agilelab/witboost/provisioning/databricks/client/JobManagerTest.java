package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.compute.AzureAvailability;
import com.databricks.sdk.service.compute.RuntimeEngine;
import com.databricks.sdk.service.jobs.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.GitReferenceType;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.GitSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.JobClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.SchedulingSpecific;
import java.util.Collections;
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

    DatabricksConfig mockDatabricksConfig;

    @BeforeEach
    public void setUp() {
        jobManager = new JobManager(workspaceClient, "workspace");

        JobsAPI mockJobs = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(mockJobs);
    }

    @Test
    public void testCreateJobWithExistingCluster() {
        String jobName = "Test Job";
        String description = "Test job description";
        String existingClusterId = "existingCluster123";
        String notebookPath = "/path/to/notebook";
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
        String jobName = "Test Job";
        String description = "Test job description";
        String existingClusterId = "existingCluster123";
        String notebookPath = "/path/to/notebook";
        String taskKey = "task123";

        CreateResponse createResponse = new CreateResponse().setJobId(123L);
        when(workspaceClient.jobs().create(any())).thenThrow(new RuntimeException("Failed to create job"));

        Either<FailedOperation, Long> result =
                jobManager.createJobWithExistingCluster(jobName, description, existingClusterId, notebookPath, taskKey);

        assertTrue(result.isLeft());
    }

    @Test
    public void testCreateJobWithNewCluster_GitBRANCH() {

        String jobName = "MyJob";
        String description = "Description of the job";
        String taskKey = "task123";
        String notebookPath = "path/to/notebook";
        JobClusterSpecific jobClusterSpecific = new JobClusterSpecific();
        jobClusterSpecific.setClusterSparkVersion("3.0.1");
        jobClusterSpecific.setNodeTypeId("node123");
        jobClusterSpecific.setNumWorkers(2L);
        jobClusterSpecific.setFirstOnDemand(1L);
        jobClusterSpecific.setSpotBidMaxPrice(0.5);
        jobClusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        jobClusterSpecific.setDriverNodeTypeId("driver123");
        jobClusterSpecific.setSparkConf(Collections.singletonMap("confKey", "confValue"));
        jobClusterSpecific.setSparkEnvVars(Collections.singletonMap("envKey", "envValue"));
        jobClusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setJavaTimezoneId("UTC");
        schedulingSpecific.setCronExpression("0 0 12 * * ?");
        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitRepoUrl("https://github.com/user/repo.git");
        gitSpecific.setGitProvider(GitProvider.GIT_LAB);
        gitSpecific.setGitReference("main");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);

        CreateResponse createResponse = new CreateResponse().setJobId(123L);
        when(workspaceClient.jobs().create(any())).thenReturn(createResponse);

        Either<FailedOperation, Long> result = jobManager.createJobWithNewCluster(
                jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, gitSpecific);

        assertTrue(result.isRight());
        assertEquals(123L, result.get().longValue());

        verify(workspaceClient.jobs(), times(1)).create(any());
    }

    @Test
    public void testCreateJobWithNewCluster_GitTAG() {

        String jobName = "MyJob";
        String description = "Description of the job";
        String taskKey = "task123";
        String notebookPath = "path/to/notebook";
        JobClusterSpecific jobClusterSpecific = new JobClusterSpecific();
        jobClusterSpecific.setClusterSparkVersion("3.0.1");
        jobClusterSpecific.setNodeTypeId("node123");
        jobClusterSpecific.setNumWorkers(2L);
        jobClusterSpecific.setFirstOnDemand(1L);
        jobClusterSpecific.setSpotBidMaxPrice(0.5);
        jobClusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        jobClusterSpecific.setDriverNodeTypeId("driver123");
        jobClusterSpecific.setSparkConf(Collections.singletonMap("confKey", "confValue"));
        jobClusterSpecific.setSparkEnvVars(Collections.singletonMap("envKey", "envValue"));
        jobClusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setJavaTimezoneId("UTC");
        schedulingSpecific.setCronExpression("0 0 12 * * ?");
        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitRepoUrl("https://github.com/user/repo.git");
        gitSpecific.setGitProvider(GitProvider.GIT_LAB);
        gitSpecific.setGitReference("main");
        gitSpecific.setGitReferenceType(GitReferenceType.TAG);

        CreateResponse createResponse = new CreateResponse().setJobId(123L);
        when(workspaceClient.jobs().create(any())).thenReturn(createResponse);

        Either<FailedOperation, Long> result = jobManager.createJobWithNewCluster(
                jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, gitSpecific);

        assertTrue(result.isRight());
        assertEquals(123L, result.get().longValue());

        verify(workspaceClient.jobs(), times(1)).create(any());
    }

    @Test
    public void testCreateJobWithNewCluster_Failure() {
        String jobName = "MyJob";
        String description = "Description of the job";
        String taskKey = "task123";
        String notebookPath = "path/to/notebook";
        JobClusterSpecific jobClusterSpecific = new JobClusterSpecific();
        jobClusterSpecific.setClusterSparkVersion("3.0.1");
        jobClusterSpecific.setNodeTypeId("node123");
        jobClusterSpecific.setNumWorkers(2L);
        jobClusterSpecific.setFirstOnDemand(1L);
        jobClusterSpecific.setSpotBidMaxPrice(0.5);
        jobClusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        jobClusterSpecific.setDriverNodeTypeId("driver123");
        jobClusterSpecific.setSparkConf(Collections.singletonMap("confKey", "confValue"));
        jobClusterSpecific.setSparkEnvVars(Collections.singletonMap("envKey", "envValue"));
        jobClusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setJavaTimezoneId("UTC");
        schedulingSpecific.setCronExpression("0 0 12 * * ?");
        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitRepoUrl("https://github.com/user/repo.git");
        gitSpecific.setGitProvider(GitProvider.GIT_LAB);
        gitSpecific.setGitReference("main");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);

        when(workspaceClient.jobs().create(any())).thenThrow(new RuntimeException("Failed to create job"));

        Either<FailedOperation, Long> result = jobManager.createJobWithNewCluster(
                jobName, description, taskKey, jobClusterSpecific, schedulingSpecific, gitSpecific);

        assertTrue(result.isLeft());
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
