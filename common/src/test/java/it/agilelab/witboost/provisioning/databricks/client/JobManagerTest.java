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
import it.agilelab.witboost.provisioning.databricks.model.databricks.ClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitReferenceType;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.SchedulingSpecific;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

    //    @InjectMocks
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
        jobManager.createJobWithExistingCluster(jobName, description, existingClusterId, notebookPath, taskKey);

        verify(workspaceClient.jobs(), times(1)).create(any());
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

        assertFalse(result.isRight());
    }

    @Test
    public void testCreateJobWithNewCluster() {

        String jobName = "MyJob";
        String description = "Description of the job";
        String taskKey = "task123";
        String notebookPath = "path/to/notebook";
        ClusterSpecific clusterSpecific = new ClusterSpecific();
        clusterSpecific.setClusterSparkVersion("3.0.1");
        clusterSpecific.setNodeTypeId("node123");
        clusterSpecific.setNumWorkers(2L);
        clusterSpecific.setFirstOnDemand(1L);
        clusterSpecific.setSpotBidMaxPrice(0.5);
        clusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        clusterSpecific.setDriverNodeTypeId("driver123");
        clusterSpecific.setSparkConf(Collections.singletonMap("confKey", "confValue"));
        clusterSpecific.setSparkEnvVars(Collections.singletonMap("envKey", "envValue"));
        clusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
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
                jobName, description, taskKey, clusterSpecific, schedulingSpecific, gitSpecific);

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
        ClusterSpecific clusterSpecific = new ClusterSpecific();
        clusterSpecific.setClusterSparkVersion("3.0.1");
        clusterSpecific.setNodeTypeId("node123");
        clusterSpecific.setNumWorkers(2L);
        clusterSpecific.setFirstOnDemand(1L);
        clusterSpecific.setSpotBidMaxPrice(0.5);
        clusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        clusterSpecific.setDriverNodeTypeId("driver123");
        clusterSpecific.setSparkConf(Collections.singletonMap("confKey", "confValue"));
        clusterSpecific.setSparkEnvVars(Collections.singletonMap("envKey", "envValue"));
        clusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
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
                jobName, description, taskKey, clusterSpecific, schedulingSpecific, gitSpecific);

        assertFalse(result.isRight());
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
}
