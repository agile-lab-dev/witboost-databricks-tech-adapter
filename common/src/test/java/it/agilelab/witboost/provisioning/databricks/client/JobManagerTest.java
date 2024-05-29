package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.compute.RuntimeEngine;
import com.databricks.sdk.service.jobs.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
@ExtendWith(MockitoExtension.class)
public class JobManagerTest {

    private JobManager jobManager;
    private WorkspaceClient workspaceClient;
    DatabricksConfig mockDatabricksConfig;

    @BeforeEach
    public void setUp() {
        jobManager = new JobManager();
        workspaceClient = mock(WorkspaceClient.class);

        JobsAPI mockJobs = mock(JobsAPI.class);
        mockDatabricksConfig = mock(DatabricksConfig.class);
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
        when(workspaceClient.config()).thenReturn(mockDatabricksConfig);
        jobManager.createJobWithExistingCluster(
                workspaceClient, jobName, description, existingClusterId, notebookPath, taskKey);

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

        Either<FailedOperation, Long> result = jobManager.createJobWithExistingCluster(
                workspaceClient, jobName, description, existingClusterId, notebookPath, taskKey);

        assertFalse(result.isRight());
    }

    @Test
    public void testCreateJobWithNewCluster() {

        String jobName = "MyJob";
        String description = "Description of the job";
        String taskKey = "task123";
        String notebookPath = "path/to/notebook";
        NewClusterParams newClusterParams = new NewClusterParams();
        newClusterParams.setSparkVersion("3.0.1");
        newClusterParams.setNodeTypeId("node123");
        newClusterParams.setNumWorkers(2L);
        newClusterParams.setFirstOnDemand(1L);
        newClusterParams.setSpotBidMaxPrice(0.5);
        newClusterParams.setAvailability("ON_DEMAND_AZURE");
        newClusterParams.setDriverNodeTypeId("driver123");
        newClusterParams.setSparkConf(Collections.singletonMap("confKey", "confValue"));
        newClusterParams.setSparkEnvVars(Collections.singletonMap("envKey", "envValue"));
        newClusterParams.setRuntimeEngine(RuntimeEngine.PHOTON);
        ScheduleParams scheduleParams = new ScheduleParams();
        scheduleParams.setTimeZoneId("UTC");
        scheduleParams.setCronExpression("0 0 12 * * ?");
        GitJobSource gitSource = new GitJobSource();
        gitSource.setGitUrl("https://github.com/user/repo.git");
        gitSource.setGitProvider(GitProvider.GIT_LAB);
        gitSource.setGitBranch("main");
        gitSource.setGitReferenceType(GitReferenceType.BRANCH);

        CreateResponse createResponse = new CreateResponse().setJobId(123L);
        when(workspaceClient.jobs().create(any())).thenReturn(createResponse);

        Either<FailedOperation, Long> result = jobManager.createJobWithNewCluster(
                workspaceClient,
                jobName,
                description,
                taskKey,
                newClusterParams,
                scheduleParams,
                gitSource,
                notebookPath);

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
        NewClusterParams newClusterParams = new NewClusterParams();
        newClusterParams.setSparkVersion("3.0.1");
        newClusterParams.setNodeTypeId("node123");
        newClusterParams.setNumWorkers(2L);
        newClusterParams.setFirstOnDemand(1L);
        newClusterParams.setSpotBidMaxPrice(0.5);
        newClusterParams.setAvailability("ON_DEMAND_AZURE");
        newClusterParams.setDriverNodeTypeId("driver123");
        newClusterParams.setSparkConf(Collections.singletonMap("confKey", "confValue"));
        newClusterParams.setSparkEnvVars(Collections.singletonMap("envKey", "envValue"));
        newClusterParams.setRuntimeEngine(RuntimeEngine.PHOTON);
        ScheduleParams scheduleParams = new ScheduleParams();
        scheduleParams.setTimeZoneId("UTC");
        scheduleParams.setCronExpression("0 0 12 * * ?");
        GitJobSource gitSource = new GitJobSource();
        gitSource.setGitUrl("https://github.com/user/repo.git");
        gitSource.setGitProvider(GitProvider.GIT_LAB);
        gitSource.setGitBranch("main");
        gitSource.setGitReferenceType(GitReferenceType.BRANCH);

        when(workspaceClient.jobs().create(any())).thenThrow(new RuntimeException("Failed to create job"));

        Either<FailedOperation, Long> result = jobManager.createJobWithNewCluster(
                workspaceClient,
                jobName,
                description,
                taskKey,
                newClusterParams,
                scheduleParams,
                gitSource,
                notebookPath);

        assertFalse(result.isRight());
    }

    @Test
    public void testExportJob() {
        Long jobId = 123L;
        Job job = new Job().setJobId(jobId);

        when(workspaceClient.jobs().get(jobId)).thenReturn(job);
        Either<FailedOperation, Job> result = jobManager.exportJob(workspaceClient, jobId);

        assertTrue(result.isRight());

        verify(workspaceClient.jobs(), times(1)).get(jobId);
    }

    @Test
    public void testExportJob_Failure() {
        Long jobId = 123L;

        when(workspaceClient.jobs().get(jobId)).thenThrow(new RuntimeException("Failed to export job"));

        Either<FailedOperation, Job> result = jobManager.exportJob(workspaceClient, jobId);
        assertFalse(result.isRight()); // Check if result is a Left, indicating a failure
    }
}
