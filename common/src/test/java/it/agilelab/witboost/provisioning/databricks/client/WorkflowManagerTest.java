package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WorkflowManagerTest {

    @Mock
    private WorkspaceClient workspaceClient;

    @InjectMocks
    private WorkflowManager workflowManager;

    @Mock
    private JobManager jobManager;

    @Mock
    private JobsAPI jobsAPI;

    private static final String workspaceName = "testWorkspace";
    private static final String jobName = "TestJob";
    private static final Long jobId = 123L;

    @BeforeEach
    public void setUp() {
        workflowManager = new WorkflowManager(workspaceClient, workspaceName);
    }

    @Test
    public void testCreateOrUpdateWorkflow_CreateNewWorkflow() {
        Job job = new Job();
        job.setSettings(new JobSettings().setName(jobName));
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.create(any(CreateJob.class))).thenReturn(new CreateResponse().setJobId(jobId));

        Either<FailedOperation, Long> result = workflowManager.createOrUpdateWorkflow(job);

        assertTrue(result.isRight());
        assertEquals(jobId, result.get());
        verify(workspaceClient.jobs(), times(1)).create(any(CreateJob.class));
    }

    @Test
    public void testCreateOrUpdateWorkflow_UpdateExistingWorkflow() {
        Job job = new Job();
        job.setJobId(jobId);
        JobSettings jobSettings = new JobSettings().setName(jobName);

        List<Task> existingTasks = new ArrayList<>();
        existingTasks.add(new Task().setTaskKey("taskTest1"));
        existingTasks.add(new Task().setTaskKey("taskTest2"));
        jobSettings.setTasks(existingTasks);

        List<JobCluster> existingClusters = new ArrayList<>();
        existingClusters.add(new JobCluster().setJobClusterKey("clusterKey1"));
        existingClusters.add(new JobCluster().setJobClusterKey("clusterKey2"));
        jobSettings.setJobClusters(existingClusters);

        job.setSettings(jobSettings);

        Job newJob = new Job();
        newJob.setJobId(jobId);
        JobSettings newJobSettings = new JobSettings().setName(jobName);

        List<Task> newTasks = new ArrayList<>();
        newTasks.add(new Task().setTaskKey("taskTest1"));
        newTasks.add(new Task().setTaskKey("taskTest3"));
        newJobSettings.setTasks(newTasks);

        List<JobCluster> newClusters = new ArrayList<>();
        newClusters.add(new JobCluster().setJobClusterKey("clusterKey1"));
        newClusters.add(new JobCluster().setJobClusterKey("clusterKey3"));
        newJobSettings.setJobClusters(newClusters);

        newJob.setSettings(newJobSettings);

        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.get(anyLong())).thenReturn(job);
        BaseJob existingJob = new BaseJob().setJobId(jobId).setSettings(jobSettings);
        when(jobsAPI.list(any())).thenReturn(List.of(existingJob));

        Either<FailedOperation, Long> result = workflowManager.createOrUpdateWorkflow(newJob);

        assertTrue(result.isRight());
        assertEquals(jobId, result.get());
        verify(workspaceClient.jobs(), times(1)).update(any(UpdateJob.class));
    }

    @Test
    public void testCreateOrUpdateWorkflow_WorkflowNotUnique() {
        Job job = new Job();
        job.setJobId(jobId);
        job.setSettings(new JobSettings().setName(jobName));
        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        BaseJob existingJob1 = new BaseJob().setJobId(jobId).setSettings(new JobSettings().setName(jobName));
        BaseJob existingJob2 = new BaseJob().setJobId(456l).setSettings(new JobSettings().setName(jobName));

        when(jobsAPI.list(any())).thenReturn(List.of(existingJob1, existingJob2));

        Either<FailedOperation, Long> result = workflowManager.createOrUpdateWorkflow(job);

        assertTrue(result.isLeft());
        String expectedErrorMessage =
                "Error trying to update the workflow 'TestJob'. The workflow name is not unique in testWorkspace.";
        assertEquals(expectedErrorMessage, result.getLeft().problems().get(0).description());
    }

    @Test
    public void testCreateWorkflow_Success() {
        Job job = new Job();
        job.setJobId(jobId);
        job.setSettings(new JobSettings().setName(jobName));
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(workspaceClient.jobs().create(any(CreateJob.class))).thenReturn(new CreateResponse().setJobId(jobId));

        Either<FailedOperation, Long> result = workflowManager.createWorkflow(job);

        assertTrue(result.isRight());
        assertEquals(jobId, result.get());
        verify(workspaceClient.jobs(), times(1)).create(any(CreateJob.class));
    }

    @Test
    public void testCreateWorkflow_Failure() {
        Job job = new Job();
        job.setSettings(new JobSettings().setName(jobName));
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(workspaceClient.jobs().create(any(CreateJob.class))).thenThrow(new RuntimeException("Creation failed"));

        Either<FailedOperation, Long> result = workflowManager.createWorkflow(job);

        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while creating the workflow TestJob in testWorkspace. Please try again and if the error persists contact the platform team. Details: Creation failed",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testUpdateWorkflow_Success() {
        Job job = new Job();
        job.setJobId(jobId);
        job.setSettings(new JobSettings().setName(jobName));
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(workspaceClient.jobs().get(job.getJobId())).thenReturn(job);
        Either<FailedOperation, Long> result = workflowManager.updateWorkflow(job);

        assertTrue(result.isRight());
        assertEquals(jobId, result.get());
        verify(workspaceClient.jobs(), times(1)).update(any(UpdateJob.class));
    }

    @Test
    public void testUpdateWorkflow_Failure() {
        Job job = new Job();
        job.setSettings(new JobSettings().setName(jobName));
        job.setJobId(123l);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.get(job.getJobId())).thenReturn(job);
        doThrow(new RuntimeException("Update failed")).when(jobsAPI).update(any(UpdateJob.class));

        Either<FailedOperation, Long> result = workflowManager.updateWorkflow(job);

        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while updating the workflow TestJob in testWorkspace. Please try again and if the error persists contact the platform team. Details: Update failed",
                result.getLeft().problems().get(0).description());
    }
}
