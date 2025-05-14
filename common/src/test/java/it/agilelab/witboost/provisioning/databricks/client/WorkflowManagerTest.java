package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.mixin.ClustersExt;
import com.databricks.sdk.service.compute.ClusterDetails;
import com.databricks.sdk.service.jobs.*;
import com.databricks.sdk.service.pipelines.GetPipelineResponse;
import com.databricks.sdk.service.pipelines.ListPipelinesRequest;
import com.databricks.sdk.service.pipelines.PipelineStateInfo;
import com.databricks.sdk.service.pipelines.PipelinesAPI;
import com.databricks.sdk.service.sql.GetWarehouseResponse;
import com.databricks.sdk.service.sql.WarehousesAPI;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WorkflowManagerTest {

    @Mock
    private WorkspaceClient workspaceClient;

    @InjectMocks
    private WorkflowManager workflowManager;

    @Mock
    WorkspaceLevelManagerFactory workspaceLevelManagerFactory;

    @Mock
    private JobsAPI jobsAPI;

    private static final String workspaceName = "testWorkspace";
    private static final String jobName = "TestJob";
    private static final Long jobId = 123L;

    @BeforeEach
    public void setUp() {
        Mockito.reset(workspaceClient, jobsAPI, workspaceLevelManagerFactory);

        workflowManager = new WorkflowManager(workspaceClient, workspaceName, workspaceLevelManagerFactory);
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
        BaseJob existingJob = new BaseJob().setJobId(jobId).setSettings(jobSettings);
        when(jobsAPI.list(any())).thenReturn(List.of(existingJob));

        Either<FailedOperation, Long> result = workflowManager.createOrUpdateWorkflow(newJob);

        assertTrue(result.isRight());
        assertEquals(jobId, result.get());
        verify(workspaceClient.jobs(), times(1)).reset(anyLong(), any(JobSettings.class));
    }

    @Test
    public void testCreateOrUpdateWorkflow_WorkflowNotUnique() {
        Job job = new Job();
        job.setJobId(jobId);
        job.setSettings(new JobSettings().setName(jobName));
        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        BaseJob existingJob1 = new BaseJob().setJobId(jobId).setSettings(new JobSettings().setName(jobName));
        BaseJob existingJob2 = new BaseJob().setJobId(456L).setSettings(new JobSettings().setName(jobName));

        when(jobsAPI.list(any())).thenReturn(List.of(existingJob1, existingJob2));

        Either<FailedOperation, Long> result = workflowManager.createOrUpdateWorkflow(job);

        assertTrue(result.isLeft());
        String expectedErrorMessage =
                "Error trying to update the workflow 'TestJob'. The workflow name is not unique in testWorkspace.";
        assertEquals(expectedErrorMessage, result.getLeft().problems().get(0).description());
    }

    @Test
    public void testCreateOrUpdateWorkflow_ListJobsFailure() {
        when(workspaceClient.jobs()).thenReturn(mock(JobsAPI.class));
        when(workspaceClient.jobs().list(any(ListJobsRequest.class)))
                .thenThrow(new RuntimeException("Error listing jobs"));

        Job job = new Job().setSettings(new JobSettings().setName("JobName"));

        Either<FailedOperation, Long> result = workflowManager.createOrUpdateWorkflow(job);

        assertTrue(result.isLeft());
        assert (result.getLeft().problems().get(0).description().contains("Error listing jobs"));
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
        Either<FailedOperation, Long> result = workflowManager.updateWorkflow(job);

        assertTrue(result.isRight());
        assertEquals(jobId, result.get());
        verify(workspaceClient.jobs(), times(1)).reset(anyLong(), any(JobSettings.class));
    }

    @Test
    public void testUpdateWorkflow_Failure() {
        Job job = new Job();
        job.setSettings(new JobSettings().setName(jobName));
        job.setJobId(123L);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        doThrow(new RuntimeException("Update failed")).when(jobsAPI).reset(anyLong(), any(JobSettings.class));

        Either<FailedOperation, Long> result = workflowManager.updateWorkflow(job);

        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while updating the workflow TestJob in testWorkspace. Please try again and if the error persists contact the platform team. Details: Update failed",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testGetWorkflowTaskInfoFromId_NotebookCompute_Success() {
        Task task = new Task()
                .setTaskKey("computeTaskKey")
                .setNotebookTask(new NotebookTask())
                .setExistingClusterId("cluster123");

        ClustersExt clustersAPI = mock(ClustersExt.class);
        when(workspaceClient.clusters()).thenReturn(clustersAPI);
        when(clustersAPI.get("cluster123")).thenReturn(new ClusterDetails().setClusterName("Cluster Name"));

        Either<FailedOperation, Optional<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo>> result =
                workflowManager.getWorkflowTaskInfoFromId(task);

        assertTrue(result.isRight());
        assertTrue(result.get().isPresent());

        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo taskInfo =
                result.get().get();
        assertEquals("computeTaskKey", taskInfo.getTaskKey());
        assertEquals("notebook_compute", taskInfo.getReferencedTaskType());
        assertEquals("Cluster Name", taskInfo.getReferencedClusterName());
        assertEquals("cluster123", taskInfo.getReferencedClusterId());

        verify(workspaceClient.clusters(), times(1)).get("cluster123");
    }

    @Test
    public void testGetWorkflowTaskInfoFromId_PipelineTask_Success() {
        Task task = new Task().setTaskKey("pipelineTaskKey");
        PipelineTask pipelineTask = new PipelineTask().setPipelineId("pipeline123");
        task.setPipelineTask(pipelineTask);

        var pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);
        when(pipelinesAPI.get("pipeline123")).thenReturn(new GetPipelineResponse().setName("Pipeline Name"));

        Either<FailedOperation, Optional<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo>> result =
                workflowManager.getWorkflowTaskInfoFromId(task);

        assertTrue(result.isRight());
        assertTrue(result.get().isPresent());
        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo taskInfo =
                result.get().get();
        assertEquals("pipelineTaskKey", taskInfo.getTaskKey());
        assertEquals("pipeline", taskInfo.getReferencedTaskType());
        assertEquals("Pipeline Name", taskInfo.getReferencedTaskName());
        assertEquals("pipeline123", taskInfo.getReferencedTaskId());

        verify(workspaceClient.pipelines(), times(1)).get("pipeline123");
    }

    @Test
    public void testGetWorkflowTaskInfoFromId_NotebookWarehouseTask_Success() {
        Task task = new Task().setTaskKey("notebookWarehouseTaskKey");
        NotebookTask notebookTask = new NotebookTask().setWarehouseId("warehouseId123");
        task.setNotebookTask(notebookTask);

        var warehousesAPI = mock(WarehousesAPI.class);
        when(workspaceClient.warehouses()).thenReturn(warehousesAPI);
        when(warehousesAPI.get("warehouseId123")).thenReturn(new GetWarehouseResponse().setName("Warehouse Name"));

        Either<FailedOperation, Optional<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo>> result =
                workflowManager.getWorkflowTaskInfoFromId(task);

        assertTrue(result.isRight());
        assertTrue(result.get().isPresent());
        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo taskInfo =
                result.get().get();
        assertEquals("notebookWarehouseTaskKey", taskInfo.getTaskKey());
        assertEquals("notebook_warehouse", taskInfo.getReferencedTaskType());
        assertEquals("Warehouse Name", taskInfo.getReferencedClusterName());
        assertEquals("warehouseId123", taskInfo.getReferencedClusterId());

        verify(workspaceClient.warehouses(), times(1)).get("warehouseId123");
    }

    @Test
    public void testGetWorkflowTaskInfoFromId_JobTask_Success() {
        Task task = new Task().setTaskKey("jobTaskKey");
        RunJobTask runJobTask = new RunJobTask().setJobId(123L);
        task.setRunJobTask(runJobTask);

        Job job = new Job().setSettings(new JobSettings().setName("Job Name"));
        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.get(123L)).thenReturn(job);

        Either<FailedOperation, Optional<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo>> result =
                workflowManager.getWorkflowTaskInfoFromId(task);

        assertTrue(result.isRight());
        assertTrue(result.get().isPresent());
        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo workflowTaskInfo =
                result.get().get();
        assertEquals("jobTaskKey", workflowTaskInfo.getTaskKey());
        assertEquals("job", workflowTaskInfo.getReferencedTaskType());
        assertEquals("Job Name", workflowTaskInfo.getReferencedTaskName());
        assertEquals("123", workflowTaskInfo.getReferencedTaskId());
    }

    @Test
    public void testGetWorkflowTaskInfoFromId_JobTask_Failed() {
        Task task = new Task().setTaskKey("jobTaskKey");
        RunJobTask runJobTask = new RunJobTask().setJobId(123L);
        task.setRunJobTask(runJobTask);

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.get(123L)).thenThrow(new RuntimeException("Job retrieval failed"));

        Either<FailedOperation, Optional<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo>> result =
                workflowManager.getWorkflowTaskInfoFromId(task);

        assertTrue(result.isLeft());
        FailedOperation failedOperation = result.getLeft();
        assertEquals(1, failedOperation.problems().size());
        assertEquals(
                "An error occurred while retrieving workflow tasks info for task 'jobTaskKey' in testWorkspace. Please try again and if the error persists contact the platform team. Details: Job retrieval failed",
                failedOperation.problems().get(0).description());
    }

    @Test
    public void testGetWorkflowTaskInfoFromId_TaskTypeUnknown() {
        Task task = new Task().setTaskKey("unknownTaskKey");

        Either<FailedOperation, Optional<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo>> result =
                workflowManager.getWorkflowTaskInfoFromId(task);

        assertTrue(result.isRight());
        assertTrue(result.get().isEmpty());
    }

    @Test
    public void testGetWorkflowTaskInfoFromId_ExceptionOccurs() {
        Task task = new Task().setTaskKey("pipelineTaskKey");
        PipelineTask pipelineTask = new PipelineTask().setPipelineId("pipeline123");
        task.setPipelineTask(pipelineTask);

        var pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);
        when(pipelinesAPI.get("pipeline123")).thenThrow(new RuntimeException("Error retrieving pipeline"));

        Either<FailedOperation, Optional<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo>> result =
                workflowManager.getWorkflowTaskInfoFromId(task);

        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while retrieving workflow tasks info for task 'pipelineTaskKey' in testWorkspace. Please try again and if the error persists contact the platform team. Details: Error retrieving pipeline",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testCreateTaskFromWorkflowTaskInfo_PipelineTask_Success() {
        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo wfInfo =
                new DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo();
        wfInfo.setReferencedTaskType("pipeline");
        wfInfo.setReferencedTaskName("Pipeline Name");
        wfInfo.setReferencedTaskId("pipeline123");

        Task task = new Task().setTaskKey("taskKey");
        PipelineTask pipelineTask = new PipelineTask();
        task.setPipelineTask(pipelineTask);

        when(workspaceClient.pipelines()).thenReturn(mock(PipelinesAPI.class));
        when(workspaceClient.pipelines().listPipelines(any(ListPipelinesRequest.class)))
                .thenReturn(
                        List.of(new PipelineStateInfo().setName("Pipeline Name").setPipelineId("newPipeline123")));

        when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(workspaceClient))
                .thenReturn(mock(WorkspaceLevelManager.class));

        Either<FailedOperation, Task> result = workflowManager.createTaskFromWorkflowTaskInfo(wfInfo, task);

        assertTrue(result.isRight());
        assertNotNull(result.get().getPipelineTask());
        assertEquals("newPipeline123", result.get().getPipelineTask().getPipelineId());
    }

    @Test
    public void testCreateTaskFromWorkflowTaskInfo_JobTask_Success() {
        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo wfInfo =
                new DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo();
        wfInfo.setReferencedTaskType("job");
        wfInfo.setReferencedTaskName("Job Name");
        wfInfo.setReferencedTaskId("job123");

        Task task = new Task().setTaskKey("taskKey");
        RunJobTask runJobTask = new RunJobTask();
        task.setRunJobTask(runJobTask);

        when(workspaceClient.jobs()).thenReturn(mock(JobsAPI.class));
        when(workspaceClient.jobs().list(any(ListJobsRequest.class)))
                .thenReturn(List.of(new BaseJob().setJobId(123L).setSettings(new JobSettings().setName("Job Name"))));

        Either<FailedOperation, Task> result = workflowManager.createTaskFromWorkflowTaskInfo(wfInfo, task);

        assertTrue(result.isRight());
        assertNotNull(result.get().getRunJobTask());
        assertEquals(123L, result.get().getRunJobTask().getJobId().longValue());
    }

    @Test
    public void testCreateTaskFromWorkflowTaskInfo_NotebookWarehouse_Success() {
        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo wfInfo =
                new DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo();
        wfInfo.setReferencedTaskType("notebook_warehouse");
        wfInfo.setReferencedClusterName("Warehouse Name");
        wfInfo.setReferencedClusterId("warehouse123");

        Task task = new Task().setTaskKey("taskKey");
        NotebookTask notebookTask = new NotebookTask();
        task.setNotebookTask(notebookTask);

        WorkspaceLevelManager workspaceLevelManager = mock(WorkspaceLevelManager.class);
        when(workspaceLevelManager.getSqlWarehouseIdFromName("Warehouse Name"))
                .thenReturn(Either.right("newWarehouse123"));
        when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(workspaceClient))
                .thenReturn(workspaceLevelManager);

        Either<FailedOperation, Task> result = workflowManager.createTaskFromWorkflowTaskInfo(wfInfo, task);

        assertTrue(result.isRight());
        assertNotNull(result.get().getNotebookTask());
        assertEquals("newWarehouse123", result.get().getNotebookTask().getWarehouseId());
    }

    @Test
    public void testCreateTaskFromWorkflowTaskInfo_NotebookCompute_Success() {
        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo wfInfo =
                new DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo();
        wfInfo.setReferencedTaskType("notebook_compute");
        wfInfo.setReferencedClusterName("Compute Cluster Name");
        wfInfo.setReferencedClusterId("cluster123");

        Task task = new Task().setTaskKey("taskKey").setExistingClusterId("cluster123");

        WorkspaceLevelManager workspaceLevelManager = mock(WorkspaceLevelManager.class);
        when(workspaceLevelManager.getComputeClusterIdFromName("Compute Cluster Name"))
                .thenReturn(Either.right("newCluster123"));
        when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(workspaceClient))
                .thenReturn(workspaceLevelManager);

        Either<FailedOperation, Task> result = workflowManager.createTaskFromWorkflowTaskInfo(wfInfo, task);

        assertTrue(result.isRight());
        assertNotNull(result.get().getExistingClusterId());
        assertEquals("newCluster123", result.get().getExistingClusterId());
    }

    @Test
    public void testCreateTaskFromWorkflowTaskInfo_ExceptionOccurs() {
        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo wfInfo =
                new DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo();
        wfInfo.setReferencedTaskType("pipeline");
        wfInfo.setReferencedTaskName("Pipeline Name");
        wfInfo.setReferencedTaskId("pipeline123");

        Task task = new Task().setTaskKey("taskKey");
        PipelineTask pipelineTask = new PipelineTask();
        task.setPipelineTask(pipelineTask);

        when(workspaceClient.pipelines()).thenReturn(mock(PipelinesAPI.class));
        when(workspaceClient.pipelines().listPipelines(any(ListPipelinesRequest.class)))
                .thenReturn(Collections.emptyList());

        when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(workspaceClient))
                .thenReturn(mock(WorkspaceLevelManager.class));

        Either<FailedOperation, Task> result = workflowManager.createTaskFromWorkflowTaskInfo(wfInfo, task);

        assertTrue(result.isLeft());
        assert (result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains(
                        "An error occurred while searching pipeline 'Pipeline Name' in testWorkspace: no DLT found with that name."));
    }

    @Test
    public void testCreateTaskFromWorkflowTaskInfo_PipelineTask_Failure() {
        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo wfInfo =
                new DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo();
        wfInfo.setReferencedTaskType("pipeline");
        wfInfo.setReferencedTaskName("PipelineName");
        wfInfo.setReferencedTaskId("PipelineId");

        Task task = new Task();
        when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(workspaceClient))
                .thenReturn(mock(WorkspaceLevelManager.class));
        when(workspaceClient.pipelines()).thenThrow(new RuntimeException("Error abc"));
        Either<FailedOperation, Task> result = workflowManager.createTaskFromWorkflowTaskInfo(wfInfo, task);

        assertTrue(result.isLeft());
        assert (result.getLeft().problems().get(0).description().contains("Error abc"));
    }

    @Test
    public void testReconstructJobWithCorrectIds_Success() {
        Task originalTask =
                new Task().setTaskKey("task1").setPipelineTask(new PipelineTask().setPipelineId("oldPipelineId"));
        Job originalWorkflow = new Job().setSettings(new JobSettings().setTasks(List.of(originalTask)));

        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo taskInfo =
                new DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo();
        taskInfo.setTaskKey("task1");
        taskInfo.setReferencedTaskId("newPipelineId");
        taskInfo.setReferencedTaskName("task1");
        taskInfo.setReferencedTaskType("pipeline");

        when(workspaceClient.pipelines()).thenReturn(mock(PipelinesAPI.class));
        when(workspaceClient.pipelines().listPipelines(any(ListPipelinesRequest.class)))
                .thenReturn(List.of(new PipelineStateInfo().setName("task1").setPipelineId("newPipelineId")));

        Either<FailedOperation, Job> result =
                workflowManager.reconstructJobWithCorrectIds(originalWorkflow, List.of(taskInfo));

        assertTrue(result.isRight());
        Job updatedWorkflow = result.get();
        assertEquals(1, updatedWorkflow.getSettings().getTasks().size());
        Task resultTask = updatedWorkflow.getSettings().getTasks().iterator().next();
        assertEquals("task1", resultTask.getTaskKey());
        assertEquals("newPipelineId", resultTask.getPipelineTask().getPipelineId());
    }

    @Test
    public void testReconstructJobWithCorrectIds_TaskUpdateFails() {
        Task originalTask =
                new Task().setTaskKey("task1").setPipelineTask(new PipelineTask().setPipelineId("oldPipelineId"));
        Job originalWorkflow = new Job().setSettings(new JobSettings().setTasks(List.of(originalTask)));

        DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo taskInfo =
                new DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo();
        taskInfo.setTaskKey("task1");
        taskInfo.setReferencedTaskId("newPipelineId");
        taskInfo.setReferencedTaskName("task1");
        taskInfo.setReferencedTaskType("pipeline");

        when(workspaceClient.pipelines()).thenThrow(new RuntimeException("Error getting pipelines"));

        Either<FailedOperation, Job> result =
                workflowManager.reconstructJobWithCorrectIds(originalWorkflow, List.of(taskInfo));

        assertTrue(result.isLeft());
        assert result.getLeft().problems().get(0).description().contains("Error getting pipelines");
    }

    @Test
    public void testRetrieveTaskType_PipelineTask() {
        Task task = new Task().setPipelineTask(new PipelineTask());
        Optional<String> result = workflowManager.retrieveTaskType(task);
        assertTrue(result.isPresent());
        assertEquals("pipeline", result.get());
    }

    @Test
    public void testRetrieveTaskType_RunJobTask() {
        Task task = new Task().setRunJobTask(new RunJobTask());
        Optional<String> result = workflowManager.retrieveTaskType(task);
        assertTrue(result.isPresent());
        assertEquals("job", result.get());
    }

    @Test
    public void testRetrieveTaskType_NotebookComputeTask() {
        NotebookTask notebookTask = new NotebookTask();
        Task task = new Task().setNotebookTask(notebookTask).setExistingClusterId("cluster123");
        Optional<String> result = workflowManager.retrieveTaskType(task);
        assertTrue(result.isPresent());
        assertEquals("notebook_compute", result.get());
    }

    @Test
    public void testRetrieveTaskType_NotebookWarehouseTask() {
        NotebookTask notebookTask = new NotebookTask().setWarehouseId("warehouse123");
        Task task = new Task().setNotebookTask(notebookTask);
        Optional<String> result = workflowManager.retrieveTaskType(task);
        assertTrue(result.isPresent());
        assertEquals("notebook_warehouse", result.get());
    }

    @Test
    public void testRetrieveTaskType_UnknownTask() {
        Task task = new Task();
        Optional<String> result = workflowManager.retrieveTaskType(task);
        assertFalse(result.isPresent());
    }

    @Test
    public void testReconstructJobWithCorrectIds_TaskWithoutMatchingInfo() {
        Task originalTask = new Task()
                .setTaskKey("taskWithoutInfo")
                .setPipelineTask(new PipelineTask().setPipelineId("pipeline123"));

        Job originalWorkflow = new Job().setSettings(new JobSettings().setTasks(List.of(originalTask)));

        List<DatabricksWorkflowWorkloadSpecific.WorkflowTasksInfo> workflowInfoList = Collections.emptyList();

        Either<FailedOperation, Job> result =
                workflowManager.reconstructJobWithCorrectIds(originalWorkflow, workflowInfoList);

        assertTrue(result.isRight());
        Job updatedWorkflow = result.get();

        assertEquals(1, updatedWorkflow.getSettings().getTasks().size());
        Task resultTask = updatedWorkflow.getSettings().getTasks().iterator().next();
        assertEquals("taskWithoutInfo", resultTask.getTaskKey());
        assertEquals("pipeline123", resultTask.getPipelineTask().getPipelineId());
    }
}
