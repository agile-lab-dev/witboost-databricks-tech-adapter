package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.jobs.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest.*;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WorkflowReverseProvisionHandlerTest {

    @Mock
    private WorkspaceClient workspaceClient;

    @Mock
    private WorkspaceHandler workspaceHandler;

    @InjectMocks
    private WorkflowReverseProvisionHandler workflowReverseProvision;

    private final DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspaceName", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);

    private final String useCaseTemplateId = "urn:dmb:utm:databricks-workload-workflow-template:0.0.0";
    private final String workspaceName = "workspaceName";

    private ReverseProvisioningRequest createRequest() {
        ReverseProvisioningRequest request = new ReverseProvisioningRequest();
        request.setEnvironment("qa");
        request.setUseCaseTemplateId(useCaseTemplateId);

        CatalogInfo catalogInfo = new CatalogInfo();
        CatalogInfo.Spec.Mesh mesh = new CatalogInfo.Spec.Mesh();
        mesh.setName("componentName");
        CatalogInfo.Spec spec = new CatalogInfo.Spec();
        spec.setMesh(mesh);
        catalogInfo.setSpec(spec);
        request.setCatalogInfo(catalogInfo);

        WorkflowReverseProvisioningParams params = new WorkflowReverseProvisioningParams();
        EnvironmentSpecificConfig environmentSpecificConfig = new EnvironmentSpecificConfig();
        WorkflowReverseProvisioningParams.WorkflowReverseProvisioningSpecific specific =
                new WorkflowReverseProvisioningParams.WorkflowReverseProvisioningSpecific();
        specific.setWorkspace(workspaceName);

        Job workflow = new Job();
        workflow.setJobId(123L);
        workflow.setSettings(new JobSettings().setName("jobName"));
        specific.setWorkflow(workflow);

        environmentSpecificConfig.setSpecific(specific);
        params.setEnvironmentSpecificConfig(environmentSpecificConfig);
        request.setParams(params);

        return request;
    }

    @Test
    public void testReverseProvisioning_Success() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(Either.right(workspaceClient));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        List<BaseJob> baseJobList = new ArrayList<>();
        baseJobList.add(new BaseJob().setJobId(123L));
        when(jobsAPI.list(any())).thenReturn(baseJobList);

        Job job = new Job();
        job.setJobId(123L);
        job.setSettings(new JobSettings().setName("jobNameNew"));
        when(jobsAPI.get(anyLong())).thenReturn(job);

        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        Either<FailedOperation, LinkedHashMap<Object, Object>> result =
                workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assert (result.isRight());
    }

    @Test
    public void testReverseProvisioning_WorkspaceInfoError() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        String errorMessage = "An error occurred getting info of the workspace ABC";
        when(workspaceHandler.getWorkspaceInfo(workspaceName))
                .thenReturn(Either.left(new FailedOperation(Collections.singletonList(new Problem(errorMessage)))));

        Either<FailedOperation, LinkedHashMap<Object, Object>> result =
                workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assert (result.isLeft());
        assert result.getLeft().problems().get(0).getMessage().contains(errorMessage);
    }

    @Test
    public void testReverseProvisioning_WorkspaceClientError() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        String errorMessage = "An error occurred getting client of the workspace ABC";
        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo))
                .thenReturn(Either.left(new FailedOperation(Collections.singletonList(new Problem(errorMessage)))));

        Either<FailedOperation, LinkedHashMap<Object, Object>> result =
                workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assert (result.isLeft());
        assert result.getLeft().problems().get(0).getMessage().contains(errorMessage);
    }

    @Test
    public void testReverseProvisioning_WorkspaceEmptyInfo() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.empty()));

        Either<FailedOperation, LinkedHashMap<Object, Object>> result =
                workflowReverseProvision.reverseProvision(reverseProvisioningRequest);

        assert (result.isLeft());
        assert result.getLeft()
                .problems()
                .get(0)
                .getMessage()
                .contains("Validation failed. Workspace 'workspaceName' not found.");
    }

    @Test
    public void testReverseProvisioning_ValidationFailedException() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        String errorMessage = "exception while listing jobs";
        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(Either.right(workspaceClient));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        doThrow(new DatabricksException(errorMessage)).when(jobsAPI).list(any());

        Either<FailedOperation, LinkedHashMap<Object, Object>> result =
                workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assert (result.isLeft());
        assert result.getLeft().problems().get(0).getMessage().contains(errorMessage);
    }

    @Test
    public void testReverseProvisioning_ValidationFailedWorkflowNotFound() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        String errorMessage = "Workflow jobName not found";
        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(Either.right(workspaceClient));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        List<BaseJob> baseJobList = new ArrayList<>();
        when(jobsAPI.list(any())).thenReturn(baseJobList);

        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        Either<FailedOperation, LinkedHashMap<Object, Object>> result =
                workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assert (result.isLeft());
        assert result.getLeft().problems().get(0).getMessage().contains(errorMessage);
    }

    @Test
    public void testReverseProvisioning_ValidationFailedWorkflowNotUnique() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        String errorMessage = "Workflow jobName is not unique";
        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(Either.right(workspaceClient));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        List<BaseJob> baseJobList = new ArrayList<>();
        baseJobList.add(new BaseJob().setJobId(123L).setSettings(new JobSettings().setName("jobName")));
        baseJobList.add(new BaseJob().setJobId(123L).setSettings(new JobSettings().setName("jobName")));
        when(jobsAPI.list(any())).thenReturn(baseJobList);

        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        Either<FailedOperation, LinkedHashMap<Object, Object>> result =
                workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assert (result.isLeft());
        assert result.getLeft().problems().get(0).getMessage().contains(errorMessage);
    }

    @Test
    public void testReverseProvisioning_Tasks_Null() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(Either.right(workspaceClient));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        Job workflow = new Job();
        workflow.setJobId(123L);
        workflow.setSettings(new JobSettings().setName("jobName").setTasks(null));
        when(jobsAPI.get(anyLong())).thenReturn(workflow);

        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any()))
                .thenReturn(Collections.singletonList(
                        new BaseJob().setJobId(123L).setSettings(new JobSettings().setName("jobName"))));

        Either<FailedOperation, LinkedHashMap<Object, Object>> result =
                workflowReverseProvision.reverseProvision(reverseProvisioningRequest);

        assert (result.isRight());
        LinkedHashMap<Object, Object> updates = result.get();
        List<?> workflowTasksInfoList = (List<?>) updates.get("spec.mesh.specific.workflowTasksInfoList");
        assertEquals(0, workflowTasksInfoList.size());
    }

    @Test
    public void testReverseProvisioning_TaskFailure() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(Either.right(workspaceClient));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        Job workflow = new Job();
        workflow.setJobId(123L);
        workflow.setSettings(new JobSettings().setName("jobName"));

        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any()))
                .thenReturn(Collections.singletonList(
                        new BaseJob().setJobId(123L).setSettings(new JobSettings().setName("jobName"))));
        when(jobsAPI.get(anyLong())).thenReturn(workflow);

        Task task = new Task();
        task.setTaskKey("task1");
        task.setPipelineTask(new PipelineTask());
        workflow.setSettings(workflow.getSettings().setTasks(Collections.singletonList(task)));

        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        Either<FailedOperation, LinkedHashMap<Object, Object>> result =
                workflowReverseProvision.reverseProvision(reverseProvisioningRequest);

        assert (result.isLeft());
        assert result.getLeft()
                .problems()
                .get(0)
                .getMessage()
                .contains("An error occurred while retrieving workflow tasks info for task 'task1' in workspaceName.");
    }
}
