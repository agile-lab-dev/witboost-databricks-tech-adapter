package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.jobs.BaseJob;
import com.databricks.sdk.service.jobs.Job;
import com.databricks.sdk.service.jobs.JobSettings;
import com.databricks.sdk.service.jobs.JobsAPI;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest.*;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WorkflowReverseProvisionTest {

    @Mock
    private WorkspaceClient workspaceClient;

    @Mock
    private WorkspaceHandler workspaceHandler;

    @InjectMocks
    private WorkflowReverseProvision workflowReverseProvision;

    private final DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspaceName", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);

    private final String useCaseTemplateId = "urn:dmb:utm:databricks-workload-workflow-template:0.0.0";
    private final String workspaceName = "workspaceName";

    private ReverseProvisioningRequest createRequest() {
        ReverseProvisioningRequest request = new ReverseProvisioningRequest();
        request.setEnvironment("qa");
        request.setUseCaseTemplateId(useCaseTemplateId);

        CatalogInfo catalogInfo = new CatalogInfo<>();
        request.setCatalogInfo(catalogInfo);

        WorkflowReverseProvisioningParams params = new WorkflowReverseProvisioningParams();
        EnvironmentSpecificConfig environmentSpecificConfig = new EnvironmentSpecificConfig();
        WorkflowReverseProvisioningSpecific specific = new WorkflowReverseProvisioningSpecific();
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
        job.setSettings(new JobSettings().setName("jobName"));
        when(jobsAPI.get(anyLong())).thenReturn(job);

        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        ReverseProvisioningStatus result = workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assertEquals(ReverseProvisioningStatus.StatusEnum.COMPLETED, result.getStatus());
    }

    @Test
    public void testReverseProvisioning_WorkspaceInfoError() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        when(workspaceHandler.getWorkspaceInfo(workspaceName))
                .thenReturn(Either.left(new FailedOperation(Collections.singletonList(new Problem("error")))));

        ReverseProvisioningStatus result = workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, result.getStatus());
        assert result.getLogs()
                .get(0)
                .getMessage()
                .contains("Error while retrieving workspace info of workspaceName. Details: error");
    }

    @Test
    public void testReverseProvisioning_WorkspaceClientError() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo))
                .thenReturn(Either.left(new FailedOperation(Collections.singletonList(new Problem("error")))));

        ReverseProvisioningStatus result = workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, result.getStatus());
        assert result.getLogs()
                .get(0)
                .getMessage()
                .contains("Error while retrieving workspace client of workspaceName. Details: error");
    }

    @Test
    public void testReverseProvisioning_WorkspaceEmptyInfo() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.empty()));

        ReverseProvisioningStatus result = workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, result.getStatus());
        assert result.getLogs().get(0).getMessage().contains("Validation failed. Workspace 'workspaceName' not found.");
    }

    @Test
    public void testReverseProvisioning_ValidationFailedException() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(Either.right(workspaceClient));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        String expectedError = "exception while listing jobs";
        doThrow(new DatabricksException(expectedError)).when(jobsAPI).list(any());

        ReverseProvisioningStatus result = workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, result.getStatus());
        assert result.getLogs()
                .get(0)
                .getMessage()
                .contains(
                        "An error occurred while listing the jobs named jobName in workspaceName. "
                                + "Please try again and if the error persists contact the platform team. Details: exception while listing jobs");
    }

    @Test
    public void testReverseProvisioning_ValidationFailedWorkflowNotFound() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(Either.right(workspaceClient));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        List<BaseJob> baseJobList = new ArrayList<>();
        when(jobsAPI.list(any())).thenReturn(baseJobList);

        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        ReverseProvisioningStatus result = workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, result.getStatus());
        assert result.getLogs().get(0).getMessage().contains("Workflow jobName not found in workspaceName");
    }

    @Test
    public void testReverseProvisioning_ValidationFailedWorkflowNotUnique() {
        ReverseProvisioningRequest reverseProvisioningRequest = createRequest();

        when(workspaceHandler.getWorkspaceInfo(workspaceName)).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(Either.right(workspaceClient));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        List<BaseJob> baseJobList = new ArrayList<>();
        baseJobList.add(new BaseJob().setJobId(123L).setSettings(new JobSettings().setName("jobName")));
        baseJobList.add(new BaseJob().setJobId(123L).setSettings(new JobSettings().setName("jobName")));
        when(jobsAPI.list(any())).thenReturn(baseJobList);

        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        ReverseProvisioningStatus result = workflowReverseProvision.reverseProvision(reverseProvisioningRequest);
        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, result.getStatus());
        assert result.getLogs().get(0).getMessage().contains("Workflow jobName is not unique in workspaceName.");
    }
}
