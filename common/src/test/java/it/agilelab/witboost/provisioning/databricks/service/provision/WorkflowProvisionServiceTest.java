package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.jobs.*;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.openapi.model.*;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import it.agilelab.witboost.provisioning.databricks.service.provision.handler.WorkflowWorkloadHandler;
import it.agilelab.witboost.provisioning.databricks.service.validation.ValidationService;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WorkflowProvisionServiceTest {
    @Mock
    private ValidationService validationService;

    @Mock
    private WorkflowWorkloadHandler workflowWorkloadHandler;

    @Mock
    private WorkspaceClient workspaceClient;

    @Mock
    private WorkspaceHandler workspaceHandler;

    @Mock
    private MiscConfig miscConfig;

    @Mock
    ForkJoinPool forkJoinPool;

    @InjectMocks
    private ProvisionServiceImpl provisionService;

    private final DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspace", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);

    @BeforeEach
    public void setUp() {
        Mockito.lenient().when(forkJoinPool.submit(any(Runnable.class))).thenAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();
            forkJoinPool.awaitQuiescence(10, TimeUnit.SECONDS);
            return null;
        });
    }

    @Test
    public void testValidateOk() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        when(validationService.validate(provisioningRequest))
                .thenReturn(right(new ProvisionRequest<>(null, null, false)));
        var expectedRes = new ValidationResult(true);
        var actualRes = provisionService.validate(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testValidateError() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));
        var expectedRes = new ValidationResult(false).error(new ValidationError(List.of("error")));

        var actualRes = provisionService.validate(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testProvisionValidationError() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("validationServiceError")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));

        var token = provisionService.provision(provisioningRequest);
        assertEquals(
                "Errors: validationServiceError;\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testProvisionUnsupportedKind() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<Specific> workload = new Workload<>();
        workload.setKind("unsupported");
        when(validationService.validate(provisioningRequest))
                .thenReturn(right(new ProvisionRequest<>(null, workload, false)));

        String expectedDesc = "The kind 'unsupported' of the component is not supported by this Specific Provisioner";

        var token = provisionService.provision(provisioningRequest);
        assertEquals(expectedDesc, provisionService.getStatus(token).getResult());
    }

    @Test
    public void testProvisionWorkloadOk() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setSettings(new JobSettings().setName("databricksWorkflow"));
        specific.setWorkflow(wf);
        workload.setSpecific(specific);
        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("qa");

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceClient.jobs()).thenReturn(mock(JobsAPI.class));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        when(workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(right("workloadId"));

        var info = Map.of(
                "workspaceURL",
                Map.of(
                        "type",
                        "string",
                        "label",
                        "Databricks workspace URL",
                        "value",
                        "Open Azure Databricks Workspace",
                        "href",
                        "test"),
                "jobURL",
                Map.of(
                        "type",
                        "string",
                        "label",
                        "Job URL",
                        "value",
                        "Open job details in Databricks",
                        "href",
                        "https://https://example.com/jobs/workloadId"));

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                .info(new Info(JsonNodeFactory.instance.objectNode(), info)
                        .privateInfo(info)
                        .publicInfo(info));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
        assertEquals(expectedRes.getInfo().getPrivateInfo(), actualRes.getInfo().getPrivateInfo());
        assertEquals(expectedRes.getInfo().getPublicInfo(), actualRes.getInfo().getPublicInfo());
    }

    @Test
    public void testProvisionWorkloadWrongWorkspaceStatus() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setSettings(new JobSettings().setName("databricksWorkflow"));
        specific.setWorkflow(wf);
        workload.setSpecific(specific);
        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("qa");

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(Either.right(workspaceClient));
        when(workspaceClient.jobs()).thenReturn(mock(JobsAPI.class));
        DatabricksWorkspaceInfo wrongWorkspaceInfo = workspaceInfo;
        wrongWorkspaceInfo.setProvisioningState(ProvisioningState.DELETING);

        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(wrongWorkspaceInfo));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(ProvisioningStatus.StatusEnum.FAILED, actualRes.getStatus());
        assert actualRes.getResult().contains("The status of workspace workspace is different from 'ACTIVE'.");
    }

    @Test
    public void testProvisionWorkloadErrorCreatingJob() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setSettings(new JobSettings().setName("databricksWorkflow"));
        specific.setWorkflow(wf);
        workload.setSpecific(specific);
        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("qa");

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));
        when(workspaceClient.jobs()).thenReturn(mock(JobsAPI.class));
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("jobCreationError")));
        when(workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(left(failedOperation));

        String token = provisionService.provision(provisioningRequest);

        assertEquals(
                "Errors: -jobCreationError\n", provisionService.getStatus(token).getResult());
    }

    @Test
    public void testProvisionWorkloadErrorGettingWorkspace() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setSettings(new JobSettings().setName("databricksWorkflow"));
        specific.setWorkflow(wf);
        workload.setSpecific(specific);
        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("qa");

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(Either.right(Optional.of(workspaceInfo)));
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("getWorkspaceError")));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(left(failedOperation));

        String token = provisionService.provision(provisioningRequest);

        assertEquals(
                "Errors: -getWorkspaceError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testProvisionWorkloadErrorCreatingWorkspace() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setSettings(new JobSettings().setName("databricksWorkflow"));
        specific.setWorkflow(wf);
        workload.setSpecific(specific);
        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("qa");

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(Either.right(workspaceClient));
        when(workspaceClient.jobs()).thenReturn(mock(JobsAPI.class));
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("createWorkspaceError")));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(left(failedOperation));

        String token = provisionService.provision(provisioningRequest);

        assertEquals(
                "Errors: -createWorkspaceError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testUnprovisionValidationError() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("validationServiceError")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));

        String token = provisionService.unprovision(provisioningRequest);
        assertEquals(
                "Errors: validationServiceError;\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testUnprovisionUnsupportedKind() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<Specific> Workload = new Workload<>();
        Workload.setKind("unsupported");
        when(validationService.validate(provisioningRequest))
                .thenReturn(right(new ProvisionRequest<>(null, Workload, false)));
        String expectedDesc = "The kind 'unsupported' of the component is not supported by this Specific Provisioner";

        var token = provisionService.unprovision(provisioningRequest);
        assertEquals(expectedDesc, provisionService.getStatus(token).getResult());
    }

    @Test
    public void testUnprovisionWorkloadOk() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksWorkflowWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class)))
                .thenReturn(right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));
        when(workflowWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(right(null));
        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "");

        String token = provisionService.unprovision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
    }

    @Test
    public void testUnprovisionWorkloadWrongWorkspaceStatus() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksWorkflowWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        DatabricksWorkspaceInfo wrongWorkspaceInfo = workspaceInfo;
        wrongWorkspaceInfo.setProvisioningState(ProvisioningState.DELETING);
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class)))
                .thenReturn(right(Optional.of(wrongWorkspaceInfo)));

        String token = provisionService.unprovision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(ProvisioningStatus.StatusEnum.FAILED, actualRes.getStatus());
        assert actualRes.getResult().contains("The status of null workspace is different from 'ACTIVE'.");
    }

    @Test
    public void testUnprovisionWorkloadWorkspaceNameError() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksWorkflowWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("gettingWorkspaceNameError")));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(left(failedOperation));

        String token = provisionService.unprovision(provisioningRequest);

        assertEquals(
                "Errors: -gettingWorkspaceNameError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testUnprovisionWorkloadWorkspaceInfoError() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksWorkflowWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("gettingWorkspaceInfoError")));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class))).thenReturn(left(failedOperation));

        String token = provisionService.unprovision(provisioningRequest);

        assertEquals(
                "Errors: -gettingWorkspaceInfoError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testUnprovisionWorkloadWorkspaceInfoEmpty() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        var specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setWorkspace("test");
        workload.setSpecific(specific);

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class))).thenReturn(right(Optional.empty()));

        String token = provisionService.unprovision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);
        var expectedRes = new ProvisioningStatus(
                ProvisioningStatus.StatusEnum.COMPLETED,
                "Unprovision skipped for component null. Workspace test not found.");

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
    }

    @Test
    public void testUnprovisionWorkloadWorkspaceClientError() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksWorkflowWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class)))
                .thenReturn(right(Optional.of(workspaceInfo)));
        var failedOperation =
                new FailedOperation(Collections.singletonList(new Problem("gettingWorkspaceClientError")));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(left(failedOperation));

        String token = provisionService.unprovision(provisioningRequest);

        assertEquals(
                "Errors: -gettingWorkspaceClientError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testUnprovisionWorkloadUnprovisioningWorkloadError() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksWorkflowWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class)))
                .thenReturn(right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        var failedOperation =
                new FailedOperation(Collections.singletonList(new Problem("unprovisioningWorkloadError")));

        when(workflowWorkloadHandler.unprovisionWorkload(any(), any(), any())).thenReturn(left(failedOperation));

        String token = provisionService.unprovision(provisioningRequest);

        assertEquals(
                "Errors: -unprovisioningWorkloadError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testProvisionWorkload_ValidationOneWorkflowEqual() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setOverride(false);
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setJobId(123l);
        wf.setSettings(new JobSettings().setName("databricksWorkflow"));
        specific.setWorkflow(wf);
        workload.setSpecific(specific);
        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("qa");

        JobsAPI jobsAPI = mock(JobsAPI.class);
        List<BaseJob> baseJobList = new ArrayList<>();
        baseJobList.add(new BaseJob().setJobId(123l));
        when(jobsAPI.list(any())).thenReturn(baseJobList);
        when(jobsAPI.get(anyLong())).thenReturn(wf);

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        when(workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(right("workloadId"));

        var info = Map.of(
                "workspaceURL",
                Map.of(
                        "type",
                        "string",
                        "label",
                        "Databricks workspace URL",
                        "value",
                        "Open Azure Databricks Workspace",
                        "href",
                        "test"),
                "jobURL",
                Map.of(
                        "type",
                        "string",
                        "label",
                        "Job URL",
                        "value",
                        "Open job details in Databricks",
                        "href",
                        "https://https://example.com/jobs/workloadId"));

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                .info(new Info(JsonNodeFactory.instance.objectNode(), info)
                        .privateInfo(info)
                        .publicInfo(info));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
        assertEquals(expectedRes.getInfo().getPrivateInfo(), actualRes.getInfo().getPrivateInfo());
        assertEquals(expectedRes.getInfo().getPublicInfo(), actualRes.getInfo().getPublicInfo());
    }

    @Test
    public void testProvisionWorkload_ValidationMoreThanOneWorkflow() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setOverride(false);
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setJobId(123l);
        wf.setSettings(new JobSettings().setName("databricksWorkflow"));
        Job wf2 = new Job();
        wf2.setJobId(456l);
        wf2.setSettings(new JobSettings().setName("wf2"));

        specific.setWorkflow(wf);
        workload.setSpecific(specific);

        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("qa");

        JobsAPI jobsAPI = mock(JobsAPI.class);
        List<BaseJob> baseJobList = new ArrayList<>();
        baseJobList.add(new BaseJob().setJobId(123l));
        baseJobList.add(new BaseJob().setJobId(456l));

        when(jobsAPI.list(any())).thenReturn(baseJobList);

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(Either.right(workspaceClient));
        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assert actualRes.getStatus().equals(ProvisioningStatus.StatusEnum.FAILED);
        assert actualRes
                .getResult()
                .contains(
                        "Error during validation for deployment of workflow. Found more than one workflow named databricksWorkflow in workspace workspace. Please leave this name only to the workflow linked to the Witboost component.");
    }

    @Test
    public void testProvisionWorkload_ValidationEmptyWf() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setOverride(false);
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setJobId(123l);
        wf.setSettings(new JobSettings()
                .setName("databricksWorkflow")
                .setEmailNotifications(new JobEmailNotifications())
                .setWebhookNotifications(new WebhookNotifications())
                .setFormat(Format.MULTI_TASK)
                .setTimeoutSeconds(0l)
                .setMaxConcurrentRuns(1l));
        Job wf2 = new Job();
        wf2.setJobId(456l);

        wf2.setSettings(new JobSettings()
                .setName("wf2")
                .setEmailNotifications(new JobEmailNotifications())
                .setWebhookNotifications(new WebhookNotifications())
                .setFormat(Format.MULTI_TASK)
                .setTimeoutSeconds(0l)
                .setMaxConcurrentRuns(1l));

        specific.setWorkflow(wf);
        workload.setSpecific(specific);

        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("qa");

        JobsAPI jobsAPI = mock(JobsAPI.class);
        List<BaseJob> baseJobList = new ArrayList<>();
        baseJobList.add(new BaseJob().setJobId(456l));
        when(jobsAPI.list(any())).thenReturn(baseJobList);
        when(jobsAPI.get(456l)).thenReturn(wf2);

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assert actualRes.getStatus().equals(ProvisioningStatus.StatusEnum.FAILED);
        assert actualRes
                .getResult()
                .contains(
                        "It is not permitted to replace a NON-empty workflow [name: databricksWorkflow, "
                                + "id: 456, workspace: workspace] with an empty one. Kindly perform reverse provisioning and try again.");
    }

    @Test
    public void testProvisionWorkload_ValidationOverrideTrue() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setOverride(true);
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setJobId(123l);
        wf.setSettings(new JobSettings().setName("databricksWorkflow"));
        Job wf2 = new Job();
        wf2.setJobId(456l);
        wf2.setSettings(new JobSettings().setName("wf2"));

        specific.setWorkflow(wf);
        workload.setSpecific(specific);

        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("qa");

        JobsAPI jobsAPI = mock(JobsAPI.class);
        List<BaseJob> baseJobList = new ArrayList<>();
        baseJobList.add(new BaseJob().setJobId(456l));
        when(jobsAPI.list(any())).thenReturn(baseJobList);
        when(jobsAPI.get(456l)).thenReturn(wf2);

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        when(workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(right("workloadId"));

        var info = Map.of(
                "workspaceURL",
                Map.of(
                        "type",
                        "string",
                        "label",
                        "Databricks workspace URL",
                        "value",
                        "Open Azure Databricks Workspace",
                        "href",
                        "test"),
                "jobURL",
                Map.of(
                        "type",
                        "string",
                        "label",
                        "Job URL",
                        "value",
                        "Open job details in Databricks",
                        "href",
                        "https://https://example.com/jobs/workloadId"));

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                .info(new Info(JsonNodeFactory.instance.objectNode(), info)
                        .privateInfo(info)
                        .publicInfo(info));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
        assertEquals(expectedRes.getInfo().getPrivateInfo(), actualRes.getInfo().getPrivateInfo());
        assertEquals(expectedRes.getInfo().getPublicInfo(), actualRes.getInfo().getPublicInfo());
    }

    @Test
    public void testProvisionWorkload_DevEnvironment() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setOverride(false);
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setJobId(123l);
        wf.setSettings(new JobSettings().setName("databricksWorkflow"));
        Job wf2 = new Job();
        wf2.setJobId(456l);
        wf2.setSettings(new JobSettings().setName("wf2"));

        specific.setWorkflow(wf);
        workload.setSpecific(specific);

        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("development");
        when(miscConfig.developmentEnvironmentName()).thenReturn("development");

        JobsAPI jobsAPI = mock(JobsAPI.class);
        List<BaseJob> baseJobList = new ArrayList<>();
        baseJobList.add(new BaseJob().setJobId(456l));
        when(jobsAPI.list(any())).thenReturn(baseJobList);
        when(jobsAPI.get(456l)).thenReturn(wf2);

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(Either.right(Optional.of(workspaceInfo)));
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        String token = provisionService.provision(provisioningRequest);
        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assert actualRes.getStatus().equals(ProvisioningStatus.StatusEnum.FAILED);
        assert actualRes
                .getResult()
                .contains(
                        "The request workflow [name: databricksWorkflow, id: 456, workspace: workspace] is different from that found on Databricks. Kindly perform reverse provisioning and try again.");
    }

    @Test
    public void testProvisionWorkload_EmptyWorkspaceInfo() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setOverride(true);
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setJobId(123l);
        wf.setSettings(new JobSettings().setName("databricksWorkflow"));
        Job wf2 = new Job();
        wf2.setJobId(456l);
        wf2.setSettings(new JobSettings().setName("wf2"));

        specific.setWorkflow(wf);
        workload.setSpecific(specific);

        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("qa");

        JobsAPI jobsAPI = mock(JobsAPI.class);
        List<BaseJob> baseJobList = new ArrayList<>();
        baseJobList.add(new BaseJob().setJobId(456l));

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(Either.right(Optional.empty()));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        when(workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(right("workloadId"));

        var info = Map.of(
                "workspaceURL",
                Map.of(
                        "type",
                        "string",
                        "label",
                        "Databricks workspace URL",
                        "value",
                        "Open Azure Databricks Workspace",
                        "href",
                        "test"),
                "jobURL",
                Map.of(
                        "type",
                        "string",
                        "label",
                        "Job URL",
                        "value",
                        "Open job details in Databricks",
                        "href",
                        "https://https://example.com/jobs/workloadId"));

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                .info(new Info(JsonNodeFactory.instance.objectNode(), info)
                        .privateInfo(info)
                        .publicInfo(info));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
        assertEquals(expectedRes.getInfo().getPrivateInfo(), actualRes.getInfo().getPrivateInfo());
        assertEquals(expectedRes.getInfo().getPublicInfo(), actualRes.getInfo().getPublicInfo());
    }

    @Test
    public void testProvisionWorkload_LeftWorkspaceInfo() {
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, "", false);
        Workload<DatabricksWorkflowWorkloadSpecific> workload = new Workload<>();
        workload.setName("workflow");
        workload.setKind("workload");
        DatabricksWorkflowWorkloadSpecific specific = new DatabricksWorkflowWorkloadSpecific();
        specific.setOverride(true);
        specific.setWorkspace("workspace");
        Job wf = new Job();
        wf.setJobId(123l);
        wf.setSettings(new JobSettings().setName("databricksWorkflow"));
        Job wf2 = new Job();
        wf2.setJobId(456l);
        wf2.setSettings(new JobSettings().setName("wf2"));

        specific.setWorkflow(wf);
        workload.setSpecific(specific);

        DataProduct dataProduct = new DataProduct();
        dataProduct.setEnvironment("qa");

        List<BaseJob> baseJobList = new ArrayList<>();
        baseJobList.add(new BaseJob().setJobId(456l));

        var provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(anyString()))
                .thenReturn(Either.left(
                        new FailedOperation(Collections.singletonList(new Problem("Error retrieving workspaceInfo")))));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assert actualRes.getStatus().equals(ProvisioningStatus.StatusEnum.FAILED);
        assert actualRes.getResult().contains("Error retrieving workspaceInfo");
    }
}
