package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.WorkspaceClient;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.openapi.model.*;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import it.agilelab.witboost.provisioning.databricks.service.validation.ValidationService;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
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
public class DLTProvisionServiceTest {
    @Mock
    private ValidationService validationService;

    @Mock
    private DLTWorkloadHandler dltWorkloadHandler;

    @Mock
    private WorkspaceClient workspaceClient;

    @Mock
    private WorkspaceHandler workspaceHandler;

    @Mock
    private ForkJoinPool forkJoinPool;

    @InjectMocks
    private ProvisionServiceImpl provisionService;

    private DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
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
    public void testProvisionWorkloadWrongSpecific() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new Specific());
        workload.setName("test_workload");
        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);
        var expectedRes = new ProvisioningStatus(
                ProvisioningStatus.StatusEnum.FAILED,
                "The specific section of the component test_workload is not of type DatabricksJobWorkloadSpecific or DatabricksDLTWorkloadSpecific");

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
    }

    @Test
    public void testUnprovisionWorkloadWorkspaceInfoError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        DatabricksDLTWorkloadSpecific specific = new DatabricksDLTWorkloadSpecific();
        specific.setWorkspace("test");
        workload.setSpecific(specific);

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("gettingWorkspaceInfoError")));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(left(failedOperation));

        String token = provisionService.provision(provisioningRequest);
        assertEquals(
                "Errors: -gettingWorkspaceInfoError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testProvisionWorkloadOk() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<DatabricksDLTWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksDLTWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        when(dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(right("workloadId"));

        var info = Map.of(
                "workspaceURL",
                Map.of(
                        "type", "string",
                        "label", "Databricks workspace URL",
                        "value", "Open Azure Databricks Workspace",
                        "href", "test"),
                "pipelineURL",
                Map.of(
                        "type", "string",
                        "label", "Pipeline URL",
                        "value", "Open pipeline details in Databricks",
                        "href", "https://https://example.com/pipelines/workloadId"));

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
    public void testProvisionWorkspaceErrorCreatingPipeline() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<DatabricksDLTWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksDLTWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("pipelineCreationError")));
        when(dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(left(failedOperation));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(
                "Errors: -pipelineCreationError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testProvisionWorkloadWrongWorkspaceStatus() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<DatabricksDLTWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksDLTWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<DatabricksDLTWorkloadSpecific>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        DatabricksWorkspaceInfo databricksWorkspaceInfoWrong = workspaceInfo;
        databricksWorkspaceInfoWrong.setProvisioningState(ProvisioningState.DELETING);

        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(databricksWorkspaceInfoWrong));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(ProvisioningStatus.StatusEnum.FAILED, actualRes.getStatus());
        assert (actualRes.getResult().contains("The status of null workspace is different from 'ACTIVE'."));
    }

    @Test
    public void testProvisionWorkspaceErrorGettingWorkspace() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksDLTWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("getWorkspaceError")));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(left(failedOperation));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals("Errors: -getWorkspaceError\n", actualRes.getResult());
    }

    @Test
    public void testUnprovisionValidationError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("validationServiceError")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));

        String token = provisionService.unprovision(provisioningRequest);
        assertEquals(
                "Errors: -validationServiceError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testUnprovisionWorkloadOk() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<DatabricksDLTWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksDLTWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class)))
                .thenReturn(right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));
        when(dltWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(right(null));
        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "");

        String token = provisionService.unprovision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
    }

    @Test
    public void testProvisionWorkloadWorkspaceInfoError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksDLTWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("gettingWorkspaceInfoError")));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class))).thenReturn(left(failedOperation));

        String token = provisionService.unprovision(provisioningRequest);

        assertEquals(
                "Errors: -gettingWorkspaceInfoError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testUnprovisionWorkloadWorkspaceInfoEmpty() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        var specific = new DatabricksDLTWorkloadSpecific();
        specific.setWorkspace("test");
        workload.setSpecific(specific);

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class))).thenReturn(right(Optional.empty()));

        String token = provisionService.unprovision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);
        var expectedRes = new ProvisioningStatus(
                ProvisioningStatus.StatusEnum.COMPLETED, "Unprovision skipped. Workspace test not found.");

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
    }

    @Test
    public void testUnprovisionWorkloadWorkspaceClientError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksDLTWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
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
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksDLTWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class)))
                .thenReturn(right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        var failedOperation =
                new FailedOperation(Collections.singletonList(new Problem("unprovisioningWorkloadError")));

        when(dltWorkloadHandler.unprovisionWorkload(any(), any(), any())).thenReturn(left(failedOperation));

        String token = provisionService.unprovision(provisioningRequest);

        ProvisioningStatus status = provisionService.getStatus(token);

        assertEquals(
                "Errors: -unprovisioningWorkloadError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testUnprovisionWorkloadWrongWorkspaceStatus() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<DatabricksDLTWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksDLTWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        DatabricksWorkspaceInfo wrongWorkspaceInfo = workspaceInfo;
        wrongWorkspaceInfo.setProvisioningState(ProvisioningState.DELETING);

        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class)))
                .thenReturn(right(Optional.of(wrongWorkspaceInfo)));

        String token = provisionService.unprovision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(ProvisioningStatus.StatusEnum.FAILED, actualRes.getStatus());
        assert actualRes.getResult().contains("The status of null workspace is different from 'ACTIVE'.");
    }
}
