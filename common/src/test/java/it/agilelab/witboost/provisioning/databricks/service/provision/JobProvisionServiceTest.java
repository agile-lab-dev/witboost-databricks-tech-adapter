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
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.DatabricksJobWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.openapi.model.*;
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
public class JobProvisionServiceTest {
    @Mock
    private ValidationService validationService;

    @Mock
    private JobWorkloadHandler jobWorkloadHandler;

    @Mock
    private WorkspaceClient workspaceClient;

    @Mock
    private WorkspaceHandler workspaceHandler;

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
                .thenReturn(right(new ProvisionRequest<Specific>(null, null, false)));
        var expectedRes = new ValidationResult(true);
        var actualRes = provisionService.validate(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testValidateError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));
        var expectedRes = new ValidationResult(false).error(new ValidationError(List.of("error")));

        var actualRes = provisionService.validate(provisioningRequest);

        assertEquals(expectedRes, actualRes);
    }

    @Test
    public void testProvisionValidationError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("validationServiceError")));
        when(validationService.validate(provisioningRequest)).thenReturn(left(failedOperation));

        var token = provisionService.provision(provisioningRequest);
        assertEquals(
                "Errors: -validationServiceError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testProvisionUnsupportedKind() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
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
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<DatabricksJobWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksJobWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        when(jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(right("workloadId"));

        var info = new HashMap<String, String>();
        info.put("workspace path", "test");
        info.put("job path", "https://https://example.com/jobs/workloadId");

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")
                .info(new Info(JsonNodeFactory.instance.objectNode(), info).privateInfo(info));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
        assertEquals(expectedRes.getInfo().getPrivateInfo(), actualRes.getInfo().getPrivateInfo());
    }

    @Test
    public void testProvisionWorkloadWrongWorkspaceStatus() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<DatabricksJobWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksJobWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        DatabricksWorkspaceInfo wrongWorkspaceInfo = workspaceInfo;
        wrongWorkspaceInfo.setProvisioningState(ProvisioningState.DELETING);

        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(wrongWorkspaceInfo));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(ProvisioningStatus.StatusEnum.FAILED, actualRes.getStatus());
        assert actualRes.getResult().contains("The status of null workspace is different from 'ACTIVE'.");
    }

    @Test
    public void testProvisionWorkloadErrorCreatingJob() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<DatabricksJobWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksJobWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("jobCreationError")));
        when(jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(left(failedOperation));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(
                "Errors: -jobCreationError\n", provisionService.getStatus(token).getResult());
    }

    @Test
    public void testProvisionWorkloadErrorGettingWorkspace() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksJobWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("getWorkspaceError")));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(left(failedOperation));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(
                "Errors: -getWorkspaceError\n",
                provisionService.getStatus(token).getResult());
    }

    @Test
    public void testProvisionWorkloadErrorCreatingWorkspace() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksJobWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("createWorkspaceError")));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(left(failedOperation));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(
                "Errors: -createWorkspaceError\n",
                provisionService.getStatus(token).getResult());
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
    public void testUnprovisionUnsupportedKind() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
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
        Workload<DatabricksJobWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksJobWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        when(workspaceHandler.getWorkspaceInfo(any())).thenReturn(right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));
        when(jobWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo))
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
        Workload<DatabricksJobWorkloadSpecific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksJobWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        DatabricksWorkspaceInfo wrongWorkspaceInfo = workspaceInfo;
        wrongWorkspaceInfo.setProvisioningState(ProvisioningState.DELETING);
        when(workspaceHandler.getWorkspaceInfo(any())).thenReturn(right(Optional.of(wrongWorkspaceInfo)));

        String token = provisionService.unprovision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(ProvisioningStatus.StatusEnum.FAILED, actualRes.getStatus());
        assert actualRes.getResult().contains("The status of null workspace is different from 'ACTIVE'.");
    }

    @Test
    public void testUnprovisionWorkloadWorkspaceNameError() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksJobWorkloadSpecific());

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
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        Workload<Specific> workload = new Workload<>();
        workload.setKind("workload");
        workload.setSpecific(new DatabricksJobWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("gettingWorkspaceInfoError")));
        when(workspaceHandler.getWorkspaceInfo(any())).thenReturn(left(failedOperation));

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
        var specific = new DatabricksJobWorkloadSpecific();
        specific.setWorkspace("test");
        workload.setSpecific(specific);

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        when(workspaceHandler.getWorkspaceInfo(any())).thenReturn(right(Optional.empty()));

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
        workload.setSpecific(new DatabricksJobWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        when(workspaceHandler.getWorkspaceInfo(any())).thenReturn(right(Optional.of(workspaceInfo)));
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
        workload.setSpecific(new DatabricksJobWorkloadSpecific());

        var provisionRequest = new ProvisionRequest<>(null, workload, false);
        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceName(any())).thenReturn(right("test"));
        when(workspaceHandler.getWorkspaceInfo(any())).thenReturn(right(Optional.of(workspaceInfo)));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        var failedOperation =
                new FailedOperation(Collections.singletonList(new Problem("unprovisioningWorkloadError")));

        when(jobWorkloadHandler.unprovisionWorkload(any(), any(), any())).thenReturn(left(failedOperation));

        String token = provisionService.unprovision(provisioningRequest);

        ProvisioningStatus status = provisionService.getStatus(token);

        assertEquals(
                "Errors: -unprovisioningWorkloadError\n",
                provisionService.getStatus(token).getResult());
    }
}
