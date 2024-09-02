package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.TableInfo;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.Info;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningStatus;
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
public class OutputPortProvisionServiceTest {
    @Mock
    private ValidationService validationService;

    @Mock
    private OutputPortHandler outputPortHandler;

    @Mock
    private WorkspaceClient workspaceClient;

    @Mock
    private WorkspaceHandler workspaceHandler;

    @Mock
    ForkJoinPool forkJoinPool;

    @InjectMocks
    private ProvisionServiceImpl provisionService;

    private OutputPort outputPort;

    private DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspace", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);

    private DatabricksOutputPortSpecific databricksOutputPortSpecific;

    @BeforeEach
    public void setUp() {
        Mockito.lenient().when(forkJoinPool.submit(any(Runnable.class))).thenAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();
            forkJoinPool.awaitQuiescence(10, TimeUnit.SECONDS);
            return null;
        });

        outputPort = new OutputPort<>();
        databricksOutputPortSpecific = new DatabricksOutputPortSpecific();
        databricksOutputPortSpecific.setWorkspace("ws");
        databricksOutputPortSpecific.setCatalogName("catalog");
        databricksOutputPortSpecific.setSchemaName("schema");
        databricksOutputPortSpecific.setTableName("t");
        databricksOutputPortSpecific.setSqlWarehouseName("sql_wh");
        databricksOutputPortSpecific.setWorkspaceOP("ws_op");
        databricksOutputPortSpecific.setCatalogNameOP("catalog_op");
        databricksOutputPortSpecific.setSchemaNameOP("schema_op");
        databricksOutputPortSpecific.setViewNameOP("view");
    }

    @Test
    public void testProvisionOutputPort_Success() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();

        OutputPort<DatabricksOutputPortSpecific> outputPort1 = new OutputPort<>();
        outputPort1.setKind("outputport");
        outputPort1.setSpecific(databricksOutputPortSpecific);

        var provisionRequest = new ProvisionRequest<>(null, outputPort1, false);

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));

        TableInfo tableInfoMock = mock(TableInfo.class);
        when(tableInfoMock.getTableId()).thenReturn("tableId");
        when(tableInfoMock.getFullName()).thenReturn("tableFullName");

        when(outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(right(tableInfoMock));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        var info = Map.of(
                "tableID",
                        Map.of(
                                "type", "string",
                                "label", "Table ID",
                                "value", "tableId"),
                "tableFullName",
                        Map.of(
                                "type", "string",
                                "label", "Table full name",
                                "value", "tableFullName"),
                "tableUrl",
                        Map.of(
                                "type", "string",
                                "label", "Table URL",
                                "value", "Open table details in Databricks",
                                "href", "https://https://example.com/explore/data/null/null/null"));

        var expectedRes =
                new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "").info(new Info(info, info));

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
        assertEquals(expectedRes.getInfo().getPrivateInfo(), actualRes.getInfo().getPrivateInfo());
        assertEquals(expectedRes.getInfo().getPublicInfo(), actualRes.getInfo().getPublicInfo());
    }

    @Test
    public void testProvisionOutputPort_FailureCreatedWorkspace() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();

        OutputPort<DatabricksOutputPortSpecific> outputPort1 = new OutputPort<>();
        outputPort1.setKind("outputport");
        outputPort1.setSpecific(databricksOutputPortSpecific);

        var provisionRequest = new ProvisionRequest<DatabricksOutputPortSpecific>(null, outputPort1, false);

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("createWorkspaceError")));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(left(failedOperation));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.FAILED, "createWorkspaceError");

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals("Errors: -createWorkspaceError\n", actualRes.getResult());
    }

    @Test
    public void testProvisionOutputPort_FailureWorkspaceClient() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();

        OutputPort<DatabricksOutputPortSpecific> outputPort1 = new OutputPort<>();
        outputPort1.setKind("outputport");
        outputPort1.setSpecific(databricksOutputPortSpecific);

        var provisionRequest = new ProvisionRequest<DatabricksOutputPortSpecific>(null, outputPort1, false);

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.provisionWorkspace(any())).thenReturn(right(workspaceInfo));

        var failedOperation =
                new FailedOperation(Collections.singletonList(new Problem("gettingWorkspaceClientError")));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(left(failedOperation));

        String token = provisionService.provision(provisioningRequest);

        ProvisioningStatus actualRes = provisionService.getStatus(token);

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.FAILED, "gettingWorkspaceClientError");

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals("Errors: -gettingWorkspaceClientError\n", actualRes.getResult());
    }

    @Test
    public void testUnprovisionOutputPort_Success() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        OutputPort<DatabricksOutputPortSpecific> outputPort1 = new OutputPort<>();
        outputPort1.setKind("outputport");
        outputPort1.setSpecific(databricksOutputPortSpecific);

        var provisionRequest = new ProvisionRequest<>(null, outputPort1, false);

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class)))
                .thenReturn(right(Optional.of(workspaceInfo)));

        when(outputPortHandler.unprovisionOutputPort(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(right(null));

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "");

        String token = provisionService.unprovision(provisioningRequest);
        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
    }

    @Test
    public void testUnprovisionOutputPort_UnexistingWorkspaceSuccess() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        OutputPort<DatabricksOutputPortSpecific> outputPort1 = new OutputPort<>();
        outputPort1.setKind("outputport");
        outputPort1.setSpecific(databricksOutputPortSpecific);

        var provisionRequest = new ProvisionRequest<>(null, outputPort1, false);

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class))).thenReturn(right(Optional.empty()));

        var expectedRes = new ProvisioningStatus(
                ProvisioningStatus.StatusEnum.COMPLETED, "Unprovision skipped. Workspace ws not found.");

        String token = provisionService.unprovision(provisioningRequest);
        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
    }

    @Test
    public void testUnprovisionOutputPort_FailureGetWorkspaceInfo() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        OutputPort<DatabricksOutputPortSpecific> outputPort1 = new OutputPort<>();
        outputPort1.setKind("outputport");
        outputPort1.setSpecific(databricksOutputPortSpecific);

        var provisionRequest = new ProvisionRequest<>(null, outputPort1, false);

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("")));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class))).thenReturn(left(failedOperation));

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.FAILED, "");

        String token = provisionService.unprovision(provisioningRequest);
        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
    }

    @Test
    public void testUnprovisionOutputPort_FailureGetWorkspaceClient() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        OutputPort<DatabricksOutputPortSpecific> outputPort1 = new OutputPort<>();
        outputPort1.setKind("outputport");
        outputPort1.setSpecific(databricksOutputPortSpecific);

        var provisionRequest = new ProvisionRequest<>(null, outputPort1, false);

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));

        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class)))
                .thenReturn(right(Optional.of(workspaceInfo)));

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("")));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(left(failedOperation));

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.FAILED, "");

        String token = provisionService.unprovision(provisioningRequest);
        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
    }

    @Test
    public void testUnprovisionOutputPort_FailureUnprovisionOutputPort() {
        ProvisioningRequest provisioningRequest = new ProvisioningRequest();
        OutputPort<DatabricksOutputPortSpecific> outputPort1 = new OutputPort<>();
        outputPort1.setKind("outputport");
        outputPort1.setSpecific(databricksOutputPortSpecific);

        var provisionRequest = new ProvisionRequest<>(null, outputPort1, false);

        when(validationService.validate(provisioningRequest)).thenReturn(right(provisionRequest));
        when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));
        when(workspaceHandler.getWorkspaceInfo(any(ProvisionRequest.class)))
                .thenReturn(right(Optional.of(workspaceInfo)));

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("")));
        when(outputPortHandler.unprovisionOutputPort(provisionRequest, workspaceClient, workspaceInfo))
                .thenReturn(left(failedOperation));

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.FAILED, "");

        String token = provisionService.unprovision(provisioningRequest);
        ProvisioningStatus actualRes = provisionService.getStatus(token);

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
    }
}
