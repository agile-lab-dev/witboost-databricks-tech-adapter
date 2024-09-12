package it.agilelab.witboost.provisioning.databricks.service.updateacl;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.WorkspaceClient;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.SpecificProvisionerValidationException;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.*;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import it.agilelab.witboost.provisioning.databricks.service.provision.OutputPortHandler;
import it.agilelab.witboost.provisioning.databricks.service.validation.ValidationServiceImpl;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class UpdateAclServiceImplTest {

    @Mock
    private WorkspaceClient workspaceClient;

    @Mock
    OutputPortHandler outputPortHandler;

    @Mock
    WorkspaceHandler workspaceHandler;

    @Mock
    ValidationServiceImpl validationService;

    @InjectMocks
    private UpdateAclServiceImpl updateAclService;

    private DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspace", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);

    @Test
    public void testUpdateAcl_Success() {
        // Test that does not enter in outputPortHandler.updateAcl business logics

        ProvisionRequest provisionRequest = createProvisionRequest();
        UpdateAclRequest updateAclRequest = createUpdateAclRequest(createProvisionRequest());

        when(validationService.validate(any(ProvisioningRequest.class))).thenReturn(right(provisionRequest));

        when(workspaceHandler.getWorkspaceInfo("ws_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        when(outputPortHandler.updateAcl(
                        eq(provisionRequest),
                        eq(updateAclRequest),
                        eq(workspaceClient),
                        any(UnityCatalogManager.class)))
                .thenReturn(right(new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "")));

        var actualRes = updateAclService.updateAcl(updateAclRequest);

        var expectedRes = new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "");

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
    }

    @Test
    public void testUpdateAcl_WorkspaceInfoFailure() {

        UpdateAclRequest updateAclRequest = createUpdateAclRequest(createProvisionRequest());

        when(validationService.validate(any(ProvisioningRequest.class))).thenReturn(right(createProvisionRequest()));

        when(workspaceHandler.getWorkspaceInfo("ws_op")).thenReturn(left(new FailedOperation(List.of())));

        var actualRes = updateAclService.updateAcl(updateAclRequest);

        var expectedRes = new ProvisioningStatus(
                ProvisioningStatus.StatusEnum.FAILED, "Update Acl failed while getting workspace info");

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
    }

    @Test
    public void testUpdateAcl_WorkspaceClientFailure() {

        UpdateAclRequest updateAclRequest = createUpdateAclRequest(createProvisionRequest());

        when(validationService.validate(any(ProvisioningRequest.class))).thenReturn(right(createProvisionRequest()));

        when(workspaceHandler.getWorkspaceInfo("ws_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(left(new FailedOperation(List.of())));

        var actualRes = updateAclService.updateAcl(updateAclRequest);

        var expectedRes = new ProvisioningStatus(
                ProvisioningStatus.StatusEnum.FAILED, "Update Acl failed while getting workspace client");

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
    }

    @Test
    public void testUpdateAcl_NoOutputPortKindFailure() {

        ProvisionRequest provisionRequest = createProvisionRequestNoOutputPort();

        UpdateAclRequest updateAclRequest = createUpdateAclRequest(provisionRequest);

        when(validationService.validate(any(ProvisioningRequest.class))).thenReturn(right(provisionRequest));

        when(workspaceHandler.getWorkspaceInfo("ws_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        Assertions.assertThrows(
                SpecificProvisionerValidationException.class,
                () -> updateAclService.updateAcl(updateAclRequest),
                "The kind 'workload_outputportneeded' of the component is not supported by this Specific Provisioner");
    }

    @Test
    public void testUpdateAcl_updateAclFailure() {

        ProvisionRequest provisionRequest = createProvisionRequest();
        UpdateAclRequest updateAclRequest = createUpdateAclRequest(createProvisionRequest());

        when(validationService.validate(any(ProvisioningRequest.class))).thenReturn(right(provisionRequest));

        when(workspaceHandler.getWorkspaceInfo("ws_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        when(outputPortHandler.updateAcl(
                        eq(provisionRequest),
                        eq(updateAclRequest),
                        eq(workspaceClient),
                        any(UnityCatalogManager.class)))
                .thenReturn(left(new FailedOperation(List.of())));

        var actualRes = updateAclService.updateAcl(updateAclRequest);

        var expectedRes = new ProvisioningStatus(
                ProvisioningStatus.StatusEnum.FAILED, "Update Acl failed while assigning Databricks permissions.");

        assertEquals(expectedRes.getStatus(), actualRes.getStatus());
        assertEquals(expectedRes.getResult(), actualRes.getResult());
    }

    private ProvisionRequest createProvisionRequest() {

        DataProduct dataProduct = new DataProduct();
        OutputPort outputPort = new OutputPort();
        DatabricksOutputPortSpecific databricksOutputPortSpecific = new DatabricksOutputPortSpecific();

        databricksOutputPortSpecific.setWorkspace("ws");
        databricksOutputPortSpecific.setCatalogName("catalog");
        databricksOutputPortSpecific.setSchemaName("schema");
        databricksOutputPortSpecific.setTableName("t");
        databricksOutputPortSpecific.setSqlWarehouseName("sql_wh");
        databricksOutputPortSpecific.setWorkspaceOP("ws_op");
        databricksOutputPortSpecific.setCatalogNameOP("catalog_op");
        databricksOutputPortSpecific.setSchemaNameOP("schema_op");
        databricksOutputPortSpecific.setViewNameOP("view");

        outputPort.setKind("outputport");
        outputPort.setSpecific(databricksOutputPortSpecific);
        return new ProvisionRequest<>(dataProduct, outputPort, false);
    }

    private ProvisionRequest createProvisionRequestNoOutputPort() {

        DataProduct dataProduct = new DataProduct();
        OutputPort outputPort = new OutputPort();
        DatabricksOutputPortSpecific databricksOutputPortSpecific = new DatabricksOutputPortSpecific();

        databricksOutputPortSpecific.setWorkspace("ws");
        databricksOutputPortSpecific.setCatalogName("catalog");
        databricksOutputPortSpecific.setSchemaName("schema");
        databricksOutputPortSpecific.setTableName("t");
        databricksOutputPortSpecific.setSqlWarehouseName("sql_wh");
        databricksOutputPortSpecific.setWorkspaceOP("ws_op");
        databricksOutputPortSpecific.setCatalogNameOP("catalog_op");
        databricksOutputPortSpecific.setSchemaNameOP("schema_op");
        databricksOutputPortSpecific.setViewNameOP("view");

        outputPort.setKind("workload_outputportneeded");
        outputPort.setSpecific(databricksOutputPortSpecific);
        return new ProvisionRequest<>(dataProduct, outputPort, false);
    }

    private UpdateAclRequest createUpdateAclRequest(ProvisionRequest provisionRequest) {
        return new UpdateAclRequest(
                List.of("user:a_email.com", "group:group_test"),
                new ProvisionInfo(provisionRequest.toString(), "result"));
    }
}
