package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.TableExistsResponse;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TablesAPI;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.bean.ApiClientConfig;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.config.WorkloadTemplatesConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.DescriptorKind;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import it.agilelab.witboost.provisioning.databricks.util.ResourceUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class ValidationServiceTest {

    @Autowired
    private WorkloadTemplatesConfig workloadTemplatesConfig;

    @MockBean
    private Function<ApiClientConfig.ApiClientConfigParams, ApiClient> apiClientFactory;

    @Mock
    ApiClient apiClientMock;

    @Mock
    WorkspaceHandler workspaceHandler;

    @Autowired
    MiscConfig miscConfig;

    @Autowired
    ValidationService service;

    @Test
    public void testValidateJobWorkloadOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptors/databricks/job_workload.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        var actualRes = service.validate(provisioningRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateDLTWorkloadOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptors/databricks/dlt_workload.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        var actualRes = service.validate(provisioningRequest);
        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateWorkflowWorkloadOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptors/databricks/workflow_workload.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        var actualRes = service.validate(provisioningRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateDLTWorkloadWrongDescriptor() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptors/databricks/dlt_wrong_workload.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        var actualRes = service.validate(provisioningRequest);

        String expectedError = "Failed to deserialize the component. Details: Unrecognized field \"wrong\"";
        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        assertTrue(actualRes.getLeft().problems().get(0).description().contains(expectedError));
    }

    @Test
    public void testValidateUnsupportedUseCaseTemplateId() throws IOException {
        String ymlDescriptor =
                ResourceUtils.getContentFromResource("/descriptors/pr_descriptor_unsupported_workload.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        String expectedError =
                "An error occurred while parsing the component \"Databricks Workload\". Please try again and if the error persists contact the platform team. Details: unsupported use case template id.";

        var actualRes = service.validate(provisioningRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        assertTrue(actualRes.getLeft().problems().get(0).description().contains(expectedError));
    }

    @Test
    public void testValidateWrongDescriptorKind() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptors/pr_descriptor_outputport.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.DATAPRODUCT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "The descriptorKind field is not valid. Expected: 'COMPONENT_DESCRIPTOR', Actual: 'DATAPRODUCT_DESCRIPTOR'";

        var actualResult = service.validate(provisioningRequest);

        Assertions.assertTrue(actualResult.isLeft());
        assertEquals(1, actualResult.getLeft().problems().size());
        actualResult.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateWrongDescriptorFormat() {
        String ymlDescriptor = "an_invalid_descriptor";
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc = "Failed to deserialize the Yaml Descriptor. Details: ";

        var actualRes = service.validate(provisioningRequest);

        Assertions.assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            Assertions.assertTrue(p.description().startsWith(expectedDesc));
            Assertions.assertTrue(p.cause().isPresent());
        });
    }

    @Test
    public void testValidateMissingComponentIdToProvision() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource(
                "/descriptors/pr_descriptor_storage_missing_componentIdToProvision.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc = "Component with ID null not found in the Descriptor";

        var actualRes = service.validate(provisioningRequest);

        Assertions.assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateMissingComponentToProvision() throws IOException {
        String ymlDescriptor =
                ResourceUtils.getContentFromResource("/descriptors/pr_descriptor_storage_missing_component.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "Component with ID urn:dmb:cmp:healthcare:vaccinations:0:storage not found in the Descriptor";

        var actualResult = service.validate(provisioningRequest);

        Assertions.assertTrue(actualResult.isLeft());
        assertEquals(1, actualResult.getLeft().problems().size());
        actualResult.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateMissingComponentKindToProvision() throws IOException {
        String ymlDescriptor =
                ResourceUtils.getContentFromResource("/descriptors/pr_descriptor_storage_missing_componentKind.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "Component Kind not found for the component with ID urn:dmb:cmp:healthcare:vaccinations:0:storage";

        var actualRes = service.validate(provisioningRequest);

        Assertions.assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateWrongComponentKindToProvision() throws IOException {
        String ymlDescriptor =
                ResourceUtils.getContentFromResource("/descriptors/pr_descriptor_storage_wrong_componentKind.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "The kind 'wrong' of the component to provision is not supported by this Specific Provisioner";

        var actualRes = service.validate(provisioningRequest);

        Assertions.assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateOutputPortOk() throws Exception {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptors/databricks/outputport_ok.yml");

        WorkspaceHandler workspaceHandlerMock = mock(WorkspaceHandler.class);

        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);

        when(workspaceHandlerMock.getWorkspaceInfo(any(String.class)))
                .thenReturn(right(Optional.of(mock(DatabricksWorkspaceInfo.class))));
        when(workspaceHandlerMock.getWorkspaceClient(any())).thenReturn(right(workspaceClientMock));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        ApiClient apiClientMock = mock(ApiClient.class);

        when(apiClientFactory.apply(any(ApiClientConfig.ApiClientConfigParams.class)))
                .thenReturn(apiClientMock);

        when(workspaceClientMock.tables()).thenReturn(mock(TablesAPI.class));

        when(workspaceClientMock.tables().exists("catalog_name.schema_name.table_name"))
                .thenReturn(tableExistsResponseMock);

        TableInfo tableInfoMock = mock(TableInfo.class);

        when(workspaceClientMock.tables().get("catalog_name.schema_name.table_name"))
                .thenReturn(tableInfoMock);

        // Mock columns of original table.
        Collection<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(new ColumnInfo().setName("col_1"));
        columnInfos.add(new ColumnInfo().setName("col_2"));

        when(tableInfoMock.getColumns()).thenReturn(columnInfos);

        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        ValidationService service =
                new ValidationServiceImpl(apiClientFactory, miscConfig, workspaceHandlerMock, workloadTemplatesConfig);

        var actualRes = service.validate(provisioningRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateOutputPortUnexistingCol() throws Exception {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/descriptors/databricks/outputport_missing.yml");

        WorkspaceHandler workspaceHandlerMock = mock(WorkspaceHandler.class);

        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);

        when(workspaceHandlerMock.getWorkspaceInfo(any(String.class)))
                .thenReturn(right(Optional.of(mock(DatabricksWorkspaceInfo.class))));
        when(workspaceHandlerMock.getWorkspaceClient(any())).thenReturn(right(workspaceClientMock));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        ApiClient apiClientMock = mock(ApiClient.class);

        when(apiClientFactory.apply(any(ApiClientConfig.ApiClientConfigParams.class)))
                .thenReturn(apiClientMock);

        when(workspaceClientMock.tables()).thenReturn(mock(TablesAPI.class));

        when(workspaceClientMock.tables().exists("catalog_name.schema_name.table_name"))
                .thenReturn(tableExistsResponseMock);

        TableInfo tableInfoMock = mock(TableInfo.class);

        when(workspaceClientMock.tables().get("catalog_name.schema_name.table_name"))
                .thenReturn(tableInfoMock);

        // Mock columns of original table.
        Collection<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(new ColumnInfo().setName("col_1"));
        columnInfos.add(new ColumnInfo().setName("col_2"));

        when(tableInfoMock.getColumns()).thenReturn(columnInfos);

        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        ValidationService service =
                new ValidationServiceImpl(apiClientFactory, miscConfig, workspaceHandlerMock, workloadTemplatesConfig);

        var actualRes = service.validate(provisioningRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());

        String actualResDescription = actualRes.getLeft().problems().get(0).description();
        String expectedResDescription =
                "Check for Output Port test-op: the column 'unexisting_col' cannot be found in the table 'catalog_name.schema_name.table_name'.";

        assertEquals(expectedResDescription, actualResDescription);
    }
}
