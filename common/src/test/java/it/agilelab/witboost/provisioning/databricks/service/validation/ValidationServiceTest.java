package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.right;
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
import it.agilelab.witboost.provisioning.databricks.bean.DatabricksApiClientBean;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.config.TemplatesConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.DescriptorKind;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import it.agilelab.witboost.provisioning.databricks.util.ResourceUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Optional;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class ValidationServiceTest {

    @Autowired
    private TemplatesConfig templatesConfig;

    @Mock
    DatabricksApiClientBean databricksApiClientBean;

    @Mock
    WorkspaceHandler workspaceHandler;

    @Autowired
    MiscConfig miscConfig;

    private ValidationService service =
            new ValidationServiceImpl(databricksApiClientBean, miscConfig, workspaceHandler);

    @Test
    public void testValidateWorkloadOk() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_workload.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        var actualRes = service.validate(provisioningRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateWrongDescriptorKind() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_outputport.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.DATAPRODUCT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "The descriptorKind field is not valid. Expected: 'COMPONENT_DESCRIPTOR', Actual: 'DATAPRODUCT_DESCRIPTOR'";

        var actualResult = service.validate(provisioningRequest);

        Assertions.assertTrue(actualResult.isLeft());
        Assertions.assertEquals(1, actualResult.getLeft().problems().size());
        actualResult.getLeft().problems().forEach(p -> {
            Assertions.assertEquals(expectedDesc, p.description());
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
        Assertions.assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            Assertions.assertTrue(p.description().startsWith(expectedDesc));
            Assertions.assertTrue(p.cause().isPresent());
        });
    }

    @Test
    public void testValidateMissingComponentIdToProvision() throws IOException {
        String ymlDescriptor =
                ResourceUtils.getContentFromResource("/pr_descriptor_storage_missing_componentIdToProvision.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc = "Component with ID null not found in the Descriptor";

        var actualRes = service.validate(provisioningRequest);

        Assertions.assertTrue(actualRes.isLeft());
        Assertions.assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            Assertions.assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateMissingComponentToProvision() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_storage_missing_component.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "Component with ID urn:dmb:cmp:healthcare:vaccinations:0:storage not found in the Descriptor";

        var actualResult = service.validate(provisioningRequest);

        Assertions.assertTrue(actualResult.isLeft());
        Assertions.assertEquals(1, actualResult.getLeft().problems().size());
        actualResult.getLeft().problems().forEach(p -> {
            Assertions.assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateMissingComponentKindToProvision() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_storage_missing_componentKind.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "Component Kind not found for the component with ID urn:dmb:cmp:healthcare:vaccinations:0:storage";

        var actualRes = service.validate(provisioningRequest);

        Assertions.assertTrue(actualRes.isLeft());
        Assertions.assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            Assertions.assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateWrongComponentKindToProvision() throws IOException {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_storage_wrong_componentKind.yml");
        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);
        String expectedDesc =
                "The kind 'wrong' of the component to provision is not supported by this Specific Provisioner";

        var actualRes = service.validate(provisioningRequest);

        Assertions.assertTrue(actualRes.isLeft());
        Assertions.assertEquals(1, actualRes.getLeft().problems().size());
        actualRes.getLeft().problems().forEach(p -> {
            Assertions.assertEquals(expectedDesc, p.description());
            Assertions.assertTrue(p.cause().isEmpty());
        });
    }

    @Test
    public void testValidateOutputPortOk() throws Exception {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_outputport_ok.yml");

        DatabricksApiClientBean databricksApiClientBeanMock = mock(DatabricksApiClientBean.class);

        WorkspaceHandler workspaceHandlerMock = mock(WorkspaceHandler.class);

        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);

        when(workspaceHandlerMock.getWorkspaceInfo(any(String.class)))
                .thenReturn(right(Optional.of(mock(DatabricksWorkspaceInfo.class))));
        when(workspaceHandlerMock.getWorkspaceClient(any())).thenReturn(right(workspaceClientMock));

        when(workspaceHandlerMock.getWorkspaceHost("workspace_name")).thenReturn(right("http://workspace"));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        ApiClient apiClientMock = mock(ApiClient.class);

        when(databricksApiClientBeanMock.getObject("http://workspace")).thenReturn(apiClientMock);

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
                new ValidationServiceImpl(databricksApiClientBeanMock, miscConfig, workspaceHandlerMock);

        var actualRes = service.validate(provisioningRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateOutputPortUnexistingCol() throws Exception {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_outputport_missing.yml");

        DatabricksApiClientBean databricksApiClientBeanMock = mock(DatabricksApiClientBean.class);

        WorkspaceHandler workspaceHandlerMock = mock(WorkspaceHandler.class);

        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);

        when(workspaceHandlerMock.getWorkspaceInfo(any(String.class)))
                .thenReturn(right(Optional.of(mock(DatabricksWorkspaceInfo.class))));
        when(workspaceHandlerMock.getWorkspaceClient(any())).thenReturn(right(workspaceClientMock));
        when(workspaceHandlerMock.getWorkspaceHost("workspace_name")).thenReturn(right("http://workspace"));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        ApiClient apiClientMock = mock(ApiClient.class);

        when(databricksApiClientBeanMock.getObject("http://workspace")).thenReturn(apiClientMock);

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
                new ValidationServiceImpl(databricksApiClientBeanMock, miscConfig, workspaceHandlerMock);

        var actualRes = service.validate(provisioningRequest);

        assertTrue(actualRes.isLeft());
        Assertions.assertEquals(1, actualRes.getLeft().problems().size());

        String actualResDescription = actualRes.getLeft().problems().get(0).description();
        String expectedResDescription =
                "Check for Output Port test-op: the column 'unexisting_col' cannot be found in the table 'catalog_name.schema_name.table_name'.";

        Assertions.assertEquals(expectedResDescription, actualResDescription);
    }
}
