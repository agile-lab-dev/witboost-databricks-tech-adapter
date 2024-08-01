package it.agilelab.witboost.provisioning.databricks.service.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.TableExistsResponse;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TablesAPI;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.bean.DatabricksTableAPIBean;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.config.TemplatesConfig;
import it.agilelab.witboost.provisioning.databricks.openapi.model.DescriptorKind;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.util.ResourceUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
    DatabricksTableAPIBean databricksTableAPIBean;

    @Autowired
    MiscConfig miscConfig;

    private ValidationService service = new ValidationServiceImpl(databricksTableAPIBean, miscConfig);

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

        DatabricksTableAPIBean databricksTableAPIBeanMock = mock(DatabricksTableAPIBean.class);

        TablesAPI tablesAPIMock = mock(TablesAPI.class);
        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);

        when(tableExistsResponseMock.getTableExists()).thenReturn(true);
        when(tablesAPIMock.exists("catalog_name.schema_name.table_name")).thenReturn(tableExistsResponseMock);

        when(databricksTableAPIBeanMock.getObject("https://workspace_host.net")).thenReturn(tablesAPIMock);

        TableInfo tableInfoMock = mock(TableInfo.class);
        when(tablesAPIMock.get("catalog_name.schema_name.table_name")).thenReturn(tableInfoMock);

        // Mock columns of original table.
        Collection<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(new ColumnInfo().setName("col_1"));
        columnInfos.add(new ColumnInfo().setName("col_2"));

        when(tableInfoMock.getColumns()).thenReturn(columnInfos);

        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        ValidationService service = new ValidationServiceImpl(databricksTableAPIBeanMock, miscConfig);

        var actualRes = service.validate(provisioningRequest);

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testValidateOutputPortUnexistingCol() throws Exception {
        String ymlDescriptor = ResourceUtils.getContentFromResource("/pr_descriptor_outputport_missing.yml");

        DatabricksTableAPIBean databricksTableAPIBeanMock = mock(DatabricksTableAPIBean.class);

        TablesAPI tablesAPIMock = mock(TablesAPI.class);
        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);

        when(tableExistsResponseMock.getTableExists()).thenReturn(true);
        when(tablesAPIMock.exists("catalog_name.schema_name.table_name")).thenReturn(tableExistsResponseMock);

        when(databricksTableAPIBeanMock.getObject("https://workspace_host.net")).thenReturn(tablesAPIMock);

        TableInfo tableInfoMock = mock(TableInfo.class);
        when(tablesAPIMock.get("catalog_name.schema_name.table_name")).thenReturn(tableInfoMock);

        // Mock columns of original table.
        Collection<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(new ColumnInfo().setName("col_1"));
        columnInfos.add(new ColumnInfo().setName("col_2"));

        when(tableInfoMock.getColumns()).thenReturn(columnInfos);

        ProvisioningRequest provisioningRequest =
                new ProvisioningRequest(DescriptorKind.COMPONENT_DESCRIPTOR, ymlDescriptor, false);

        ValidationService service = new ValidationServiceImpl(databricksTableAPIBeanMock, miscConfig);

        var actualRes = service.validate(provisioningRequest);

        assertTrue(actualRes.isLeft());
        assertEquals(1, actualRes.getLeft().problems().size());

        String actualResDescription = actualRes.getLeft().problems().get(0).description();
        String expectedResDescription =
                "Check for Output Port test-op: the column 'unexisting_col' cannot be found in the table 'catalog_name.schema_name.table_name'.";

        assertEquals(expectedResDescription, actualResDescription);
    }
}
