package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.*;
import com.witboost.provisioning.model.Column;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.outputport.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest.*;
import it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest.CatalogInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class OutputPortReverseProvisionHandlerTest {

    private DatabricksOutputPortSpecific databricksOutputPortSpecific;

    @InjectMocks
    private OutputPortReverseProvisionHandler outputPortReverseProvisionHandler;

    @Mock
    private WorkspaceHandler workspaceHandler;

    @Mock
    private WorkspaceClient workspaceClient;

    private final DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspace_op", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);

    private ReverseProvisioningRequest createReverseProvisioningRequestForTests() {
        OutputPort<DatabricksOutputPortSpecific> outputPort = new OutputPort<>();
        databricksOutputPortSpecific = new DatabricksOutputPortSpecific();
        databricksOutputPortSpecific.setWorkspaceOP("workspace_op");
        databricksOutputPortSpecific.setCatalogName("catalog");
        databricksOutputPortSpecific.setSchemaName("schema");
        databricksOutputPortSpecific.setTableName("t");

        outputPort.setKind("outputport");
        outputPort.setSpecific(databricksOutputPortSpecific);

        CatalogInfo.Spec spec = new CatalogInfo.Spec();
        CatalogInfo.Spec.Mesh mesh = new CatalogInfo.Spec.Mesh();
        mesh.setName("opName");
        mesh.setSpecific(databricksOutputPortSpecific);
        spec.setMesh(mesh);

        CatalogInfo catalogInfo = new CatalogInfo();
        catalogInfo.setSpec(spec);

        OutputportReverseProvisioningParams.OutputPortReverseProvisioningSpecific specific =
                new OutputportReverseProvisioningParams.OutputPortReverseProvisioningSpecific();
        specific.setWorkspace("workspace_op");
        EnvironmentSpecificConfig environmentSpecificConfig = new EnvironmentSpecificConfig();
        environmentSpecificConfig.setSpecific(specific);

        OutputportReverseProvisioningParams params = new OutputportReverseProvisioningParams();
        params.setCatalogName("catalog_PARAMS");
        params.setSchemaName("schema_PARAMS");
        params.setTableName("table_PARAMS");
        params.setReverseProvisioningOption("SCHEMA_AND_DETAILS"); // or "SCHEMA_ONLY"
        params.setEnvironmentSpecificConfig(environmentSpecificConfig); // or "SCHEMA_ONLY"

        ReverseProvisioningRequest request = new ReverseProvisioningRequest("useCaseTemplateId", "qa");
        request.setParams(params);
        request.setCatalogInfo(catalogInfo);

        return request;
    }

    @Test
    public void testGetters() {

        CatalogInfo.Spec spec = new CatalogInfo.Spec();

        spec.setInstanceOf("setInstanceOf");
        spec.setType("setType");
        spec.setLifecycle("setLifecycle");
        spec.setOwner("setOwner");
        spec.setSystem("setSystem");
        spec.setDomain("setDomain");

        CatalogInfo catalogInfo = new CatalogInfo();
        catalogInfo.setSpec(spec);

        assertEquals("setInstanceOf", catalogInfo.getSpec().getInstanceOf());
        assertEquals("setType", catalogInfo.getSpec().getType());
        assertEquals("setLifecycle", catalogInfo.getSpec().getLifecycle());
        assertEquals("setOwner", catalogInfo.getSpec().getOwner());
        assertEquals("setSystem", catalogInfo.getSpec().getSystem());
        assertEquals("setDomain", catalogInfo.getSpec().getDomain());
    }

    @Test
    public void testReverseProvisionOutputPort_Success() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableExistsResponseMock);

        TableInfo tableInfoMock = mock(TableInfo.class);
        when(workspaceClient.tables().get("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableInfoMock);
        when(tableInfoMock.getTableType()).thenReturn(TableType.EXTERNAL);

        // Mock the schema of the table. Let's set 2 columns with no constraint
        Collection<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(new ColumnInfo()
                .setName("col_1")
                .setTypeName(ColumnTypeName.INT)
                .setNullable(true)
                .setTypeText("int"));
        columnInfos.add(new ColumnInfo()
                .setName("col_2")
                .setTypeName(ColumnTypeName.STRING)
                .setNullable(true)
                .setTypeText("string"));

        when(tableInfoMock.getColumns()).thenReturn(columnInfos);

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.COMPLETED, status.getStatus());

        HashMap<Object, Object> updates = (HashMap<Object, Object>) status.getUpdates();
        List<Column> listColumns = (List<Column>) updates.get("spec.mesh.dataContract.schema");
        Column col1 = listColumns.get(0);
        Column col2 = listColumns.get(1);

        assertEquals("col_1", col1.getName());
        assertEquals("int", col1.getDescription());
        assertEquals("INT", col1.getDataType());
        assertEquals(Optional.empty(), col1.getArrayDataType());
        assertEquals(Optional.empty(), col1.getDataLength());
        assertEquals(Optional.empty(), col1.getConstraint());
        assertEquals(Optional.empty(), col1.getPrecision());
        assertEquals(Optional.empty(), col1.getScale());
        assertEquals(List.of(), col1.getTags());

        assertEquals("col_2", col2.getName());
        assertEquals("string", col2.getDescription());
        assertEquals("STRING", col2.getDataType());
        assertEquals(Optional.empty(), col2.getArrayDataType());
        assertEquals(Optional.of(65535), col2.getDataLength());
        assertEquals(Optional.empty(), col2.getConstraint());
        assertEquals(Optional.empty(), col2.getPrecision());
        assertEquals(Optional.empty(), col2.getScale());
        assertEquals(List.of(), col2.getTags());

        assertEquals("catalog_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.catalogName"));
        assertEquals("schema_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.schemaName"));
        assertEquals("table_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.tableName"));

        assertEquals("catalog_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.catalogName"));
        assertEquals("schema_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.schemaName"));
        assertEquals("table_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.tableName"));
    }

    @Test
    public void testReverseProvisionOutputPort_Success_AllDataTypes() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableExistsResponseMock);

        TableInfo tableInfoMock = mock(TableInfo.class);
        when(workspaceClient.tables().get("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableInfoMock);
        when(tableInfoMock.getTableType()).thenReturn(TableType.EXTERNAL);

        // Mock the schema of the table. Let's set 2 columns with no constraint
        Collection<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(new ColumnInfo()
                .setName("col_1")
                .setTypeName(ColumnTypeName.INT)
                .setNullable(true)
                .setTypeText("int"));
        columnInfos.add(new ColumnInfo()
                .setName("col_2")
                .setTypeName(ColumnTypeName.CHAR)
                .setNullable(true)
                .setTypeText("char(2)"));
        columnInfos.add(new ColumnInfo()
                .setName("col_3")
                .setTypeName(ColumnTypeName.STRING)
                .setNullable(true)
                .setTypeText("varchar(3)"));
        columnInfos.add(new ColumnInfo()
                .setName("col_4")
                .setTypeName(ColumnTypeName.STRING)
                .setNullable(true)
                .setTypeText("string"));
        columnInfos.add(new ColumnInfo()
                .setName("col_5")
                .setTypeName(ColumnTypeName.DECIMAL)
                .setNullable(true)
                .setTypeText("decimal(5,4)")
                .setTypePrecision(5L)
                .setTypeScale(4L));

        when(tableInfoMock.getColumns()).thenReturn(columnInfos);

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.COMPLETED, status.getStatus());

        HashMap<Object, Object> updates = (HashMap<Object, Object>) status.getUpdates();
        List<Column> listColumns = (List<Column>) updates.get("spec.mesh.dataContract.schema");
        Column col1 = listColumns.get(0);
        Column col2 = listColumns.get(1);
        Column col3 = listColumns.get(2);
        Column col4 = listColumns.get(3);
        Column col5 = listColumns.get(4);

        assertEquals("col_1", col1.getName());
        assertEquals("int", col1.getDescription());
        assertEquals("INT", col1.getDataType());
        assertEquals(Optional.empty(), col1.getArrayDataType());
        assertEquals(Optional.empty(), col1.getDataLength());
        assertEquals(Optional.empty(), col1.getConstraint());
        assertEquals(Optional.empty(), col1.getPrecision());
        assertEquals(Optional.empty(), col1.getScale());
        assertEquals(List.of(), col1.getTags());

        assertEquals("col_2", col2.getName());
        assertEquals("char(2)", col2.getDescription());
        assertEquals("CHAR", col2.getDataType());
        assertEquals(Optional.empty(), col2.getArrayDataType());
        assertEquals(Optional.empty(), col2.getDataLength());
        assertEquals(Optional.empty(), col2.getConstraint());
        assertEquals(Optional.empty(), col2.getPrecision());
        assertEquals(Optional.empty(), col2.getScale());
        assertEquals(List.of(), col2.getTags());

        assertEquals("col_3", col3.getName());
        assertEquals("varchar(3)", col3.getDescription());
        assertEquals("STRING", col3.getDataType());
        assertEquals(Optional.empty(), col3.getArrayDataType());
        assertEquals(Optional.of(65535), col3.getDataLength());
        assertEquals(Optional.empty(), col3.getConstraint());
        assertEquals(Optional.empty(), col3.getPrecision());
        assertEquals(Optional.empty(), col3.getScale());
        assertEquals(List.of(), col3.getTags());

        assertEquals("col_4", col4.getName());
        assertEquals("string", col4.getDescription());
        assertEquals("STRING", col4.getDataType());
        assertEquals(Optional.empty(), col4.getArrayDataType());
        assertEquals(Optional.of(65535), col4.getDataLength());
        assertEquals(Optional.empty(), col4.getConstraint());
        assertEquals(Optional.empty(), col4.getPrecision());
        assertEquals(Optional.empty(), col4.getScale());
        assertEquals(List.of(), col4.getTags());

        assertEquals("col_5", col5.getName());
        assertEquals("decimal(5,4)", col5.getDescription());
        assertEquals("DECIMAL", col5.getDataType());
        assertEquals(Optional.empty(), col5.getArrayDataType());
        assertEquals(Optional.empty(), col5.getDataLength());
        assertEquals(Optional.empty(), col5.getConstraint());
        assertEquals(Optional.of(5), col5.getPrecision());
        assertEquals(Optional.of(4), col5.getScale());
        assertEquals(List.of(), col5.getTags());

        assertEquals("catalog_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.catalogName"));
        assertEquals("schema_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.schemaName"));
        assertEquals("table_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.tableName"));

        assertEquals("catalog_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.catalogName"));
        assertEquals("schema_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.schemaName"));
        assertEquals("table_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.tableName"));
    }

    @Test
    public void testReverseProvisionOutputPort_Failed_UnexistingDataTypes() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableExistsResponseMock);

        TableInfo tableInfoMock = mock(TableInfo.class);
        when(workspaceClient.tables().get("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableInfoMock);
        when(tableInfoMock.getTableType()).thenReturn(TableType.EXTERNAL);

        // Mock the schema of the table. Let's set 2 columns with no constraint
        Collection<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(new ColumnInfo().setName("col_1").setTypeName(ColumnTypeName.BYTE));

        when(tableInfoMock.getColumns()).thenReturn(columnInfos);

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        assertEquals(
                "Not able to convert data type 'BYTE' in Open Metadata",
                status.getLogs().get(0).getMessage());
        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, status.getStatus());
    }

    @Test
    public void testReverseProvisionOutputPort_Success_NotNullConstraint() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableExistsResponseMock);

        TableInfo tableInfoMock = mock(TableInfo.class);
        when(workspaceClient.tables().get("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableInfoMock);
        when(tableInfoMock.getTableType()).thenReturn(TableType.EXTERNAL);

        // Mock the schema of the table. Let's set 2 columns, where col_1 has a NOT_NULL constraint
        Collection<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(new ColumnInfo()
                .setName("col_1")
                .setTypeName(ColumnTypeName.INT)
                .setNullable(false)
                .setTypeText("int"));
        columnInfos.add(new ColumnInfo()
                .setName("col_2")
                .setTypeName(ColumnTypeName.STRING)
                .setNullable(true)
                .setTypeText("string"));

        when(tableInfoMock.getColumns()).thenReturn(columnInfos);

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        HashMap<Object, Object> updatesExpected = new HashMap<>();
        // updatesExpected.put("spec.mesh.dataContract.schema", schemaExpected);
        updatesExpected.put("spec.mesh.specific.catalogName", "catalog_PARAMS");
        updatesExpected.put("spec.mesh.specific.schemaName", "schema_PARAMS");
        updatesExpected.put("spec.mesh.specific.tableName", "table_PARAMS");

        // updatesExpected.put("witboost.parameters.schemaDefinition", schemaExpected);
        updatesExpected.put("witboost.parameters.catalogName", "catalog_PARAMS");
        updatesExpected.put("witboost.parameters.schemaName", "schema_PARAMS");
        updatesExpected.put("witboost.parameters.tableName", "table_PARAMS");

        // Assertions
        assertEquals(ReverseProvisioningStatus.StatusEnum.COMPLETED, status.getStatus());

        HashMap<Object, Object> updates = (HashMap<Object, Object>) status.getUpdates();
        List<Column> listColumns = (List<Column>) updates.get("spec.mesh.dataContract.schema");
        Column col1 = listColumns.get(0);
        Column col2 = listColumns.get(1);

        assertEquals("col_1", col1.getName());
        assertEquals("int", col1.getDescription());
        assertEquals("INT", col1.getDataType());
        assertEquals(Optional.empty(), col1.getArrayDataType());
        assertEquals(Optional.empty(), col1.getDataLength());
        assertEquals(Optional.of("NOT_NULL"), col1.getConstraint());
        assertEquals(Optional.empty(), col1.getPrecision());
        assertEquals(Optional.empty(), col1.getScale());
        assertEquals(List.of(), col1.getTags());

        assertEquals("col_2", col2.getName());
        assertEquals("string", col2.getDescription());
        assertEquals("STRING", col2.getDataType());
        assertEquals(Optional.empty(), col2.getArrayDataType());
        assertEquals(Optional.of(65535), col2.getDataLength());
        assertEquals(Optional.empty(), col2.getConstraint());
        assertEquals(Optional.empty(), col2.getPrecision());
        assertEquals(Optional.empty(), col2.getScale());
        assertEquals(List.of(), col2.getTags());

        assertEquals("catalog_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.catalogName"));
        assertEquals("schema_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.schemaName"));
        assertEquals("table_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.tableName"));

        assertEquals("catalog_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.catalogName"));
        assertEquals("schema_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.schemaName"));
        assertEquals("table_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.tableName"));
    }

    @Test
    public void testReverseProvisionOutputPort_Success_PrimaryKeyConstraint() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableExistsResponseMock);

        TableInfo tableInfoMock = mock(TableInfo.class);
        when(workspaceClient.tables().get("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableInfoMock);
        when(tableInfoMock.getTableType()).thenReturn(TableType.EXTERNAL);

        PrimaryKeyConstraint primaryKeyConstraint =
                new PrimaryKeyConstraint().setName("constraint_name").setChildColumns(List.of("col_1"));

        when(tableInfoMock.getTableConstraints())
                .thenReturn(List.of(new TableConstraint().setPrimaryKeyConstraint(primaryKeyConstraint)));

        // Mock the schema of the table. Let's set 2 columns, where col_1 has a NOT_NULL constraint and PRIMARY_KEY
        // constraints
        Collection<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(new ColumnInfo()
                .setName("col_1")
                .setTypeName(ColumnTypeName.INT)
                .setNullable(false)
                .setTypeText("int"));
        columnInfos.add(new ColumnInfo()
                .setName("col_2")
                .setTypeName(ColumnTypeName.STRING)
                .setNullable(true)
                .setTypeText("string"));

        when(tableInfoMock.getColumns()).thenReturn(columnInfos);

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.COMPLETED, status.getStatus());

        HashMap<Object, Object> updates = (HashMap<Object, Object>) status.getUpdates();
        List<Column> listColumns = (List<Column>) updates.get("spec.mesh.dataContract.schema");
        Column col1 = listColumns.get(0);
        Column col2 = listColumns.get(1);

        assertEquals("col_1", col1.getName());
        assertEquals("int", col1.getDescription());
        assertEquals("INT", col1.getDataType());
        assertEquals(Optional.empty(), col1.getArrayDataType());
        assertEquals(Optional.empty(), col1.getDataLength());
        assertEquals(Optional.of("PRIMARY_KEY"), col1.getConstraint());
        assertEquals(Optional.empty(), col1.getPrecision());
        assertEquals(Optional.empty(), col1.getScale());
        assertEquals(List.of(), col1.getTags());

        assertEquals("col_2", col2.getName());
        assertEquals("string", col2.getDescription());
        assertEquals("STRING", col2.getDataType());
        assertEquals(Optional.empty(), col2.getArrayDataType());
        assertEquals(Optional.of(65535), col2.getDataLength());
        assertEquals(Optional.empty(), col2.getConstraint());
        assertEquals(Optional.empty(), col2.getPrecision());
        assertEquals(Optional.empty(), col2.getScale());
        assertEquals(List.of(), col2.getTags());

        assertEquals("catalog_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.catalogName"));
        assertEquals("schema_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.schemaName"));
        assertEquals("table_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("spec.mesh.specific.tableName"));

        assertEquals("catalog_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.catalogName"));
        assertEquals("schema_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.schemaName"));
        assertEquals("table_PARAMS", ((HashMap<?, ?>) status.getUpdates()).get("witboost.parameters.tableName"));
    }

    @Test
    public void testReverseProvisionOutputPort_SuccessNoSchema() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableExistsResponseMock);

        TableInfo tableInfoMock = mock(TableInfo.class);
        when(workspaceClient.tables().get("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableInfoMock);
        when(tableInfoMock.getTableType()).thenReturn(TableType.EXTERNAL);

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        HashMap<Object, Object> updatesExpected = new HashMap<>();
        updatesExpected.put("spec.mesh.dataContract.schema", Collections.emptyList());
        updatesExpected.put("spec.mesh.specific.catalogName", "catalog_PARAMS");
        updatesExpected.put("spec.mesh.specific.schemaName", "schema_PARAMS");
        updatesExpected.put("spec.mesh.specific.tableName", "table_PARAMS");

        updatesExpected.put("witboost.parameters.schemaDefinition", Collections.emptyList());
        updatesExpected.put("witboost.parameters.catalogName", "catalog_PARAMS");
        updatesExpected.put("witboost.parameters.schemaName", "schema_PARAMS");
        updatesExpected.put("witboost.parameters.tableName", "table_PARAMS");

        assertEquals(ReverseProvisioningStatus.StatusEnum.COMPLETED, status.getStatus());
        assertEquals(updatesExpected, status.getUpdates());
    }

    @Test
    public void testReverseProvisionOutputPort_FailedGetWorkspaceInfo() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(left(new FailedOperation(List.of())));

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, status.getStatus());
        assertEquals(
                "Error while retrieving workspace info of workspace_op",
                status.getLogs().get(0).getMessage());
    }

    @Test
    public void testReverseProvisionOutputPort_FailedEmptyWorkspace() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(right(Optional.empty()));

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, status.getStatus());
        assertEquals(
                "Validation failed. Workspace 'workspace_op' not found.",
                status.getLogs().get(0).getMessage());
    }

    @Test
    public void testReverseProvisionOutputPort_FailedGetWorkspaceClient() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(left(new FailedOperation(List.of())));

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, status.getStatus());
        assertEquals(
                "Error while retrieving workspace client for workspace workspace_op.",
                status.getLogs().get(0).getMessage());
    }

    @Test
    public void testReverseProvisionOutputPort_FailedValidation_TableDoesNotExists() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        // make Validation fail for table does not exists
        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(false);

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableExistsResponseMock);

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, status.getStatus());
        assertEquals(
                "The table 'catalog_PARAMS.schema_PARAMS.table_PARAMS', provided in the Reverse Provisioning, request does not exist. ",
                status.getLogs().get(0).getMessage());
    }

    @Test
    public void testReverseProvisionOutputPort_FailedValidation_CannotInheritDetailsFromView() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableExistsResponseMock);

        TableInfo tableInfoMock = mock(TableInfo.class);
        when(workspaceClient.tables().get("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableInfoMock);
        when(tableInfoMock.getTableType()).thenReturn(TableType.VIEW); // Set TableType = VIEW

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, status.getStatus());
        assertEquals(
                "It's not possible to inherit table details from a VIEW. ",
                status.getLogs().get(0).getMessage());
    }

    @Test
    public void testReverseProvisionOutputPort_ExceptionWhileRetrievingColumnsList() {

        ReverseProvisioningRequest request = createReverseProvisioningRequestForTests();

        when(workspaceHandler.getWorkspaceInfo("workspace_op")).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceHandler.getWorkspaceClient(workspaceInfo)).thenReturn(right(workspaceClient));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableExistsResponseMock);

        TableInfo tableInfoMock = mock(TableInfo.class);
        when(tableInfoMock.getTableType()).thenReturn(TableType.MANAGED);

        when(workspaceClient.tables().get("catalog_PARAMS.schema_PARAMS.table_PARAMS"))
                .thenReturn(tableInfoMock)
                .thenThrow(new RuntimeException("Failure inside retrieveColumnList"));

        ReverseProvisioningStatus status = outputPortReverseProvisionHandler.reverseProvision(request);

        assertEquals(ReverseProvisioningStatus.StatusEnum.FAILED, status.getStatus());
        assertEquals(
                "An error occurred while retrieving column list from Databricks table 'catalog_PARAMS.schema_PARAMS.table_PARAMS'. Please try again and if the error persists contact the platform team. Details: Failure inside retrieveColumnList",
                status.getLogs().get(0).getMessage());
    }
}
