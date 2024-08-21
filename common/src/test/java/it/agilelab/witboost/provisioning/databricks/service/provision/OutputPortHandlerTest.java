package it.agilelab.witboost.provisioning.databricks.service.provision;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.catalog.*;
import com.databricks.sdk.service.sql.*;
import com.databricks.sdk.service.workspace.WorkspaceAPI;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.DatabricksApiClientBean;
import it.agilelab.witboost.provisioning.databricks.bean.DatabricksWorkspaceClientBean;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
// @EnableConfigurationProperties
class OutputPortHandlerTest {

    private DataProduct dataProduct;
    private DatabricksOutputPortSpecific databricksOutputPortSpecific;
    private OutputPort outputPort;

    @Autowired
    private MiscConfig miscConfig;

    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @MockBean
    private AzureResourceManager azureResourceManager;

    @Autowired
    private AzureAuthConfig azureAuthConfig;

    @Autowired
    private GitCredentialsConfig gitCredentialsConfig;

    @Autowired
    private OutputPortHandler outputPortHandler;

    @MockBean
    private DatabricksApiClientBean databricksApiClientBean;

    @Mock
    WorkspaceClient workspaceClient;

    @Mock
    WorkspaceAPI workspaceAPI;

    @MockBean
    private DatabricksWorkspaceClientBean databricksWorkspaceClientBean;

    private DatabricksWorkspaceInfo databricksWorkspaceInfo =
            new DatabricksWorkspaceInfo("ws", "123", "https://ws.com", "abc", "test", ProvisioningState.SUCCEEDED);

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        dataProduct = new DataProduct();
        dataProduct.setDataProductOwner("user:dp.owner@email.com");
        dataProduct.setDevGroup("group:dev_group");
        dataProduct.setEnvironment("development");

        outputPort = new OutputPort<>();

        databricksOutputPortSpecific = new DatabricksOutputPortSpecific();
    }

    @Test
    public void provisionOutputPort_Success() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        ApiClient apiClientMock = mock(ApiClient.class);
        when(databricksApiClientBean.getObject("https://ws.com")).thenReturn(apiClientMock);

        List<DataSource> dataSourceList =
                Arrays.asList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

        when(new DataSourcesAPI(apiClientMock).list()).thenReturn(dataSourceList);

        // Mock classes and methods inside createOrReplaceOutputPortView
        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog("catalog_op")
                .setSchema("schema_op")
                .setStatement("CREATE OR REPLACE VIEW view AS SELECT * FROM catalog.schema.t;")
                .setWarehouseId("sql_wh_id");

        ExecuteStatementResponse executeStatementResponseMock = mock(ExecuteStatementResponse.class);
        when(executeStatementResponseMock.getStatementId()).thenReturn("id");
        when(new StatementExecutionAPI(apiClientMock).executeStatement(request))
                .thenReturn(executeStatementResponseMock);

        // Mocking method inside pollOnStatementExecution
        GetStatementResponse getStatementResponseMockPoll = mock(GetStatementResponse.class);
        when(new StatementExecutionAPI(apiClientMock).getStatement("id")).thenReturn(getStatementResponseMockPoll);

        StatementStatus statementStatusMockPoll = mock(StatementStatus.class);
        when(statementStatusMockPoll.getState()).thenReturn(StatementState.SUCCEEDED);

        when(getStatementResponseMockPoll.getStatus()).thenReturn(statementStatusMockPoll);

        // Mocking method inside retrieve tableInfo
        TableInfo tableInfoMock = mock(TableInfo.class);
        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().get("catalog_op.schema_op.view")).thenReturn(tableInfoMock);
        when(tableInfoMock.getTableId()).thenReturn("table_id");

        // Mocking permissions call
        when(workspaceClient.grants()).thenReturn(mock(GrantsAPI.class));

        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assert result.isRight();
        assertEquals(result.get().getTableId(), "table_id");
    }

    @Test
    public void provisionOutputPort_SuccesspollOnStatementExecutionRUNNING() {

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        ApiClient apiClientMock = mock(ApiClient.class);
        when(databricksApiClientBean.getObject("https://ws.com")).thenReturn(apiClientMock);

        List<DataSource> dataSourceList =
                Arrays.asList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

        when(new DataSourcesAPI(apiClientMock).list()).thenReturn(dataSourceList);

        // Mock classes and methods inside createOrReplaceOutputPortView
        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog("catalog_op")
                .setSchema("schema_op")
                .setStatement("CREATE OR REPLACE VIEW view AS SELECT * FROM catalog.schema.t;")
                .setWarehouseId("sql_wh_id");

        ExecuteStatementResponse executeStatementResponseMock = mock(ExecuteStatementResponse.class);
        when(executeStatementResponseMock.getStatementId()).thenReturn("id");
        when(new StatementExecutionAPI(apiClientMock).executeStatement(request))
                .thenReturn(executeStatementResponseMock);

        // Mocking method inside pollOnStatementExecution
        GetStatementResponse getStatementResponseMockPoll = mock(GetStatementResponse.class);
        when(new StatementExecutionAPI(apiClientMock).getStatement("id")).thenReturn(getStatementResponseMockPoll);

        StatementStatus statementStatusMockPoll = mock(StatementStatus.class);
        when(statementStatusMockPoll.getState())
                .thenReturn(StatementState.PENDING)
                .thenReturn(StatementState.RUNNING)
                .thenReturn(StatementState.SUCCEEDED);

        when(getStatementResponseMockPoll.getStatus()).thenReturn(statementStatusMockPoll);

        // Mocking method inside retrieve tableInfo
        TableInfo tableInfoMock = mock(TableInfo.class);
        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().get("catalog_op.schema_op.view")).thenReturn(tableInfoMock);
        when(tableInfoMock.getTableId()).thenReturn("table_id");

        // Mocking permissions call
        when(workspaceClient.grants()).thenReturn(mock(GrantsAPI.class));

        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assert result.isRight();
        assertEquals(result.get().getTableId(), "table_id");
    }

    @Test
    public void provisionOutputPort_ExecuteStatementCreateOutputPortFailure() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        ApiClient apiClientMock = mock(ApiClient.class);
        when(databricksApiClientBean.getObject("https://ws.com")).thenReturn(apiClientMock);

        List<DataSource> dataSourceList =
                Arrays.asList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

        when(new DataSourcesAPI(apiClientMock).list()).thenReturn(dataSourceList);

        // Mock classes and methods inside createOrReplaceOutputPortView
        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog("catalog_op")
                .setSchema("schema_op")
                .setStatement("CREATE OR REPLACE VIEW view AS SELECT * FROM catalog.schema.t;")
                .setWarehouseId("sql_wh_id");

        ExecuteStatementResponse executeStatementResponseMock = mock(ExecuteStatementResponse.class);
        when(executeStatementResponseMock.getStatementId()).thenReturn("statement_id");
        when(new StatementExecutionAPI(apiClientMock).executeStatement(request))
                .thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assertTrue(result.isLeft());
        String messageError =
                "An error occurred while creating view 'catalog_op.schema_op.view'. Please try again and if the error persists contact the platform team. Details: Exception";
        assertEquals(messageError, result.getLeft().problems().get(0).description());
    }

    @Test
    public void provisionOutputPort_FailureCreatingCatalog() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        when(workspaceClient.catalogs()).thenThrow(new DatabricksException("Generic Error"));

        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        String errorMessage =
                "An error occurred trying to search the catalog catalog_op. Please try again and if the error persists contact the platform team. Details: Generic Error";
        assert result.isLeft();
        assertTrue(result.getLeft().problems().get(0).description().equalsIgnoreCase(errorMessage));

        // assertEquals(result.get(), "table_id");
    }

    @Test
    public void provisionOutputPort_FailureCreatingSchema() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Failure on schema check
        when(workspaceClient.schemas()).thenThrow(new DatabricksException("Generic Error"));

        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        String errorMessage =
                "An error occurred trying to search the schema 'schema_op' in catalog 'catalog_op'. Please try again and if the error persists contact the platform team. Details: Generic Error";
        assert result.isLeft();
        assertTrue(result.getLeft().problems().get(0).description().equalsIgnoreCase(errorMessage));
    }

    @Test
    public void unprovisionOutputPort_SuccessTableExists() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op. Schema exists
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock classes and methods inside dropTableIfExists
        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog_op.schema_op.view")).thenReturn(tableExistsResponseMock);

        Either<FailedOperation, Void> result =
                outputPortHandler.unprovisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assert result.isRight();
    }

    @Test
    public void unprovisionOutputPort_SuccessSkippingCatalogDoesNotExists() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking behaviour on catalogs. The requested catalog (catalog_op) does not exists
        List<CatalogInfo> catalogList = Arrays.asList(new CatalogInfo().setName("catalog"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        Either<FailedOperation, Void> result =
                outputPortHandler.unprovisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assert result.isRight();
    }

    @Test
    public void unprovisionOutputPort_FailureCheckCatalogExistence() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, Void> result =
                outputPortHandler.unprovisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assertTrue(result.isLeft());
        String messageError =
                "An error occurred trying to search the catalog catalog_op. Please try again and if the error persists contact the platform team. Details: Exception";
        assertEquals(messageError, result.getLeft().problems().get(0).description());
    }

    @Test
    public void unprovisionOutputPort_SuccessSkippingSchemaDoesNotExists() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op. Schema exists
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        Either<FailedOperation, Void> result =
                outputPortHandler.unprovisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assert result.isRight();
    }

    @Test
    public void unprovisionOutputPort_FailureCheckSchemaExistence() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op. Schema exists
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, Void> result =
                outputPortHandler.unprovisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assertTrue(result.isLeft());
        String messageError =
                "An error occurred trying to search the schema 'schema_op' in catalog 'catalog_op'. Please try again and if the error persists contact the platform team. Details: Exception";

        assertEquals(messageError, result.getLeft().problems().get(0).description());
    }

    @Test
    public void unprovisionOutputPort_FailureDropTableIfExists() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op. Schema exists
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock classes and methods inside dropTableIfExists
        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog_op.schema_op.view")).thenReturn(tableExistsResponseMock);

        TablesAPI tablesAPIMock = mock(TablesAPI.class);
        when(workspaceClient.tables()).thenReturn(tablesAPIMock);
        when(tablesAPIMock.exists("catalog_op.schema_op.view")).thenReturn(tableExistsResponseMock);

        doThrow(new RuntimeException("Exception")).when(tablesAPIMock).delete("catalog_op.schema_op.view");

        Either<FailedOperation, Void> result =
                outputPortHandler.unprovisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assertTrue(result.isLeft());
        String messageError =
                "An error occurred while dropping table 'catalog_op.schema_op.view'. Please try again and if the error persists contact the platform team. Details: Exception";
        assertEquals(messageError, result.getLeft().problems().get(0).description());
    }

    @Test
    public void test_pollOnStatementExecutionFailure_CANCELED() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        ApiClient apiClientMock = mock(ApiClient.class);
        when(databricksApiClientBean.getObject("https://ws.com")).thenReturn(apiClientMock);

        List<DataSource> dataSourceList =
                Arrays.asList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

        when(new DataSourcesAPI(apiClientMock).list()).thenReturn(dataSourceList);

        // Mock classes and methods inside createOrReplaceOutputPortView
        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog("catalog_op")
                .setSchema("schema_op")
                .setStatement("CREATE OR REPLACE VIEW view AS SELECT * FROM catalog.schema.t;")
                .setWarehouseId("sql_wh_id");

        ExecuteStatementResponse executeStatementResponseMock = mock(ExecuteStatementResponse.class);
        when(executeStatementResponseMock.getStatementId()).thenReturn("id");
        when(new StatementExecutionAPI(apiClientMock).executeStatement(request))
                .thenReturn(executeStatementResponseMock);

        // Mocking method inside pollOnStatementExecution
        GetStatementResponse getStatementResponseMockPoll = mock(GetStatementResponse.class);
        when(new StatementExecutionAPI(apiClientMock).getStatement("id")).thenReturn(getStatementResponseMockPoll);

        StatementStatus statementStatusMockPoll = mock(StatementStatus.class);

        // poll returns CANCELED state
        when(statementStatusMockPoll.getState()).thenReturn(StatementState.CANCELED);

        when(getStatementResponseMockPoll.getStatus()).thenReturn(statementStatusMockPoll);

        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assertTrue(result.isLeft());
        String messageError = "Status of statement (id: id): CANCELED. ";
        assertEquals(messageError, result.getLeft().problems().get(0).description());
    }

    @Test
    public void provisionOutputPort_GetSqlWarehouseIdFailure() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        ApiClient apiClientMock = mock(ApiClient.class);
        when(databricksApiClientBean.getObject("https://ws.com")).thenReturn(apiClientMock);

        List<DataSource> dataSourceList =
                Arrays.asList(new DataSource().setName("sql_fake").setWarehouseId("sql_wh_id_fake"));

        when(new DataSourcesAPI(apiClientMock).list()).thenReturn(dataSourceList);

        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assertTrue(result.isLeft());
        String messageError =
                "An error occurred while searching for Sql Warehouse 'sql_wh' details. Please try again and if the error persists contact the platform team. Details: Sql Warehouse not found.";
        assertEquals(messageError, result.getLeft().problems().get(0).description());
    }

    private ProvisionRequest<DatabricksOutputPortSpecific> createOPProvisionRequest() {
        databricksOutputPortSpecific.setWorkspace("ws");
        databricksOutputPortSpecific.setCatalogName("catalog");
        databricksOutputPortSpecific.setSchemaName("schema");
        databricksOutputPortSpecific.setTableName("t");
        databricksOutputPortSpecific.setSqlWarehouseName("sql_wh");
        databricksOutputPortSpecific.setWorkspaceOP("ws_op");
        databricksOutputPortSpecific.setCatalogNameOP("catalog_op");
        databricksOutputPortSpecific.setSchemaNameOP("schema_op");
        databricksOutputPortSpecific.setViewNameOP("view");

        outputPort.setSpecific(databricksOutputPortSpecific);

        ProvisionRequest provisionRequest = new ProvisionRequest<>(dataProduct, outputPort, false);
        return provisionRequest;
    }
}
