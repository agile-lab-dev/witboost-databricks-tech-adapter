package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.catalog.*;
import com.databricks.sdk.service.iam.AccountGroupsAPI;
import com.databricks.sdk.service.iam.Group;
import com.databricks.sdk.service.sql.*;
import com.databricks.sdk.service.workspace.WorkspaceAPI;
import com.witboost.provisioning.model.Column;
import com.witboost.provisioning.model.DataContract;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.ApiClientConfig;
import it.agilelab.witboost.provisioning.databricks.bean.WorkspaceClientConfig;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.object.View;
import it.agilelab.witboost.provisioning.databricks.model.databricks.outputport.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisionInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.openapi.model.UpdateAclRequest;
import java.util.*;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;

@SpringBootTest
class OutputPortHandlerTest {

    private DataProduct dataProduct;
    private DatabricksOutputPortSpecific databricksOutputPortSpecific;
    private OutputPort<DatabricksOutputPortSpecific> outputPort;

    @MockBean
    private AzureResourceManager azureResourceManager;

    @MockBean
    private AccountClient accountClient;

    @Autowired
    private OutputPortHandler outputPortHandler;

    @MockBean
    private Function<ApiClientConfig.ApiClientConfigParams, ApiClient> apiClientFactory;

    @Mock
    ApiClient apiClientMock;

    @MockBean
    private Function<WorkspaceClientConfig.WorkspaceClientConfigParams, WorkspaceClient> workspaceClientFactory;

    @Mock
    WorkspaceClient workspaceClient;

    @Mock
    WorkspaceAPI workspaceAPI;

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
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("dev_group")));

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        // Mocking metastore attachment
        MetastoresAPI metastoresAPIMock = mock(MetastoresAPI.class);

        Iterable<MetastoreInfo> iterableMetastoresList = Collections.singletonList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"));

        when(workspaceClient.metastores()).thenReturn(metastoresAPIMock);
        when(metastoresAPIMock.list()).thenReturn(iterableMetastoresList);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op
        List<SchemaInfo> schemaInfoList = Collections.singletonList(
                new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        List<DataSource> dataSourceList =
                Collections.singletonList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

        when(workspaceClient.dataSources()).thenReturn(mock(DataSourcesAPI.class));
        when(workspaceClient.dataSources().list()).thenReturn(dataSourceList);

        // Mock classes and methods inside createOrReplaceOutputPortView
        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog("catalog_op")
                .setSchema("schema_op")
                .setStatement("CREATE OR REPLACE VIEW `view` AS SELECT col_1,col_2 FROM `catalog`.`schema`.`t`;")
                .setWarehouseId("sql_wh_id");

        StatementResponse executeStatementResponseMock = mock(StatementResponse.class);

        when(new StatementExecutionAPI(apiClientMock).executeStatement(request))
                .thenReturn(executeStatementResponseMock);
        when(executeStatementResponseMock.getStatementId()).thenReturn("id");

        // Mocking method inside pollOnStatementExecution
        StatementResponse getStatementResponseMockPoll = mock(StatementResponse.class);
        when(new StatementExecutionAPI(apiClientMock).getStatement("id")).thenReturn(getStatementResponseMockPoll);

        StatementStatus statementStatusMockPoll = mock(StatementStatus.class);
        when(statementStatusMockPoll.getState()).thenReturn(StatementState.SUCCEEDED);

        when(getStatementResponseMockPoll.getStatus()).thenReturn(statementStatusMockPoll);

        when(workspaceClient.statementExecution()).thenReturn(mock(StatementExecutionAPI.class));
        StatementResponse statementResponse = new StatementResponse();
        statementResponse.setStatementId("id");
        statementResponse.setStatus(new StatementStatus().setState(StatementState.SUCCEEDED));
        when(workspaceClient.statementExecution().executeStatement(any())).thenReturn(statementResponse);
        when(workspaceClient.statementExecution().getStatement("id")).thenReturn(statementResponse);

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
        assertEquals("table_id", result.get().getTableId());
    }

    @Test
    public void provisionOutputPort_SuccessWithEmptySchema() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequestEmptySchema();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        // Mocking metastore attachment
        MetastoresAPI metastoresAPIMock = mock(MetastoresAPI.class);

        Iterable<MetastoreInfo> iterableMetastoresList = Collections.singletonList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"));

        when(workspaceClient.metastores()).thenReturn(metastoresAPIMock);
        when(metastoresAPIMock.list()).thenReturn(iterableMetastoresList);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op
        List<SchemaInfo> schemaInfoList = Collections.singletonList(
                new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        List<DataSource> dataSourceList =
                Collections.singletonList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

        when(workspaceClient.dataSources()).thenReturn(mock(DataSourcesAPI.class));
        when(workspaceClient.dataSources().list()).thenReturn(dataSourceList);

        when(new DataSourcesAPI(apiClientMock).list()).thenReturn(dataSourceList);

        // Mock classes and methods inside createOrReplaceOutputPortView
        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog("catalog_op")
                .setSchema("schema_op")
                .setStatement("CREATE OR REPLACE VIEW `view` AS SELECT * FROM `catalog`.`schema`.`t`;")
                .setWarehouseId("sql_wh_id");

        StatementResponse executestatementResponseMock = mock(StatementResponse.class);
        when(executestatementResponseMock.getStatementId()).thenReturn("id");
        when(new StatementExecutionAPI(apiClientMock).executeStatement(request))
                .thenReturn(executestatementResponseMock);

        // Mocking method inside pollOnStatementExecution
        StatementResponse getstatementResponseMockPoll = mock(StatementResponse.class);
        when(new StatementExecutionAPI(apiClientMock).getStatement("id")).thenReturn(getstatementResponseMockPoll);

        StatementStatus statementStatusMockPoll = mock(StatementStatus.class);
        when(statementStatusMockPoll.getState()).thenReturn(StatementState.SUCCEEDED);

        when(getstatementResponseMockPoll.getStatus()).thenReturn(statementStatusMockPoll);

        when(workspaceClient.statementExecution()).thenReturn(mock(StatementExecutionAPI.class));
        StatementResponse statementResponse = new StatementResponse();
        statementResponse.setStatementId("id");
        statementResponse.setStatus(new StatementStatus().setState(StatementState.SUCCEEDED));
        when(workspaceClient.statementExecution().executeStatement(any())).thenReturn(statementResponse);
        when(workspaceClient.statementExecution().getStatement("id")).thenReturn(statementResponse);

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
        assertEquals("table_id", result.get().getTableId());
    }

    @Test
    public void provisionOutputPort_Exception() {
        OutputPort<DatabricksOutputPortSpecific> op = new OutputPort<>();
        op.setName("op");
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, op, false);
        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains(
                        "An error occurred while provisioning component op. Please try again and if the error persists contact the platform team.");
    }

    @Test
    public void provisionOutputPort_ExecuteStatementCreateOrReplaceViewFailure() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking metastore attachment
        MetastoresAPI metastoresAPIMock = mock(MetastoresAPI.class);

        Iterable<MetastoreInfo> iterableMetastoresList = Collections.singletonList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"));

        when(workspaceClient.metastores()).thenReturn(metastoresAPIMock);
        when(metastoresAPIMock.list()).thenReturn(iterableMetastoresList);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op
        List<SchemaInfo> schemaInfoList = Collections.singletonList(
                new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        List<DataSource> dataSourceList =
                Collections.singletonList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

        when(workspaceClient.dataSources()).thenReturn(mock(DataSourcesAPI.class));
        when(workspaceClient.dataSources().list()).thenReturn(dataSourceList);

        StatementResponse statementResponseMock = mock(StatementResponse.class);
        when(statementResponseMock.getStatementId()).thenReturn("statement_id");
        when(workspaceClient.statementExecution()).thenReturn(mock(StatementExecutionAPI.class));
        when(workspaceClient.statementExecution()).thenThrow(new RuntimeException("Exception executing statement"));

        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assertTrue(result.isLeft());
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("An error occurred while running query 'CREATE OR REPLACE VIEW `view`");
        assert (result.getLeft().problems().get(0).description().contains("Exception executing statement"));
    }

    @Test
    public void provisionOutputPort_FailureCreatingCatalog() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        MetastoresAPI metastoresAPIMock = mock(MetastoresAPI.class);

        Iterable<MetastoreInfo> iterableMetastoresList = Collections.singletonList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"));

        when(workspaceClient.metastores()).thenReturn(metastoresAPIMock);
        when(metastoresAPIMock.list()).thenReturn(iterableMetastoresList);

        when(workspaceClient.catalogs()).thenThrow(new DatabricksException("Generic Error"));

        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assert result.isLeft();
        assertEquals(
                "An error occurred trying to search the catalog catalog_op. Please try again and if the error persists contact the platform team. Details: Generic Error",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void provisionOutputPort_FailureCreatingSchema() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking metastore attachment
        MetastoresAPI metastoresAPIMock = mock(MetastoresAPI.class);

        Iterable<MetastoreInfo> iterableMetastoresList = Collections.singletonList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"));

        when(workspaceClient.metastores()).thenReturn(metastoresAPIMock);
        when(metastoresAPIMock.list()).thenReturn(iterableMetastoresList);

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
        List<SchemaInfo> schemaInfoList = Collections.singletonList(
                new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

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

        // Mocking behaviour on catalogs. The requested catalog (catalog_op) does not exist
        List<CatalogInfo> catalogList = Collections.singletonList(new CatalogInfo().setName("catalog"));

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
        List<SchemaInfo> schemaInfoList = Collections.singletonList(
                new SchemaInfo().setCatalogName("catalog_op").setName("schema"));

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
        List<SchemaInfo> schemaInfoList = Collections.singletonList(
                new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

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
    public void pollOnStatementExecutionFailure_CANCELED() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking metastore attachment
        MetastoresAPI metastoresAPIMock = mock(MetastoresAPI.class);

        Iterable<MetastoreInfo> iterableMetastoresList = Collections.singletonList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"));

        when(workspaceClient.metastores()).thenReturn(metastoresAPIMock);
        when(metastoresAPIMock.list()).thenReturn(iterableMetastoresList);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op
        List<SchemaInfo> schemaInfoList = Collections.singletonList(
                new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        List<DataSource> dataSourceList =
                Collections.singletonList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

        when(workspaceClient.dataSources()).thenReturn(mock(DataSourcesAPI.class));
        when(workspaceClient.dataSources().list()).thenReturn(dataSourceList);

        // Mock classes and methods inside createOrReplaceOutputPortView
        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog("catalog_op")
                .setSchema("schema_op")
                .setStatement("CREATE OR REPLACE VIEW `view` AS SELECT col_1,col_2 FROM `catalog`.`schema`.`t`;")
                .setWarehouseId("sql_wh_id");

        StatementResponse statementResponseMock = mock(StatementResponse.class);
        when(statementResponseMock.getStatementId()).thenReturn("id");
        when(new StatementExecutionAPI(apiClientMock).executeStatement(request)).thenReturn(statementResponseMock);

        // Mocking method inside pollOnStatementExecution
        StatementResponse getstatementResponseMockPoll = mock(StatementResponse.class);
        when(new StatementExecutionAPI(apiClientMock).getStatement("id")).thenReturn(getstatementResponseMockPoll);

        StatementStatus statementStatusMockPoll = mock(StatementStatus.class);

        var error = new ServiceError();
        when(statementStatusMockPoll.getError()).thenReturn(error.setMessage("Error!"));

        when(workspaceClient.statementExecution()).thenReturn(mock(StatementExecutionAPI.class));
        StatementResponse statementResponse = new StatementResponse();
        statementResponse.setStatementId("id");
        statementResponse.setStatus(
                new StatementStatus().setState(StatementState.CANCELED).setError(error));
        when(workspaceClient.statementExecution().executeStatement(any())).thenReturn(statementResponse);
        when(workspaceClient.statementExecution().getStatement("id")).thenReturn(statementResponse);

        // poll returns CANCELED state
        when(statementStatusMockPoll.getState()).thenReturn(StatementState.CANCELED);

        when(getstatementResponseMockPoll.getStatus()).thenReturn(statementStatusMockPoll);

        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assertTrue(result.isLeft());
        String messageError = "Status of statement (id: id): CANCELED. ";
        assertTrue(result.getLeft().problems().get(0).description().contains(messageError));
    }

    @Test
    public void provisionOutputPort_GetSqlWarehouseIdFailure() {
        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        // Mocking metastore attachment
        MetastoresAPI metastoresAPIMock = mock(MetastoresAPI.class);

        Iterable<MetastoreInfo> iterableMetastoresList = Collections.singletonList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"));

        when(workspaceClient.metastores()).thenReturn(metastoresAPIMock);
        when(metastoresAPIMock.list()).thenReturn(iterableMetastoresList);

        // Mocking behaviour on catalogs. The requested catalog exists
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog_op"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        // Mocking schema_op in catalog_op
        List<SchemaInfo> schemaInfoList = Collections.singletonList(
                new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        when(apiClientFactory.apply(any(ApiClientConfig.ApiClientConfigParams.class)))
                .thenReturn(apiClientMock);

        List<DataSource> dataSourceList =
                Collections.singletonList(new DataSource().setName("sql_fake").setWarehouseId("sql_wh_id_fake"));

        when(workspaceClient.dataSources()).thenReturn(mock(DataSourcesAPI.class));
        when(workspaceClient.dataSources().list()).thenReturn(dataSourceList);

        Either<FailedOperation, TableInfo> result =
                outputPortHandler.provisionOutputPort(provisionRequest, workspaceClient, databricksWorkspaceInfo);

        assertTrue(result.isLeft());
        String messageError =
                "An error occurred while searching for Sql Warehouse 'sql_wh' details. Please try again and if the error persists contact the platform team. Details: Sql Warehouse not found.";
        assertEquals(messageError, result.getLeft().problems().get(0).description());
    }

    @Test
    public void updateAcl_Success() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        UpdateAclRequest updateAclRequest = new UpdateAclRequest(
                List.of("user:a_email.com", "group:group_test"),
                new ProvisionInfo(provisionRequest.toString(), "result"));

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        GrantsAPI grantsAPIMock = mock(GrantsAPI.class);
        when(workspaceClient.grants()).thenReturn(grantsAPIMock);
        when(grantsAPIMock.get(any(SecurableType.class), anyString())).thenReturn(mock(PermissionsList.class));

        UnityCatalogManager unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

        Either<FailedOperation, ProvisioningStatus> result =
                outputPortHandler.updateAcl(provisionRequest, updateAclRequest, workspaceClient, unityCatalogManager);

        assert result.isRight();
        assertEquals(ProvisioningStatus.StatusEnum.COMPLETED, result.get().getStatus());
        assertEquals("Update of Acl completed!", result.get().getResult());
    }

    @Test
    public void updateAcl_DatabricksMappingFailure() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        UpdateAclRequest updateAclRequest = new UpdateAclRequest(
                List.of("a_email.com", "group:group_test"), new ProvisionInfo(provisionRequest.toString(), "result"));

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        GrantsAPI grantsAPIMock = mock(GrantsAPI.class);
        when(workspaceClient.grants()).thenReturn(grantsAPIMock);
        when(grantsAPIMock.get(any(SecurableType.class), anyString())).thenReturn(mock(PermissionsList.class));

        UnityCatalogManager unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

        Either<FailedOperation, ProvisioningStatus> result =
                outputPortHandler.updateAcl(provisionRequest, updateAclRequest, workspaceClient, unityCatalogManager);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("The subject a_email.com is neither a Witboost user nor a group");
    }

    @Test
    public void updateAcl_DatabricksMappingAccumulatingFailures() {

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        UpdateAclRequest updateAclRequest = new UpdateAclRequest(
                List.of("a_email.com", "null_email.com", "group_test"),
                new ProvisionInfo(provisionRequest.toString(), "result"));

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        GrantsAPI grantsAPIMock = mock(GrantsAPI.class);
        when(workspaceClient.grants()).thenReturn(grantsAPIMock);
        when(grantsAPIMock.get(any(SecurableType.class), anyString())).thenReturn(mock(PermissionsList.class));

        UnityCatalogManager unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

        Either<FailedOperation, ProvisioningStatus> result =
                outputPortHandler.updateAcl(provisionRequest, updateAclRequest, workspaceClient, unityCatalogManager);

        // The subject a_email.com is neither a Witboost user nor a group
        // The subject null_email.com is neither a Witboost user nor a group
        // The subject group_test is neither a Witboost user nor a group

        assert result.isLeft();
        assertEquals(3, result.getLeft().problems().size());
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .equalsIgnoreCase(
                        "java.lang.Throwable: The subject a_email.com is neither a Witboost user nor a group");
        assert result.getLeft()
                .problems()
                .get(1)
                .description()
                .equalsIgnoreCase(
                        "java.lang.Throwable: The subject null_email.com is neither a Witboost user nor a group");
        assert result.getLeft()
                .problems()
                .get(2)
                .description()
                .equalsIgnoreCase("java.lang.Throwable: The subject group_test is neither a Witboost user nor a group");
    }

    @Test
    public void updateAcl_AssigningTablePermissionsFailure() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("group_test")));

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        UpdateAclRequest updateAclRequest = new UpdateAclRequest(
                List.of("user:a_email.com", "group:group_test"),
                new ProvisionInfo(provisionRequest.toString(), "result"));

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        GrantsAPI grantsAPIMock = mock(GrantsAPI.class);

        when(workspaceClient.grants()).thenReturn(grantsAPIMock);

        when(grantsAPIMock.get(any(SecurableType.class), anyString())).thenReturn(mock(PermissionsList.class));

        when(grantsAPIMock.update(any(UpdatePermissions.class))).thenThrow(new RuntimeException("PermissionError"));

        UnityCatalogManager unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

        Either<FailedOperation, ProvisioningStatus> result =
                outputPortHandler.updateAcl(provisionRequest, updateAclRequest, workspaceClient, unityCatalogManager);

        assert result.isLeft();
        assertEquals(2, result.getLeft().problems().size());

        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains(
                        "An error occurred while adding permission SELECT for object 'catalog_op.schema_op.view' for principal a@email.com. Please try again and if the error persists contact the platform team. Details: PermissionError");
        assert result.getLeft()
                .problems()
                .get(1)
                .description()
                .contains(
                        "An error occurred while adding permission SELECT for object 'catalog_op.schema_op.view' for principal group_test. Please try again and if the error persists contact the platform team. Details: PermissionError");
    }

    @Test
    public void updateAcl_NoSelectGrantsToRemoveSuccess() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        UpdateAclRequest updateAclRequest = new UpdateAclRequest(
                List.of("user:a_email.com", "group:group_test"), new ProvisionInfo(provisionRequest.toString(), ""));

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        GrantsAPI grantsAPIMock = mock(GrantsAPI.class);
        when(workspaceClient.grants()).thenReturn(grantsAPIMock);

        // Mock the current grants on the table: lets a@email.com have SELECT grants on op.
        // Remember that the map of user:a_email.com is a@email.com
        PrivilegeAssignment privilegeAssignment = new PrivilegeAssignment().setPrincipal("a@email.com");

        UnityCatalogManager unityCatalogManagerMock = mock(UnityCatalogManager.class);

        when(unityCatalogManagerMock.retrieveDatabricksPermissions(eq(SecurableType.TABLE), any(View.class)))
                .thenReturn(Either.right(Collections.singletonList(privilegeAssignment)));

        when(unityCatalogManagerMock.assignDatabricksPermissionSelectToTableOrView(any(), any(View.class)))
                .thenReturn(Either.right(null));

        Either<FailedOperation, ProvisioningStatus> result = outputPortHandler.updateAcl(
                provisionRequest, updateAclRequest, workspaceClient, unityCatalogManagerMock);

        assert result.isRight();
        assertEquals(ProvisioningStatus.StatusEnum.COMPLETED, result.get().getStatus());
        assertEquals("Update of Acl completed!", result.get().getResult());

        // Test that the remove method is NEVER called
        verify(unityCatalogManagerMock, times(0))
                .updateDatabricksPermissions(
                        "a@email.com", Privilege.SELECT, Boolean.FALSE, new View("catalog_op", "schema_op", "view"));
    }

    @Test
    public void updateAcl_TwoGrantsToRemoveSuccess() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        UpdateAclRequest updateAclRequest = new UpdateAclRequest(
                List.of("user:a_email.com", "group:group_test"), new ProvisionInfo(provisionRequest.toString(), ""));

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        GrantsAPI grantsAPIMock = mock(GrantsAPI.class);
        when(workspaceClient.grants()).thenReturn(grantsAPIMock);

        // Mock the current grants on the table: lets a@email.com have SELECT grants on op.
        // Remember that the map of user:a_email.com is a@email.com
        PrivilegeAssignment privilegeAssignment1 = new PrivilegeAssignment().setPrincipal("b@email.com");
        PrivilegeAssignment privilegeAssignment2 = new PrivilegeAssignment().setPrincipal("c@email.com");

        ArrayList<PrivilegeAssignment> privilegeAssignmentCollection = new ArrayList<>();
        privilegeAssignmentCollection.add(privilegeAssignment1);
        privilegeAssignmentCollection.add(privilegeAssignment2);

        UnityCatalogManager unityCatalogManagerMock = mock(UnityCatalogManager.class);

        when(unityCatalogManagerMock.retrieveDatabricksPermissions(eq(SecurableType.TABLE), any(View.class)))
                .thenReturn(Either.right(privilegeAssignmentCollection));

        when(unityCatalogManagerMock.assignDatabricksPermissionSelectToTableOrView(any(), any(View.class)))
                .thenReturn(Either.right(null));

        when(unityCatalogManagerMock.updateDatabricksPermissions(
                        eq("b@email.com"), eq(Privilege.SELECT), eq(Boolean.FALSE), any(View.class)))
                .thenReturn(Either.right(null));
        when(unityCatalogManagerMock.updateDatabricksPermissions(
                        eq("c@email.com"), eq(Privilege.SELECT), eq(Boolean.FALSE), any(View.class)))
                .thenReturn(Either.right(null));

        Either<FailedOperation, ProvisioningStatus> result = outputPortHandler.updateAcl(
                provisionRequest, updateAclRequest, workspaceClient, unityCatalogManagerMock);

        assert result.isRight();
        assertEquals(ProvisioningStatus.StatusEnum.COMPLETED, result.get().getStatus());
        assertEquals("Update of Acl completed!", result.get().getResult());

        // Test that the remove method is called two times

        verify(unityCatalogManagerMock, times(1))
                .updateDatabricksPermissions(
                        eq("b@email.com"), eq(Privilege.SELECT), eq(Boolean.FALSE), any(View.class));
        verify(unityCatalogManagerMock, times(1))
                .updateDatabricksPermissions(
                        eq("c@email.com"), eq(Privilege.SELECT), eq(Boolean.FALSE), any(View.class));
    }

    @Test
    public void updateAcl_currentPrivilegeAssignmentsNull() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        UpdateAclRequest updateAclRequest = new UpdateAclRequest(
                List.of("user:a_email.com", "group:group_test"),
                new ProvisionInfo(provisionRequest.toString(), "result"));

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        GrantsAPI grantsAPIMock = mock(GrantsAPI.class);
        when(workspaceClient.grants()).thenReturn(grantsAPIMock);

        PermissionsList permissionsListMock = mock(PermissionsList.class);

        // The currentPrivilegeAssignement is null.
        when(permissionsListMock.getPrivilegeAssignments()).thenReturn(null);

        when(grantsAPIMock.get(any(SecurableType.class), anyString())).thenReturn(permissionsListMock);

        UnityCatalogManager unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

        Either<FailedOperation, ProvisioningStatus> result =
                outputPortHandler.updateAcl(provisionRequest, updateAclRequest, workspaceClient, unityCatalogManager);

        assert result.isRight();
        assertEquals(ProvisioningStatus.StatusEnum.COMPLETED, result.get().getStatus());
        assertEquals("Update of Acl completed!", result.get().getResult());
    }

    @Test
    public void updateAcl_DpOwnerPermissionsAreNotRemoved_Development() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        UpdateAclRequest updateAclRequest = new UpdateAclRequest(
                List.of("user:random@email.com"), new ProvisionInfo(provisionRequest.toString(), ""));

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        GrantsAPI grantsAPIMock = mock(GrantsAPI.class);
        when(workspaceClient.grants()).thenReturn(grantsAPIMock);

        // Mock the current grants on the table: lets a@email.com and the DataProductOwner have SELECT grants on op.
        // Remember that the map of user:a_email.com is a@email.com
        // We expect that grants for the DpOwner NOT TO BE REMOVED, as we are in dev environment
        ArrayList<PrivilegeAssignment> privilegeAssignmentCollection = new ArrayList<>();
        privilegeAssignmentCollection.add(new PrivilegeAssignment().setPrincipal("dp.owner@email.com")); // ok
        privilegeAssignmentCollection.add(new PrivilegeAssignment().setPrincipal("c@email.com")); // must be revoked

        UnityCatalogManager unityCatalogManagerMock = mock(UnityCatalogManager.class);

        when(unityCatalogManagerMock.retrieveDatabricksPermissions(eq(SecurableType.TABLE), any(View.class)))
                .thenReturn(Either.right(privilegeAssignmentCollection));

        when(unityCatalogManagerMock.updateDatabricksPermissions(
                        eq("c@email.com"), eq(Privilege.SELECT), eq(Boolean.FALSE), any(View.class)))
                .thenReturn(Either.right(null));

        when(unityCatalogManagerMock.assignDatabricksPermissionSelectToTableOrView(any(), any(View.class)))
                .thenReturn(Either.right(null));

        Either<FailedOperation, ProvisioningStatus> result = outputPortHandler.updateAcl(
                provisionRequest, updateAclRequest, workspaceClient, unityCatalogManagerMock);

        assert result.isRight();
        assertEquals(ProvisioningStatus.StatusEnum.COMPLETED, result.get().getStatus());
        assertEquals("Update of Acl completed!", result.get().getResult());

        // Test that the remove method is called just 1 time
        verify(unityCatalogManagerMock, times(1))
                .updateDatabricksPermissions(
                        eq("c@email.com"), eq(Privilege.SELECT), eq(Boolean.FALSE), any(View.class));

        verify(unityCatalogManagerMock, times(0))
                .updateDatabricksPermissions(
                        eq("random@email.com"), eq(Privilege.SELECT), eq(Boolean.FALSE), any(View.class));
    }

    @Test
    public void updateAcl_DpOwnerPermissionsAreRemoved_QA() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        dataProduct.setEnvironment("QA");

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        UpdateAclRequest updateAclRequest = new UpdateAclRequest(
                List.of("user:random@email.com"), new ProvisionInfo(provisionRequest.toString(), ""));

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);

        GrantsAPI grantsAPIMock = mock(GrantsAPI.class);
        when(workspaceClient.grants()).thenReturn(grantsAPIMock);

        // Mock the current grants on the table: lets a@email.com and the DataProductOwner have SELECT grants on op.
        // Remember that the map of user:a_email.com is a@email.com
        // We expect that grants for the DpOwner TO BE REMOVED, as we are NOT in dev environment
        ArrayList<PrivilegeAssignment> privilegeAssignmentCollection = new ArrayList<>();
        privilegeAssignmentCollection.add(
                new PrivilegeAssignment().setPrincipal("dp.owner@email.com")); // must be revoked
        privilegeAssignmentCollection.add(new PrivilegeAssignment().setPrincipal("a@email.com")); // must be revoked

        UnityCatalogManager unityCatalogManagerMock = mock(UnityCatalogManager.class);

        when(unityCatalogManagerMock.retrieveDatabricksPermissions(eq(SecurableType.TABLE), any(View.class)))
                .thenReturn(Either.right(privilegeAssignmentCollection));

        when(unityCatalogManagerMock.updateDatabricksPermissions(
                        eq("a@email.com"), eq(Privilege.SELECT), eq(Boolean.FALSE), any(View.class)))
                .thenReturn(Either.right(null));

        when(unityCatalogManagerMock.updateDatabricksPermissions(
                        eq("dp.owner@email.com"), eq(Privilege.SELECT), eq(Boolean.FALSE), any(View.class)))
                .thenReturn(Either.right(null));

        when(unityCatalogManagerMock.assignDatabricksPermissionSelectToTableOrView(any(), any(View.class)))
                .thenReturn(Either.right(null));

        Either<FailedOperation, ProvisioningStatus> result = outputPortHandler.updateAcl(
                provisionRequest, updateAclRequest, workspaceClient, unityCatalogManagerMock);

        assert result.isRight();
        assertEquals(ProvisioningStatus.StatusEnum.COMPLETED, result.get().getStatus());
        assertEquals("Update of Acl completed!", result.get().getResult());

        // Test that the remove method is called 2 times, 1 for a@email.com and the other for dp.owner@email.com
        verify(unityCatalogManagerMock, times(1))
                .updateDatabricksPermissions(
                        eq("a@email.com"), eq(Privilege.SELECT), eq(Boolean.FALSE), any(View.class));

        verify(unityCatalogManagerMock, times(1))
                .updateDatabricksPermissions(
                        eq("dp.owner@email.com"), eq(Privilege.SELECT), eq(Boolean.FALSE), any(View.class));
    }

    private ProvisionRequest<DatabricksOutputPortSpecific> createOPProvisionRequestEmptySchema() {
        databricksOutputPortSpecific.setWorkspace("ws");
        databricksOutputPortSpecific.setMetastore("metastore");
        databricksOutputPortSpecific.setCatalogName("catalog");
        databricksOutputPortSpecific.setSchemaName("schema");
        databricksOutputPortSpecific.setTableName("t");
        databricksOutputPortSpecific.setSqlWarehouseName("sql_wh");
        databricksOutputPortSpecific.setWorkspaceOP("ws_op");
        databricksOutputPortSpecific.setCatalogNameOP("catalog_op");
        databricksOutputPortSpecific.setSchemaNameOP("schema_op");
        databricksOutputPortSpecific.setViewNameOP("view");

        outputPort.setSpecific(databricksOutputPortSpecific);

        // Creating an empty schema
        List<Column> emptyDataColumnList = new ArrayList<>();
        DataContract dataContract = new DataContract();
        dataContract.setSchema(emptyDataColumnList);

        outputPort.setDataContract(dataContract);

        return new ProvisionRequest<>(dataProduct, outputPort, false);
    }

    private ProvisionRequest<DatabricksOutputPortSpecific> createOPProvisionRequest() {
        databricksOutputPortSpecific.setWorkspace("ws");
        databricksOutputPortSpecific.setMetastore("metastore");
        databricksOutputPortSpecific.setCatalogName("catalog");
        databricksOutputPortSpecific.setSchemaName("schema");
        databricksOutputPortSpecific.setTableName("t");
        databricksOutputPortSpecific.setSqlWarehouseName("sql_wh");
        databricksOutputPortSpecific.setWorkspaceOP("ws_op");
        databricksOutputPortSpecific.setCatalogNameOP("catalog_op");
        databricksOutputPortSpecific.setSchemaNameOP("schema_op");
        databricksOutputPortSpecific.setViewNameOP("view");

        outputPort.setSpecific(databricksOutputPortSpecific);

        // Creating schema
        Column dataColumn1 = new Column();
        dataColumn1.setName("col_1");
        dataColumn1.setDataType("TEXT");

        Column dataColumn2 = new Column();
        dataColumn2.setName("col_2");
        dataColumn2.setDataType("TEXT");

        DataContract dataContract = new DataContract();
        dataContract.setSchema(List.of(dataColumn1, dataColumn2));

        outputPort.setDataContract(dataContract);

        return new ProvisionRequest<>(dataProduct, outputPort, false);
    }

    @Test
    public void executeQuery_Success() {
        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);
        StatementExecutionAPI statementExecutionAPIMock = mock(StatementExecutionAPI.class);

        String expectedStatementId = "test_statement_id";
        String testQuery = "SELECT * FROM test_table;";
        String testCatalog = "test_catalog";
        String testSchema = "test_schema";
        String testWarehouseId = "test_warehouse_id";

        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog(testCatalog)
                .setSchema(testSchema)
                .setStatement(testQuery)
                .setWarehouseId(testWarehouseId);

        StatementResponse statementResponseMock = new StatementResponse().setStatementId(expectedStatementId);
        when(workspaceClientMock.statementExecution()).thenReturn(statementExecutionAPIMock);
        when(statementExecutionAPIMock.executeStatement(eq(request))).thenReturn(statementResponseMock);

        Either<FailedOperation, String> result = outputPortHandler.executeQuery(
                testQuery, testCatalog, testSchema, testWarehouseId, workspaceClientMock);

        assertTrue(result.isRight());
        assertEquals(expectedStatementId, result.get());
    }

    @Test
    public void executeStatementAlterViewSetDescription_Success() {
        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);
        StatementExecutionAPI statementExecutionAPIMock = mock(StatementExecutionAPI.class);

        String testCatalog = "test_catalog";
        String testSchema = "test_schema";
        String testWarehouseId = "test_warehouse_id";
        String testViewName = "test_view";
        String viewDescription = "Test view description";
        String expectedStatementId = "test_statement_id";

        String expectedQuery = "ALTER VIEW test_view SET TBLPROPERTIES ('comment' = \"Test view description\")";

        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog(testCatalog)
                .setSchema(testSchema)
                .setStatement(expectedQuery)
                .setWarehouseId(testWarehouseId);

        StatementResponse statementResponseMock = new StatementResponse().setStatementId(expectedStatementId);

        when(workspaceClientMock.statementExecution()).thenReturn(statementExecutionAPIMock);
        when(statementExecutionAPIMock.executeStatement(eq(request))).thenReturn(statementResponseMock);

        Either<FailedOperation, Optional<String>> result = outputPortHandler.executeStatementAlterViewSetDescription(
                testCatalog, testSchema, testViewName, viewDescription, testWarehouseId, workspaceClientMock);

        assertTrue(result.isRight());
        assertTrue(result.get().isPresent());
        assertEquals(expectedStatementId, result.get().get());
    }

    @Test
    public void executeStatementAlterViewSetDescription_NoDescription() {
        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);

        String testCatalog = "test_catalog";
        String testSchema = "test_schema";
        String testWarehouseId = "test_warehouse_id";
        String testViewName = "test_view";

        Either<FailedOperation, Optional<String>> result = outputPortHandler.executeStatementAlterViewSetDescription(
                testCatalog, testSchema, testViewName, null, testWarehouseId, workspaceClientMock);

        assertTrue(result.isRight());
        assertTrue(result.get().isEmpty());
    }

    @Test
    public void executeStatementAlterViewSetDescription_Failure() {
        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);
        StatementExecutionAPI statementExecutionAPIMock = mock(StatementExecutionAPI.class);

        String testCatalog = "test_catalog";
        String testSchema = "test_schema";
        String testWarehouseId = "test_warehouse_id";
        String testViewName = "test_view";
        String viewDescription = "Test view description";

        String expectedQuery = "ALTER VIEW test_view SET TBLPROPERTIES ('comment' = \"Test view description\")";

        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog(testCatalog)
                .setSchema(testSchema)
                .setStatement(expectedQuery)
                .setWarehouseId(testWarehouseId);

        when(workspaceClientMock.statementExecution()).thenReturn(statementExecutionAPIMock);
        when(statementExecutionAPIMock.executeStatement(eq(request)))
                .thenThrow(new RuntimeException("Execution error"));

        Either<FailedOperation, Optional<String>> result = outputPortHandler.executeStatementAlterViewSetDescription(
                testCatalog, testSchema, testViewName, viewDescription, testWarehouseId, workspaceClientMock);

        assertTrue(result.isLeft());
        assertTrue(
                result.getLeft()
                        .problems()
                        .get(0)
                        .description()
                        .contains(
                                "An error occurred while running query 'ALTER VIEW test_view SET TBLPROPERTIES ('comment' = \"Test view description\")'."));
        assertTrue(result.getLeft().problems().get(0).description().contains("Execution error"));
    }

    @Test
    public void executeStatementCommentOnColumn_Success() {
        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);
        StatementExecutionAPI statementExecutionAPIMock = mock(StatementExecutionAPI.class);

        String testCatalog = "test_catalog";
        String testSchema = "test_schema";
        String testWarehouseId = "test_warehouse_id";
        String testViewName = "test_view";
        String expectedStatementId = "test_statement_id";

        Column testColumn = new Column();
        testColumn.setName("col1");
        testColumn.setDescription("Test description");

        String expectedQuery = "COMMENT ON COLUMN test_view.col1 IS \"Test description\"";

        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog(testCatalog)
                .setSchema(testSchema)
                .setStatement(expectedQuery)
                .setWarehouseId(testWarehouseId);

        StatementResponse statementResponseMock = new StatementResponse().setStatementId(expectedStatementId);

        when(workspaceClientMock.statementExecution()).thenReturn(statementExecutionAPIMock);
        when(statementExecutionAPIMock.executeStatement(eq(request))).thenReturn(statementResponseMock);

        Either<FailedOperation, Optional<String>> result = outputPortHandler.executeStatementCommentOnColumn(
                testCatalog, testSchema, testViewName, testColumn, testWarehouseId, workspaceClientMock);

        assertTrue(result.isRight());
        assertTrue(result.get().isPresent());
        assertEquals(expectedStatementId, result.get().get());
    }

    @Test
    public void executeStatementCommentOnColumn_NoComment() {
        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);

        String testCatalog = "test_catalog";
        String testSchema = "test_schema";
        String testWarehouseId = "test_warehouse_id";
        String testViewName = "test_view";

        Column testColumn = new Column();
        testColumn.setName("col1");

        Either<FailedOperation, Optional<String>> result = outputPortHandler.executeStatementCommentOnColumn(
                testCatalog, testSchema, testViewName, testColumn, testWarehouseId, workspaceClientMock);

        assertTrue(result.isRight());
        assertTrue(result.get().isEmpty());
    }

    @Test
    public void executeStatementCommentOnColumn_Failure() {
        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);

        StatementExecutionAPI statementExecutionAPIMock = mock(StatementExecutionAPI.class);

        String testCatalog = "test_catalog";
        String testSchema = "test_schema";
        String testWarehouseId = "test_warehouse_id";
        String testViewName = "test_view";

        Column testColumn = new Column();
        testColumn.setName("col1");
        testColumn.setDescription("Test description");

        String expectedQuery = "COMMENT ON COLUMN test_view.col1 IS \"Test description\"";

        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog(testCatalog)
                .setSchema(testSchema)
                .setStatement(expectedQuery)
                .setWarehouseId(testWarehouseId);

        when(workspaceClientMock.statementExecution()).thenReturn(statementExecutionAPIMock);
        when(statementExecutionAPIMock.executeStatement(eq(request)))
                .thenThrow(new RuntimeException("Execution error"));

        Either<FailedOperation, Optional<String>> result = outputPortHandler.executeStatementCommentOnColumn(
                testCatalog, testSchema, testViewName, testColumn, testWarehouseId, workspaceClientMock);

        assertTrue(result.isLeft());
        assertTrue(
                result.getLeft()
                        .problems()
                        .get(0)
                        .description()
                        .contains(
                                "An error occurred while running query 'COMMENT ON COLUMN test_view.col1 IS \"Test description\"'."));
        assertTrue(result.getLeft().problems().get(0).description().contains("Execution error"));
    }

    @Test
    public void executeQuery_Failure() {
        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);
        StatementExecutionAPI statementExecutionAPIMock = mock(StatementExecutionAPI.class);

        String testQuery = "SELECT * FROM test_table;";
        String testCatalog = "test_catalog";
        String testSchema = "test_schema";
        String testWarehouseId = "test_warehouse_id";

        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog(testCatalog)
                .setSchema(testSchema)
                .setStatement(testQuery)
                .setWarehouseId(testWarehouseId);

        when(workspaceClientMock.statementExecution()).thenReturn(statementExecutionAPIMock);
        when(statementExecutionAPIMock.executeStatement(eq(request)))
                .thenThrow(new RuntimeException("Execution error"));

        Either<FailedOperation, String> result = outputPortHandler.executeQuery(
                testQuery, testCatalog, testSchema, testWarehouseId, workspaceClientMock);

        assertTrue(result.isLeft());
        assert result.getLeft().problems().get(0).description().contains("An error occurred while running query");
    }
}
