package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

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
import com.witboost.provisioning.model.Column;
import com.witboost.provisioning.model.DataContract;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.params.ApiClientConfigParams;
import it.agilelab.witboost.provisioning.databricks.bean.params.WorkspaceClientConfigParams;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
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
import it.agilelab.witboost.provisioning.databricks.model.databricks.object.*;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisionInfo;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.openapi.model.UpdateAclRequest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
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
    private Function<ApiClientConfigParams, ApiClient> apiClientFactory;

    @Mock
    ApiClient apiClientMock;

    @MockBean
    private Function<WorkspaceClientConfigParams, WorkspaceClient> workspaceClientFactory;

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
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        when(apiClientFactory.apply(any(ApiClientConfigParams.class))).thenReturn(apiClientMock);

        List<DataSource> dataSourceList =
                Arrays.asList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

        when(new DataSourcesAPI(apiClientMock).list()).thenReturn(dataSourceList);

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
    public void provisionOutputPort_SuccessWithEmptySchema() {
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
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        when(apiClientFactory.apply(any(ApiClientConfigParams.class))).thenReturn(apiClientMock);

        List<DataSource> dataSourceList =
                Arrays.asList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

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
    public void provisionOutputPort_Exception() {
        OutputPort op = new OutputPort();
        op.setName("op");
        ProvisionRequest provisionRequest = new ProvisionRequest<>(dataProduct, op, false);
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
    public void provisionOutputPort_ExecuteStatementCreateOutputPortFailure() {
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
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        when(apiClientFactory.apply(any(ApiClientConfigParams.class))).thenReturn(apiClientMock);

        List<DataSource> dataSourceList =
                Arrays.asList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

        when(new DataSourcesAPI(apiClientMock).list()).thenReturn(dataSourceList);

        // Mock classes and methods inside createOrReplaceOutputPortView
        ExecuteStatementRequest request = new ExecuteStatementRequest()
                .setCatalog("catalog_op")
                .setSchema("schema_op")
                .setStatement("CREATE OR REPLACE VIEW `view` AS SELECT col_1,col_2 FROM `catalog`.`schema`.`t`;")
                .setWarehouseId("sql_wh_id");

        StatementResponse statementResponseMock = mock(StatementResponse.class);
        when(statementResponseMock.getStatementId()).thenReturn("statement_id");
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
        MetastoresAPI metastoresAPIMock = mock(MetastoresAPI.class);

        Iterable<MetastoreInfo> iterableMetastoresList =
                Arrays.asList(new MetastoreInfo().setName("metastore").setMetastoreId("id"));

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
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        when(apiClientFactory.apply(any(ApiClientConfigParams.class))).thenReturn(apiClientMock);

        List<DataSource> dataSourceList =
                Arrays.asList(new DataSource().setName("sql_wh").setWarehouseId("sql_wh_id"));

        when(new DataSourcesAPI(apiClientMock).list()).thenReturn(dataSourceList);

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

        // poll returns CANCELED state
        when(statementStatusMockPoll.getState()).thenReturn(StatementState.CANCELED);

        when(getstatementResponseMockPoll.getStatus()).thenReturn(statementStatusMockPoll);

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
        List<SchemaInfo> schemaInfoList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog_op").setName("schema_op"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog_op")).thenReturn(schemaInfoList);

        // Mock the search of sqlWareHouseId
        when(apiClientFactory.apply(any(ApiClientConfigParams.class))).thenReturn(apiClientMock);

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

    @Test
    public void updateAcl_Success() {

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
        assertEquals(result.get().getStatus(), ProvisioningStatus.StatusEnum.COMPLETED);
        assertEquals(result.get().getResult(), "Update of Acl completed!");
    }

    @Test
    public void updateAcl_DatabricksMappingFailure() {

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
                .equalsIgnoreCase(
                        "An error occurred while adding permission SELECT for object 'catalog_op.schema_op.view' for principal a@email.com. Please try again and if the error persists contact the platform team. Details: PermissionError");
        assert result.getLeft()
                .problems()
                .get(1)
                .description()
                .equalsIgnoreCase(
                        "An error occurred while adding permission SELECT for object 'catalog_op.schema_op.view' for principal group_test. Please try again and if the error persists contact the platform team. Details: PermissionError");
    }

    @Test
    public void updateAcl_NoSelectGrantsToRemoveSuccess() {

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
        assertEquals(result.get().getStatus(), ProvisioningStatus.StatusEnum.COMPLETED);
        assertEquals(result.get().getResult(), "Update of Acl completed!");

        // Test that the remove method is NEVER called
        verify(unityCatalogManagerMock, times(0))
                .updateDatabricksPermissions(
                        "a@email.com", Privilege.SELECT, Boolean.FALSE, new View("catalog_op", "schema_op", "view"));
    }

    @Test
    public void updateAcl_TwoGrantsToRemoveSuccess() {

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
        assertEquals(result.get().getStatus(), ProvisioningStatus.StatusEnum.COMPLETED);
        assertEquals(result.get().getResult(), "Update of Acl completed!");

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
        assertEquals(result.get().getStatus(), ProvisioningStatus.StatusEnum.COMPLETED);
        assertEquals(result.get().getResult(), "Update of Acl completed!");
    }

    @Test
    public void updateAcl_DpOwnerPermissionsAreNotRemoved_Development() {

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
        assertEquals(result.get().getStatus(), ProvisioningStatus.StatusEnum.COMPLETED);
        assertEquals(result.get().getResult(), "Update of Acl completed!");

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
        assertEquals(result.get().getStatus(), ProvisioningStatus.StatusEnum.COMPLETED);
        assertEquals(result.get().getResult(), "Update of Acl completed!");

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

        // Creating empty schema
        List<Column> emptyDataColumnList = new ArrayList<>();
        DataContract dataContract = new DataContract();
        dataContract.setSchema(emptyDataColumnList);

        outputPort.setDataContract(dataContract);

        ProvisionRequest provisionRequest = new ProvisionRequest<>(dataProduct, outputPort, false);
        return provisionRequest;
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

        ProvisionRequest provisionRequest = new ProvisionRequest<>(dataProduct, outputPort, false);
        return provisionRequest;
    }
}
