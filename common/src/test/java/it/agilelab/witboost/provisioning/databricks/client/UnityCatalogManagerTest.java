package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.catalog.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
@ExtendWith(MockitoExtension.class)
public class UnityCatalogManagerTest {

    @Mock
    private WorkspaceClient workspaceClient;

    private UnityCatalogManager unityCatalogManager;

    private DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(
            "workspace", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);

    @BeforeEach
    public void setUp() {
        unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);
    }

    @Test
    public void testAttachMetastore() {
        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        Either<FailedOperation, Void> result = unityCatalogManager.attachMetastore("metastore");
        assertTrue(result.isRight());
        assertEquals(null, result.get());
    }

    @Test
    public void testCreateCatalog() {
        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;

        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog1"), new CatalogInfo().setName("catalog2"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(catalogList);

        Either<FailedOperation, Void> result = unityCatalogManager.createCatalogIfNotExists("new");
        assertTrue(result.isRight());
        assertEquals(null, result.get());
    }

    @Test
    public void testAttachMetastore_Exception() {
        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;

        Either<FailedOperation, Void> result = unityCatalogManager.attachMetastore("metastore");
        assertTrue(result.isLeft());
        assertEquals(
                "Error linking the workspace workspace to the metastore metastore",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testCreateCatalog_ExceptionSearchingCatalog() {
        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;

        Either<FailedOperation, Void> result = unityCatalogManager.createCatalogIfNotExists("new");
        assertTrue(result.isLeft());
        String expectedError =
                "An error occurred trying to search the catalog new. Please try again and if the error persists contact the platform team. Details: Cannot invoke \"com.databricks.sdk.service.catalog.CatalogsAPI.";
        assertTrue(result.getLeft().problems().get(0).description().contains(expectedError));
    }

    @Test
    public void testAttachMetastore_MetastoreNotFound() {
        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore1").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        Either<FailedOperation, Void> result = unityCatalogManager.attachMetastore("metastore");
        assertTrue(result.isLeft());
        String expectedError =
                "An error occurred while searching metastore 'metastore' details. Please try again and if the error persists contact the platform team. Details: Metastore not found";
        assertEquals(expectedError, result.getLeft().problems().get(0).description());
    }

    @Test
    public void testCreateCatalog_ExceptionCreatingCatalog() {
        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().create(anyString()))
                .thenThrow(new DatabricksException("Exception creating catalog"));

        Either<FailedOperation, Void> result = unityCatalogManager.createCatalogIfNotExists("new");
        assertTrue(result.isLeft());
        String expectedError =
                "An error occurred while creating unity catalog 'new'. Please try again and if the error persists contact the platform team. Details: Exception creating catalog";
        assertTrue(result.getLeft().problems().get(0).description().contains(expectedError));
    }

    @Test
    public void testCreateSchemaIfNotExists_UnexistingCatalogFailure() {

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(List.of()); // No catalogs in the ws

        Either<FailedOperation, Void> actualRes1 =
                unityCatalogManager.createSchemaIfNotExists("catalog_not_existing", "schema");

        assertTrue(actualRes1.isLeft());
        String expectedError =
                "An error occurred trying to search the schema 'schema' in catalog 'catalog_not_existing': catalog 'catalog_not_existing' does not exist!";
        assertEquals(actualRes1.getLeft().problems().get(0).description(), expectedError);
    }

    @Test
    public void testCreateSchemaIfNotExists_SchemaAlreadyExistsSuccess() {

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(List.of(new CatalogInfo().setName("catalog")));
        List<SchemaInfo> schemaList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog").setName("schema"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog")).thenReturn(schemaList);

        Either<FailedOperation, Void> actualRes = unityCatalogManager.createSchemaIfNotExists("catalog", "schema");

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testCreateSchemaIfNotExists_SchemaDoesNotExistSuccess() {

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(List.of(new CatalogInfo().setName("catalog")));

        List<SchemaInfo> schemaList =
                Arrays.asList(new SchemaInfo().setCatalogName("catalog").setName("schema"));

        when(workspaceClient.schemas()).thenReturn(mock(SchemasAPI.class));
        when(workspaceClient.schemas().list("catalog")).thenReturn(schemaList);

        Either<FailedOperation, Void> actualRes =
                unityCatalogManager.createSchemaIfNotExists("catalog", "schema_not_existing");

        assertTrue(actualRes.isRight());
    }

    @Test
    public void testCreateCatalogIfNotExists_GenericFailure() {

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        // when(workspaceClient.catalogs().list(any())).thenReturn(List.of(new CatalogInfo().setName("catalog")));

        when(workspaceClient.catalogs().create(anyString()))
                .thenThrow(new DatabricksException("Exception creating catalog"));

        Either<FailedOperation, Void> result = unityCatalogManager.createCatalogIfNotExists("new");
        assertTrue(result.isLeft());
        String expectedError =
                "An error occurred while creating unity catalog 'new'. Please try again and if the error persists contact the platform team. Details: Exception creating catalog";
        assertTrue(result.getLeft().problems().get(0).description().contains(expectedError));
    }

    @Test
    public void testCheckTableExistence_GenericFailure() {

        when(workspaceClient.tables()).thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, Boolean> result = unityCatalogManager.checkTableExistence("catalog", "schema", "table");
        assertTrue(result.isLeft());

        String messageError =
                "An error occurred while searching table catalog.schema.table. Please try again and if the error persists contact the platform team. Details: Exception";

        assertEquals(messageError, result.getLeft().problems().get(0).description());
    }

    @Test
    public void testDropTableIfExists_FailureWhileDropping() {

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        TablesAPI tablesAPIMock = mock(TablesAPI.class);
        when(workspaceClient.tables()).thenReturn(tablesAPIMock);
        when(tablesAPIMock.exists("catalog.schema.table")).thenReturn(tableExistsResponseMock);

        doThrow(new RuntimeException("Exception")).when(tablesAPIMock).delete("catalog.schema.table");

        Either<FailedOperation, Void> result = unityCatalogManager.dropTableIfExists("catalog", "schema", "table");

        assertTrue(result.isLeft());
        String messageError =
                "An error occurred while dropping table 'catalog.schema.table'. Please try again and if the error persists contact the platform team. Details: Exception";
        assertEquals(messageError, result.getLeft().problems().get(0).description());
    }

    @Test
    public void testDropTableIfExists_SuccessTableDoesNotExist() {
        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(false); // table does not exist.

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().exists("catalog.schema.table")).thenReturn(tableExistsResponseMock);

        Either<FailedOperation, Void> result = unityCatalogManager.dropTableIfExists("catalog", "schema", "table");

        assertTrue(result.isRight());
    }

    @Test
    public void testRetrieveColumnNames_GenericFailure() {

        when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClient.tables().get("catalog.schema.table")).thenThrow(new RuntimeException("Exception"));
        Either<FailedOperation, List<String>> result =
                unityCatalogManager.retrieveTableColumnsNames("catalog", "schema", "table");

        assertTrue(result.isLeft());
        String messageError =
                "An error occurred while retrieving columns for table 'catalog.schema.table' in workspace workspace. Please try again and if the error persists contact the platform team. Details: Exception";
        assertEquals(messageError, result.getLeft().problems().get(0).description());
    }
}
