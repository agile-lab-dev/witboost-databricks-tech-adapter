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
}
