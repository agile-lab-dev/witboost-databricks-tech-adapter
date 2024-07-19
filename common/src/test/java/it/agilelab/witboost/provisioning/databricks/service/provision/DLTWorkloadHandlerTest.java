package it.agilelab.witboost.provisioning.databricks.service.provision;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.AzureResourceManager;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CatalogsAPI;
import com.databricks.sdk.service.catalog.MetastoreInfo;
import com.databricks.sdk.service.catalog.MetastoresAPI;
import com.databricks.sdk.service.pipelines.CreatePipelineResponse;
import com.databricks.sdk.service.pipelines.PipelineClusterAutoscaleMode;
import com.databricks.sdk.service.pipelines.PipelineStateInfo;
import com.databricks.sdk.service.pipelines.PipelinesAPI;
import com.databricks.sdk.service.workspace.CreateRepo;
import com.databricks.sdk.service.workspace.RepoInfo;
import com.databricks.sdk.service.workspace.ReposAPI;
import com.databricks.sdk.service.workspace.WorkspaceAPI;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.DatabricksWorkspaceClientBean;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DLTClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.PipelineChannel;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.ProductEdition;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.*;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;

@SpringBootTest
@EnableConfigurationProperties
public class DLTWorkloadHandlerTest {

    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @Autowired
    private JobWorkloadHandler jobWorkloadHandler;

    @Mock
    WorkspaceClient workspaceClient;

    @Mock
    WorkspaceAPI workspaceAPI;

    @MockBean
    private DatabricksWorkspaceClientBean databricksWorkspaceClientBean;

    @MockBean
    private AzureResourceManager azureResourceManager;

    @Autowired
    private DLTWorkloadHandler dltWorkloadHandler;

    private DataProduct dataProduct;
    private Workload workload;
    private DatabricksDLTWorkloadSpecific databricksDLTWorkloadSpecific;

    private DatabricksWorkspaceInfo workspaceInfo =
            new DatabricksWorkspaceInfo("workspace", "123", "https://example.com", "abc", "test");
    private String workspaceName = "testWorkspace";

    @Test
    public void testAzurePermissionsConfigInitialization() {
        assertNotNull(azurePermissionsConfig);
        assertNotNull(azurePermissionsConfig.getSubscriptionId());
        assertNotNull(azurePermissionsConfig.getResourceGroup());
    }

    @TestConfiguration
    static class TestConfig {
        @Bean
        public AzurePermissionsConfig azurePermissionsConfig() {
            return new AzurePermissionsConfig();
        }
    }

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        dataProduct = new DataProduct();
        workload = new Workload();

        DLTClusterSpecific cluster = new DLTClusterSpecific();
        cluster.setMode(PipelineClusterAutoscaleMode.LEGACY);
        cluster.setWorkerType("Standard_DS3_v2");
        cluster.setDriverType("Standard_DS3_v2");
        cluster.setPolicyId("policyId");

        DatabricksDLTWorkloadSpecific specific = new DatabricksDLTWorkloadSpecific();
        specific.setWorkspace("workspace");
        specific.setPipelineName("pipelineName");
        specific.setProductEdition(ProductEdition.CORE);
        specific.setContinuous(true);
        specific.setNotebooks(List.of("notebook1", "notebook2"));
        specific.setFiles(List.of("file1", "file2"));
        specific.setCatalog("catalog");
        specific.setTarget("target");
        specific.setPhoton(true);
        specific.setNotificationsMails(List.of("email1@example.com", "email2@example.com"));
        specific.setNotificationsAlerts(List.of("alert1", "alert2"));
        specific.setChannel(PipelineChannel.CURRENT);
        specific.setCluster(cluster);
        specific.setMetastore("metastore");

        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitReference("main");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific.setGitPath("/src");
        gitSpecific.setGitRepoUrl("https://github.com/repo.git");
        specific.setGit(gitSpecific);

        workload.setSpecific(specific);

        dataProduct.setDataProductOwner("user:name.surname@company.it");

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
    }

    @Test
    public void provisionWorkload_Success() {

        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));
        Iterable<CatalogInfo> iterableCatalogList = catalogList;

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(
                new PipelineStateInfo().setPipelineId("pipe1"), new PipelineStateInfo().setPipelineId("pipe2"));
        Iterable<PipelineStateInfo> pipelineStateInfoIterable = pipelineStateInfos;

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.create(any(CreateRepo.class))).thenReturn(repoInfo);

        CreatePipelineResponse createPipelineResponse = mock(CreatePipelineResponse.class);
        when(createPipelineResponse.getPipelineId()).thenReturn("123");
        when(workspaceClient.pipelines().create(any())).thenReturn(createPipelineResponse);

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, String> result =
                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        assertEquals("123", result.get());
    }

    @Test
    public void provisionWorkload_ErrorCreatingRepo() {

        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));
        Iterable<CatalogInfo> iterableCatalogList = catalogList;

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(
                new PipelineStateInfo().setPipelineId("pipe1"), new PipelineStateInfo().setPipelineId("pipe2"));
        Iterable<PipelineStateInfo> pipelineStateInfoIterable = pipelineStateInfos;

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, String> result =
                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        System.out.println(result);
        assert result.isLeft();

        String errorMessage =
                "Cannot invoke \"com.databricks.sdk.service.workspace.ReposAPI.create(com.databricks.sdk.service.workspace.CreateRepo)";
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void provisionWorkload_ErrorCreatingPipeline() {

        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));
        Iterable<CatalogInfo> iterableCatalogList = catalogList;

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(
                new PipelineStateInfo().setPipelineId("pipe1"), new PipelineStateInfo().setPipelineId("pipe2"));
        Iterable<PipelineStateInfo> pipelineStateInfoIterable = pipelineStateInfos;

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.create(any(CreateRepo.class))).thenReturn(repoInfo);

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, String> result =
                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        String expectedError =
                "An error occurred while creating the DLT Pipeline pipelineName. Please try again and if the error persists contact the platform team. Details: Cannot invoke \"com.databricks.sdk.service.pipelines.CreatePipelineResponse";
        assertTrue(result.getLeft().problems().get(0).description().contains(expectedError));
    }

    @Test
    public void provisionWorkload_Exception() {
        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, new Workload(), false);
        try {
            dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);
        } catch (Exception e) {
            assertEquals(e.getClass(), NullPointerException.class);
        }
    }

    @Test
    public void unprovisionWorkload_Success() {

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, true);

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo(workspaceName, "test", "test", "test", "test");

        Optional<DatabricksWorkspaceInfo> optionalDatabricksWorkspaceInfo = Optional.of(databricksWorkspaceInfo);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(
                new PipelineStateInfo().setPipelineId("pipe1"), new PipelineStateInfo().setPipelineId("pipe2"));
        Iterable<PipelineStateInfo> pipelineStateInfoIterable = pipelineStateInfos;

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        Either<FailedOperation, Void> result =
                dltWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        verify(workspaceClient.pipelines(), times(2)).delete(anyString());
    }

    @Test
    public void unprovisionWorkload_ErrorDeletingAPipeline() {

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, true);

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo(workspaceName, "test", "test", "test", "test");

        Optional<DatabricksWorkspaceInfo> optionalDatabricksWorkspaceInfo = Optional.of(databricksWorkspaceInfo);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(
                new PipelineStateInfo().setPipelineId("pipe1"), new PipelineStateInfo().setPipelineId("pipe2"));
        Iterable<PipelineStateInfo> pipelineStateInfoIterable = pipelineStateInfos;

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        String expectedError = "error deleting pipe2";
        doThrow(new DatabricksException(expectedError)).when(pipelinesAPI).delete("pipe2");

        Either<FailedOperation, Void> result =
                dltWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assertTrue(result.getLeft().problems().get(0).description().contains(expectedError));
    }
}
