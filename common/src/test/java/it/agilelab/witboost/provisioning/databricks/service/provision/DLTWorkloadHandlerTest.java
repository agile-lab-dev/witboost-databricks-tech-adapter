package it.agilelab.witboost.provisioning.databricks.service.provision;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CatalogsAPI;
import com.databricks.sdk.service.catalog.MetastoreInfo;
import com.databricks.sdk.service.catalog.MetastoresAPI;
import com.databricks.sdk.service.iam.*;
import com.databricks.sdk.service.pipelines.CreatePipelineResponse;
import com.databricks.sdk.service.pipelines.PipelineClusterAutoscaleMode;
import com.databricks.sdk.service.pipelines.PipelineStateInfo;
import com.databricks.sdk.service.pipelines.PipelinesAPI;
import com.databricks.sdk.service.workspace.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksPermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;

@SpringBootTest
@ExtendWith(MockitoExtension.class)
@EnableConfigurationProperties
public class DLTWorkloadHandlerTest {

    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @Autowired
    AzureAuthConfig azureAuthConfig;

    @Autowired
    DatabricksPermissionsConfig databricksPermissionsConfig;

    @Autowired
    GitCredentialsConfig gitCredentialsConfig;

    @Mock
    WorkspaceClient workspaceClient;

    @Mock
    WorkspaceAPI workspaceAPI;

    @MockBean
    private AccountClient accountClient;

    @MockBean
    private AzureResourceManager azureResourceManager;

    private DLTWorkloadHandler dltWorkloadHandler;
    private DataProduct dataProduct;
    private Workload workload;
    private DatabricksDLTWorkloadSpecific databricksDLTWorkloadSpecific;

    private DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspace", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);
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

        dltWorkloadHandler = new DLTWorkloadHandler(
                azureAuthConfig, gitCredentialsConfig, databricksPermissionsConfig, accountClient);
        MockitoAnnotations.openMocks(this);
        dataProduct = new DataProduct();
        workload = new Workload();

        DLTClusterSpecific cluster = new DLTClusterSpecific();
        cluster.setMode(PipelineClusterAutoscaleMode.LEGACY);
        cluster.setWorkerType("Standard_DS3_v2");
        cluster.setDriverType("Standard_DS3_v2");
        cluster.setPolicyId("policyId");
        HashMap<String, String> sparkConf = new HashMap<>();
        sparkConf.put("spark.conf", "value");
        cluster.setSparkConf(sparkConf);

        DatabricksDLTWorkloadSpecific specific = new DatabricksDLTWorkloadSpecific();
        specific.setWorkspace("workspace");
        specific.setPipelineName("pipelineName");
        specific.setRepoPath("dataproduct/component");
        specific.setProductEdition(ProductEdition.CORE);
        specific.setContinuous(true);
        specific.setNotebooks(List.of("notebook1", "notebook2"));
        specific.setFiles(List.of("file1", "file2"));
        specific.setCatalog("catalog");
        specific.setTarget("target");
        specific.setPhoton(true);
        HashMap notifications = new HashMap();
        notifications.put("email@email.com", List.of("alert1", "alert2"));
        specific.setNotifications(notifications);
        specific.setChannel(PipelineChannel.CURRENT);
        specific.setCluster(cluster);
        specific.setMetastore("metastore");

        DLTGitSpecific dltGitSpecific = new DLTGitSpecific();
        dltGitSpecific.setGitRepoUrl("https://github.com/repo.git");
        specific.setGit(dltGitSpecific);

        workload.setSpecific(specific);
        workload.setName("fake_workload");

        dataProduct.setDataProductOwner("user:name.surname@company.it");
        dataProduct.setDevGroup("group:developers");
    }

    @Test
    public void provisionWorkload_Success() {

        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));
        Iterable<CatalogInfo> iterableCatalogList = catalogList;
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
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

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.create(any(CreateRepo.class))).thenReturn(repoInfo);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users =
                Arrays.asList(new User().setUserName("name.surname@company.it").setId("123"));

        List<Group> groups =
                Arrays.asList(new Group().setDisplayName("developers").setId("456"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(reposAPI.getPermissions(anyString())).thenReturn(repoPermissions);

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
    public void provisionWorkload_SuccessNoPermissions() {

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
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

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.create(any(CreateRepo.class))).thenReturn(repoInfo);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users =
                Arrays.asList(new User().setUserName("name.surname@company.it").setId("123"));
        List<Group> groups =
                Arrays.asList(new Group().setDisplayName("developers").setId("234"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(reposAPI.getPermissions(anyString())).thenReturn(repoPermissions);

        CreatePipelineResponse createPipelineResponse = mock(CreatePipelineResponse.class);
        when(createPipelineResponse.getPipelineId()).thenReturn("123");
        when(workspaceClient.pipelines().create(any())).thenReturn(createPipelineResponse);

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        DatabricksPermissionsConfig.Workload workloadPermissions = new DatabricksPermissionsConfig.Workload();
        workloadPermissions.setDeveloper("NO_PERMISSIONS");
        workloadPermissions.setOwner("NO_PERMISSIONS");
        databricksPermissionsConfig.setWorkload(workloadPermissions);
        dltWorkloadHandler = new DLTWorkloadHandler(
                azureAuthConfig, gitCredentialsConfig, databricksPermissionsConfig, accountClient);

        Either<FailedOperation, String> result =
                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        assertEquals("123", result.get());
    }

    @Test
    public void provisionWorkload_ErrorMappingDpOwner() {

        dataProduct.setDataProductOwner("wrong_user");
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

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, String> result =
                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("The subject wrong_user is neither a Witboost user nor a group");
    }

    // TODO: Temporarily removed. See annotation in DLTWorkloadHandler.provisionWorkload

    //    @Test
    //    public void provisionWorkload_ErrorMappingDevGroup() {
    //
    //        dataProduct.setDevGroup("wrong_group");
    //        List<CatalogInfo> catalogList =
    //                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));
    //        Iterable<CatalogInfo> iterableCatalogList = catalogList;
    //
    //        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
    //        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);
    //
    //        List<MetastoreInfo> metastoresList = Arrays.asList(
    //                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
    //                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
    //        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;
    //
    //        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
    //        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
    //        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);
    //
    //        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
    //                new ProvisionRequest<>(dataProduct, workload, false);
    //        Either<FailedOperation, String> result =
    //                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);
    //
    //        assert result.isLeft();
    //        assert result.getLeft()
    //                .problems()
    //                .get(0)
    //                .description()
    //                .contains("The subject wrong_group is neither a Witboost user nor a group");
    //    }

    @Test
    public void provisionWorkload_ErrorCreatingRepo() {

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
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

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, String> result =
                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        String errorMessage =
                "Cannot invoke \"com.databricks.sdk.service.workspace.ReposAPI.create(com.databricks.sdk.service.workspace.CreateRepo)";
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void provisionWorkload_ErrorAssigningPermissions() {

        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));
        Iterable<CatalogInfo> iterableCatalogList = catalogList;
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users =
                Arrays.asList(new User().setUserName("name.surname@company.it").setId("123"));
        List<Group> groups =
                Arrays.asList(new Group().setDisplayName("developers").setId("456"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.create(any(CreateRepo.class))).thenReturn(repoInfo);

        when(reposAPI.getPermissions(anyString())).thenThrow(new DatabricksException("permissions exception"));

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, String> result =
                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft().problems().get(0).description().contains("Details: permissions exception");
    }

    @Test
    public void provisionWorkload_ErrorCreatingPipeline() {
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);

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

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.create(any(CreateRepo.class))).thenReturn(repoInfo);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users =
                Arrays.asList(new User().setUserName("name.surname@company.it").setId("123"));

        List<Group> groups =
                Arrays.asList(new Group().setDisplayName("developers").setId("456"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(reposAPI.getPermissions(anyString())).thenReturn(repoPermissions);

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

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, true);

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

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(
                new PipelineStateInfo().setPipelineId("pipe1"), new PipelineStateInfo().setPipelineId("pipe2"));
        Iterable<PipelineStateInfo> pipelineStateInfoIterable = pipelineStateInfos;

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        String expectedError = "error deleting pipe1";
        doThrow(new DatabricksException(expectedError)).when(pipelinesAPI).delete("pipe1");

        Either<FailedOperation, Void> result =
                dltWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assertTrue(result.getLeft().problems().get(0).description().contains(expectedError));
    }
}
