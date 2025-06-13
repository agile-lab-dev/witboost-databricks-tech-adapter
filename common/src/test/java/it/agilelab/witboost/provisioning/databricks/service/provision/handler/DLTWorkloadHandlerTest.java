package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

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
import it.agilelab.witboost.provisioning.databricks.bean.WorkspaceClientConfig;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManagerFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.config.*;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.SparkConf;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt.DLTClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt.DatabricksDLTWorkloadSpecific;
import java.util.*;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
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

    @MockBean
    WorkspaceLevelManagerFactory workspaceLevelManagerFactory;

    @Mock
    WorkspaceLevelManager workspaceLevelManager;

    @MockBean
    private Function<WorkspaceClientConfig.WorkspaceClientConfigParams, WorkspaceClient> workspaceClientFactory;

    private DLTWorkloadHandler dltWorkloadHandler;
    private DataProduct dataProduct;
    private Workload<DatabricksDLTWorkloadSpecific> workload;

    private final DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspace", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);

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
                azureAuthConfig,
                gitCredentialsConfig,
                databricksPermissionsConfig,
                accountClient,
                workspaceLevelManagerFactory,
                workspaceClientFactory);
        setUpDataProduct();
        setUpWorkload();

        lenient()
                .when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(any(WorkspaceClient.class)))
                .thenReturn(workspaceLevelManager);
        lenient().when(workspaceLevelManager.setGitCredentials(any(), any())).thenReturn(Either.right(null));
    }

    @Test
    public void provisionWorkload_Success() {

        Iterable<CatalogInfo> iterableCatalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(metastoresList);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(reposAPI.create(any(CreateRepoRequest.class))).thenReturn(repoInfo);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users = Collections.singletonList(
                new User().setUserName("name.surname@company.it").setId("123"));

        List<Group> groups = Collections.singletonList(
                new Group().setDisplayName("developers").setId("456"));

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
        Iterable<CatalogInfo> iterableCatalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);

        Iterable<MetastoreInfo> iterableMetastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(reposAPI.create(any(CreateRepoRequest.class))).thenReturn(repoInfo);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users = Collections.singletonList(
                new User().setUserName("name.surname@company.it").setId("123"));
        List<Group> groups = Collections.singletonList(
                new Group().setDisplayName("developers").setId("234"));

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
                azureAuthConfig,
                gitCredentialsConfig,
                databricksPermissionsConfig,
                accountClient,
                workspaceLevelManagerFactory,
                workspaceClientFactory);

        Either<FailedOperation, String> result =
                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        assertEquals("123", result.get());
    }

    @Test
    public void provisionWorkload_ErrorMappingDpOwner() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        dataProduct.setDataProductOwner("wrong_user");
        Iterable<CatalogInfo> iterableCatalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);

        Iterable<MetastoreInfo> iterableMetastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

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

    // TODO: Temporarily removed. See annotation in BaseWorkloadHandler.mapUsers

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
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        Iterable<CatalogInfo> iterableCatalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);

        Iterable<MetastoreInfo> iterableMetastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, String> result =
                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        String errorMessage =
                "An error occurred while creating the repo with URL https://github.com/repo.git in workspace.";
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void provisionWorkload_ErrorAssigningPermissions() {

        Iterable<CatalogInfo> iterableCatalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);

        Iterable<MetastoreInfo> iterableMetastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users = Collections.singletonList(
                new User().setUserName("name.surname@company.it").setId("123"));
        List<Group> groups = Collections.singletonList(
                new Group().setDisplayName("developers").setId("456"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(reposAPI.create(any(CreateRepoRequest.class))).thenReturn(repoInfo);

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

        Iterable<CatalogInfo> iterableCatalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);

        Iterable<MetastoreInfo> iterableMetastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(reposAPI.create(any(CreateRepoRequest.class))).thenReturn(repoInfo);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users = Collections.singletonList(
                new User().setUserName("name.surname@company.it").setId("123"));

        List<Group> groups = Collections.singletonList(
                new Group().setDisplayName("developers").setId("456"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(reposAPI.getPermissions(anyString())).thenReturn(repoPermissions);

        dataProduct.setEnvironment("qa");
        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, String> result =
                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        String expectedError =
                "An error occurred while creating the DLT Pipeline pipelineName in workspace. Please try again and if the error persists contact the platform team. Details: Cannot invoke \"com.databricks.sdk.service.pipelines.CreatePipelineResponse";
        assertTrue(result.getLeft().problems().get(0).description().contains(expectedError));
    }

    @Test
    public void provisionWorkload_ErrorGettingSparkEnvVars() {

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any()))
                .thenReturn(Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2")));

        Iterable<MetastoreInfo> iterableMetastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(reposAPI.create(any(CreateRepoRequest.class))).thenReturn(repoInfo);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users = Collections.singletonList(
                new User().setUserName("name.surname@company.it").setId("123"));
        List<Group> groups = Collections.singletonList(
                new Group().setDisplayName("developers").setId("234"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(reposAPI.getPermissions(anyString())).thenReturn(repoPermissions);

        dataProduct.setEnvironment("INVALID");
        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        DatabricksPermissionsConfig.Workload workloadPermissions = new DatabricksPermissionsConfig.Workload();
        workloadPermissions.setDeveloper("NO_PERMISSIONS");
        workloadPermissions.setOwner("NO_PERMISSIONS");
        databricksPermissionsConfig.setWorkload(workloadPermissions);
        dltWorkloadHandler = new DLTWorkloadHandler(
                azureAuthConfig,
                gitCredentialsConfig,
                databricksPermissionsConfig,
                accountClient,
                workspaceLevelManagerFactory,
                workspaceClientFactory);

        Either<FailedOperation, String> result =
                dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        String expectedError =
                "An error occurred while getting the Spark environment variables for the pipeline 'pipelineName' in the environment 'INVALID'. The specified environment is invalid. Available options are: DEVELOPMENT, QA, PRODUCTION.";
        assertTrue(result.getLeft().problems().get(0).description().contains(expectedError));
    }

    @Test
    public void provisionWorkload_Exception() {
        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, new Workload<>(), false);
        try {
            dltWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);
        } catch (Exception e) {
            assertEquals(NullPointerException.class, e.getClass());
        }
    }

    @Test
    public void unprovisionWorkload_Success() {

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, true);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        when(pipelinesAPI.listPipelines(any()))
                .thenReturn(Arrays.asList(
                        new PipelineStateInfo().setPipelineId("pipe1"),
                        new PipelineStateInfo().setPipelineId("pipe2")));

        when(pipelinesAPI.listPipelines(any()))
                .thenReturn(Arrays.asList(
                        new PipelineStateInfo().setPipelineId("pipe1"),
                        new PipelineStateInfo().setPipelineId("pipe2")));

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

        Iterable<PipelineStateInfo> pipelineStateInfoIterable = Arrays.asList(
                new PipelineStateInfo().setPipelineId("pipe1"), new PipelineStateInfo().setPipelineId("pipe2"));

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        String expectedError = "error deleting pipe1";
        doThrow(new DatabricksException(expectedError)).when(pipelinesAPI).delete("pipe1");

        Either<FailedOperation, Void> result =
                dltWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assertTrue(result.getLeft().problems().get(0).description().contains(expectedError));
    }

    @Test
    public void unprovisionWorkloadRemoveDataFalse_Success() {

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(
                new PipelineStateInfo().setPipelineId("pipe1"), new PipelineStateInfo().setPipelineId("pipe2"));

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfos);

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfos);

        Either<FailedOperation, Void> result =
                dltWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        verify(workspaceClient.pipelines(), times(2)).delete(anyString());
    }

    @Test
    public void unprovisionWorkloadRemoveDataTrue_Exception() {
        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        Iterable<PipelineStateInfo> pipelineStateInfoIterable = Arrays.asList(
                new PipelineStateInfo().setPipelineId("pipe1"), new PipelineStateInfo().setPipelineId("pipe2"));
        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        ProvisionRequest<DatabricksDLTWorkloadSpecific> mockProvisionRequest = mock(ProvisionRequest.class);
        Workload<DatabricksDLTWorkloadSpecific> mockComponent = mock(Workload.class);
        DatabricksDLTWorkloadSpecific mockSpecific = mock(DatabricksDLTWorkloadSpecific.class);
        when(mockProvisionRequest.component()).thenReturn(mockComponent);
        when(mockComponent.getSpecific()).thenReturn(mockSpecific);
        when(mockSpecific.getGit()).thenThrow(new DatabricksException("Exception retrieving git infos"));

        Either<FailedOperation, Void> result =
                dltWorkloadHandler.unprovisionWorkload(mockProvisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft().problems().get(0).description().contains("Details: Exception retrieving git infos");
        verify(workspaceClient.pipelines(), times(2)).delete(anyString());
    }

    @Test
    public void unprovisionWorkload_ErrorRemovingRepo() {

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, true);

        PipelinesAPI pipelinesAPI = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(pipelinesAPI);

        Iterable<PipelineStateInfo> pipelineStateInfoIterable = Arrays.asList(
                new PipelineStateInfo().setPipelineId("pipe1"), new PipelineStateInfo().setPipelineId("pipe2"));

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        when(pipelinesAPI.listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        String errorMessage = "This is a workspace list exception";
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        when(workspaceAPI.list(anyString())).thenThrow(new RuntimeException(errorMessage));
        Either<FailedOperation, Void> result =
                dltWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        verify(workspaceClient.pipelines(), times(2)).delete(anyString());

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("An error occurred while deleting the repo with path /dataproduct/component in workspace.");
    }

    private void setUpWorkload() {
        workload = new Workload<>();

        DLTClusterSpecific cluster = new DLTClusterSpecific();
        cluster.setMode(PipelineClusterAutoscaleMode.LEGACY);
        cluster.setWorkerType("Standard_DS3_v2");
        cluster.setDriverType("Standard_DS3_v2");
        cluster.setPolicyId("policyId");

        SparkConf sparkConf = new SparkConf();
        sparkConf.setName("spark.conf");
        sparkConf.setValue("value");
        cluster.setSparkConf(List.of(sparkConf));

        DatabricksDLTWorkloadSpecific specific = new DatabricksDLTWorkloadSpecific();
        specific.setWorkspace("workspace");
        specific.setPipelineName("pipelineName");
        specific.setRepoPath("dataproduct/component");
        specific.setProductEdition(DatabricksDLTWorkloadSpecific.ProductEdition.CORE);
        specific.setContinuous(true);
        specific.setNotebooks(List.of("notebook1", "notebook2"));
        specific.setFiles(List.of("file1", "file2"));
        specific.setCatalog("catalog");
        specific.setTarget("target");
        specific.setPhoton(true);
        List<DatabricksDLTWorkloadSpecific.PipelineNotification> notifications = new ArrayList<>();
        notifications.add(new DatabricksDLTWorkloadSpecific.PipelineNotification(
                "email@email.com", Collections.singletonList("on-update-test")));
        notifications.add(new DatabricksDLTWorkloadSpecific.PipelineNotification(
                "email2@email.com", Collections.singletonList("on-update-test")));
        specific.setNotifications(notifications);
        specific.setChannel(DatabricksDLTWorkloadSpecific.PipelineChannel.CURRENT);
        specific.setCluster(cluster);
        specific.setMetastore("metastore");

        DatabricksWorkflowWorkloadSpecific.GitSpecific dltGitSpecific =
                new DatabricksWorkflowWorkloadSpecific.GitSpecific();
        dltGitSpecific.setGitRepoUrl("https://github.com/repo.git");
        specific.setGit(dltGitSpecific);

        workload.setSpecific(specific);
        workload.setName("fake_workload");
    }

    private void setUpDataProduct() {
        dataProduct = new DataProduct();
        dataProduct.setEnvironment("development");
        dataProduct.setDataProductOwner("user:name.surname@company.it");
        dataProduct.setDevGroup("group:developers");
    }
}
