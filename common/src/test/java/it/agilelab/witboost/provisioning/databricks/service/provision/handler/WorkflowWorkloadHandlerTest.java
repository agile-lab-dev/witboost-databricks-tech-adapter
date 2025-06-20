package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CatalogsAPI;
import com.databricks.sdk.service.catalog.MetastoreInfo;
import com.databricks.sdk.service.catalog.MetastoresAPI;
import com.databricks.sdk.service.iam.*;
import com.databricks.sdk.service.jobs.*;
import com.databricks.sdk.service.workspace.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManagerFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class WorkflowWorkloadHandlerTest {
    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @Autowired
    private WorkflowWorkloadHandler workflowWorkloadHandler;

    @MockBean
    AccountClient accountClient;

    @Mock
    WorkspaceClient workspaceClient;

    @Autowired
    AzureAuthConfig azureAuthConfig;

    @MockBean
    WorkspaceLevelManagerFactory workspaceLevelManagerFactory;

    @Mock
    WorkspaceLevelManager workspaceLevelManager;

    private DataProduct dataProduct;
    private Workload workload;
    private DatabricksWorkflowWorkloadSpecific databricksWorkflowWorkloadSpecific;

    private DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspace", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);
    private String workspaceName = "testWorkspace";

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        dataProduct = new DataProduct();
        databricksWorkflowWorkloadSpecific = new DatabricksWorkflowWorkloadSpecific();
        workload = new Workload();

        databricksWorkflowWorkloadSpecific.setWorkspace(workspaceName);
        databricksWorkflowWorkloadSpecific.setRepoPath("dataproduct/component");

        DatabricksWorkflowWorkloadSpecific.GitSpecific gitSpecific =
                new DatabricksWorkflowWorkloadSpecific.GitSpecific();
        gitSpecific.setGitRepoUrl("https://github.com/repo.git");
        databricksWorkflowWorkloadSpecific.setGit(gitSpecific);

        Job workflow = new Job();
        workflow.setSettings(new JobSettings().setName("workflowName"));
        databricksWorkflowWorkloadSpecific.setWorkflow(workflow);

        workload.setSpecific(databricksWorkflowWorkloadSpecific);
        workload.setName("workload");
        dataProduct.setDataProductOwner("user:name.surname@company.it");
        dataProduct.setDevGroup("group:developers");

        lenient()
                .when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(any(WorkspaceClient.class)))
                .thenReturn(workspaceLevelManager);
        lenient().when(workspaceLevelManager.setGitCredentials(any(), any())).thenReturn(Either.right(null));
    }

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

    @Test
    public void provisionWorkflow_Success() {
        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);
        mockJobAPI(workspaceClient);
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(metastoresList);

        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(workspaceClient.repos().create(any(CreateRepoRequest.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123l);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));
        when(workspaceClient.users()).thenReturn(mock(UsersAPI.class));
        when(workspaceClient.groups()).thenReturn(mock(GroupsAPI.class));

        List<User> users =
                Arrays.asList(new User().setUserName("name.surname@company.it").setId("123"));
        List<Group> groups =
                Arrays.asList(new Group().setDisplayName("developers").setId("234"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(workspaceClient.repos().getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        Either<FailedOperation, String> result =
                workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        assertEquals(result.get(), "123");
    }

    @Test
    public void provisionWorkflow_ErrorCreatingJob() {
        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);

        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(metastoresList);

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);

        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(reposAPI.create(any(CreateRepoRequest.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123l);

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(reposAPI.getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));
        when(workspaceClient.users()).thenReturn(mock(UsersAPI.class));
        when(workspaceClient.groups()).thenReturn(mock(GroupsAPI.class));

        List<User> users =
                Arrays.asList(new User().setUserName("name.surname@company.it").setId("123"));

        List<Group> groups =
                Arrays.asList(new Group().setDisplayName("developers").setId("456"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);

        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        Either<FailedOperation, String> result =
                workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assertTrue(
                result.getLeft()
                        .problems()
                        .get(0)
                        .description()
                        .contains(
                                "Cannot invoke \"com.databricks.sdk.service.jobs.JobsAPI.list(com.databricks.sdk.service.jobs.ListJobsRequest)\" because the return value of \"com.databricks.sdk.WorkspaceClient.jobs()\" is null"));
    }

    @Test
    public void provisionWorkflow_ErrorMappingDpOwner() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        dataProduct.setDataProductOwner("wrong_user");
        List<CatalogInfo> catalogList =
                Arrays.asList(new CatalogInfo().setName("catalog"), new CatalogInfo().setName("catalog2"));
        Iterable<CatalogInfo> iterableCatalogList = catalogList;

        when(workspaceClient.catalogs()).thenReturn(mock(CatalogsAPI.class));
        when(workspaceClient.catalogs().list(any())).thenReturn(iterableCatalogList);

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(metastoresList);

        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, String> result =
                workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("The subject wrong_user is neither a Witboost user nor a group");
    }

    // TODO: Temporarily removed. See annotation in BaseWorkloadHanler.mapUsers

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
    //        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
    //                new ProvisionRequest<>(dataProduct, workload, false);
    //        Either<FailedOperation, String> result =
    //                workflowWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);
    //
    //        assert result.isLeft();
    //        assert result.getLeft()
    //                .problems()
    //                .get(0)
    //                .description()
    //                .contains("The subject wrong_group is neither a Witboost user nor a group");
    //    }

    @Test
    public void unprovisionWorkflowRemoveDataFalse_Success() {

        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        List<BaseJob> baseJobList = Arrays.asList(new BaseJob().setJobId(1l), new BaseJob().setJobId(2l));
        Iterable<BaseJob> baseJobIterable = baseJobList;

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any())).thenReturn(baseJobIterable);

        Either<FailedOperation, Void> result =
                workflowWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        verify(workspaceClient.jobs(), times(2)).delete(anyLong());
    }

    @Test
    public void unprovisionWorkflowRemoveDataTrue_Success() {

        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, true);

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo(workspaceName, "123", "test", "test", "test", ProvisioningState.SUCCEEDED);

        Optional<DatabricksWorkspaceInfo> optionalDatabricksWorkspaceInfo = Optional.of(databricksWorkspaceInfo);

        List<BaseJob> baseJobList = Arrays.asList(new BaseJob().setJobId(1l), new BaseJob().setJobId(2l));
        Iterable<BaseJob> baseJobIterable = baseJobList;

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any())).thenReturn(baseJobIterable);

        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);

        Iterable<ObjectInfo> objectInfos = mock(Iterable.class);
        when(workspaceAPI.list(anyString())).thenReturn(objectInfos);
        GetRepoResponse repoInfo = mock(GetRepoResponse.class);
        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        when(reposAPI.get(anyLong())).thenReturn(repoInfo);
        when(repoInfo.getUrl()).thenReturn("repoUrl");
        when(repoInfo.getPath()).thenReturn("path");

        List<ObjectInfo> folderContent = Arrays.asList(
                new ObjectInfo()
                        .setObjectType(ObjectType.REPO)
                        .setPath("/dataproduct/repo")
                        .setObjectId(1l),
                new ObjectInfo()
                        .setObjectType(ObjectType.REPO)
                        .setPath("/dataproduct/repo2")
                        .setObjectId(2l));

        when(workspaceClient.workspace().list("/dataproduct")).thenReturn(folderContent);

        Either<FailedOperation, Void> result =
                workflowWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        verify(workspaceClient.jobs(), times(2)).delete(anyLong());
        assert result.isRight();
    }

    @Test
    public void unprovisionWorkflowRemoveDataTrue_Exception() {

        List<BaseJob> baseJobList = Arrays.asList(new BaseJob().setJobId(1l), new BaseJob().setJobId(2l));
        Iterable<BaseJob> baseJobIterable = baseJobList;

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any())).thenReturn(baseJobIterable);

        ProvisionRequest mockProvisionRequest = mock(ProvisionRequest.class);
        Workload<DatabricksWorkflowWorkloadSpecific> mockComponent = mock(Workload.class);
        DatabricksWorkflowWorkloadSpecific mockSpecific = mock(DatabricksWorkflowWorkloadSpecific.class);
        when(mockProvisionRequest.component()).thenReturn(mockComponent);
        when(mockComponent.getSpecific()).thenReturn(mockSpecific);

        Either<FailedOperation, Void> result =
                workflowWorkloadHandler.unprovisionWorkload(mockProvisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("DatabricksWorkflowWorkloadSpecific.getWorkflow()\" is null");
    }

    @Test
    public void unprovisionWorkflow_ErrorRemovingRepo() {

        mockReposAPI(workspaceClient);
        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, true);

        List<BaseJob> baseJobList = Arrays.asList(new BaseJob().setJobId(1l), new BaseJob().setJobId(2l));
        Iterable<BaseJob> baseJobIterable = baseJobList;

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any())).thenReturn(baseJobIterable);

        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        String errorMessage = "This is a workspace list exception";
        when(workspaceAPI.list(anyString())).thenThrow(new RuntimeException(errorMessage));

        Either<FailedOperation, Void> result =
                workflowWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        verify(workspaceClient.jobs(), times(2)).delete(anyLong());

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("An error occurred while deleting the repo with path /dataproduct/component in workspace.");
    }

    @Test
    public void provisionWorkflow_ErrorUpdatingUser() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);

        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);
        mockJobAPI(workspaceClient);
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));
        Iterable<MetastoreInfo> iterableMetastoresList = metastoresList;

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(iterableMetastoresList);

        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(workspaceClient.repos().create(any(CreateRepoRequest.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123l);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));
        when(workspaceClient.users()).thenReturn(mock(UsersAPI.class));
        when(workspaceClient.groups()).thenReturn(mock(GroupsAPI.class));

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(workspaceClient.repos().getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());

        Either<FailedOperation, String> result =
                workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("User name.surname@company.it not found at Databricks account level.");
    }

    @Test
    public void provisionWorkflow_ErrorUpdatingGroup() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);
        mockJobAPI(workspaceClient);
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(metastoresList);

        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(workspaceClient.repos().create(any(CreateRepoRequest.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123l);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));
        when(workspaceClient.users()).thenReturn(mock(UsersAPI.class));
        when(workspaceClient.groups()).thenReturn(mock(GroupsAPI.class));

        List<User> users =
                Arrays.asList(new User().setUserName("name.surname@company.it").setId("123"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(workspaceClient.repos().getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());

        Either<FailedOperation, String> result =
                workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("Group 'developers' not found at Databricks account level.");
    }

    @Test
    public void provisionWorkflow_Exception() {
        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, new Workload(), false);
        try {
            workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo);
        } catch (Exception e) {
            assertEquals(e.getClass(), NullPointerException.class);
        }
    }

    private void mockJobAPI(WorkspaceClient workspaceClient) {
        JobsAPI jobsAPI = mock(JobsAPI.class);
        CreateResponse createResponse = mock(CreateResponse.class);
        Job job = mock(Job.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.create(any())).thenReturn(createResponse);
        when(createResponse.getJobId()).thenReturn(123L);
        when(jobsAPI.get(anyLong())).thenReturn(job);
    }

    private void mockReposAPI(WorkspaceClient workspaceClient) {
        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
    }

    @Test
    public void unprovisionWorkflow_ErrorDeletingJobs() {

        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        List<BaseJob> baseJobList = Arrays.asList(new BaseJob().setJobId(1l), new BaseJob().setJobId(2l));
        Iterable<BaseJob> baseJobIterable = baseJobList;

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any())).thenReturn(baseJobIterable);

        String expectedError = "error while deleting job";
        doThrow(new DatabricksException(expectedError)).when(jobsAPI).delete(1l);
        doThrow(new DatabricksException(expectedError)).when(jobsAPI).delete(2l);

        Either<FailedOperation, Void> result =
                workflowWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("An error occurred while deleting the job with ID 1");
        assert result.getLeft()
                .problems()
                .get(1)
                .description()
                .contains("An error occurred while deleting the job with ID 2");

        verify(workspaceClient.jobs(), times(2)).delete(anyLong());
    }

    @Test
    public void unprovisionWorkflow_ErrorGettingJobs() {

        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        String expectedError = "exception while listing jobs";
        doThrow(new DatabricksException(expectedError)).when(jobsAPI).list(any());

        Either<FailedOperation, Void> result =
                workflowWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft().problems().get(0).description().contains(expectedError);
    }

    @Test
    public void provisionWorkflow_ToStringException() {
        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);
        mockJobAPI(workspaceClient);
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(metastoresList);

        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(workspaceClient.repos().create(any(CreateRepoRequest.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123l);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));
        when(workspaceClient.users()).thenReturn(mock(UsersAPI.class));
        when(workspaceClient.groups()).thenReturn(mock(GroupsAPI.class));

        List<User> users =
                Arrays.asList(new User().setUserName("name.surname@company.it").setId("123"));
        List<Group> groups =
                Arrays.asList(new Group().setDisplayName("developers").setId("234"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(workspaceClient.repos().getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        when(workspaceClient.jobs().create(any())).thenReturn(new CreateResponse().setJobId(null));
        Either<FailedOperation, String> result =
                workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceInfo);

        String expectedError =
                "An error occurred while provisioning component workload. Please try again and if the error persists contact the platform team.";

        assert result.isLeft();
        assert result.getLeft().problems().get(0).description().contains(expectedError);
    }

    @Test
    public void createWorkflow_exception() {
        Workload workload1 = new Workload();
        workload1.setName("Wrong workload");
        ProvisionRequest provisionRequest = new ProvisionRequest<>(dataProduct, workload1, false);
        Either<FailedOperation, Long> result =
                workflowWorkloadHandler.provisionWorkflow(provisionRequest, workspaceClient, workspaceName);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("An error occurred while creating the new Databricks workflow for component Wrong workload.");
    }
}
