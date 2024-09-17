package it.agilelab.witboost.provisioning.databricks.service.provision;

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
import com.databricks.sdk.service.compute.AzureAvailability;
import com.databricks.sdk.service.compute.RuntimeEngine;
import com.databricks.sdk.service.iam.*;
import com.databricks.sdk.service.jobs.BaseJob;
import com.databricks.sdk.service.jobs.CreateResponse;
import com.databricks.sdk.service.jobs.Job;
import com.databricks.sdk.service.jobs.JobsAPI;
import com.databricks.sdk.service.workspace.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.*;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.*;
import java.util.*;
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
public class JobWorkloadHandlerTest {
    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @Autowired
    private JobWorkloadHandler jobWorkloadHandler;

    @MockBean
    AccountClient accountClient;

    @Mock
    WorkspaceClient workspaceClient;

    @Autowired
    AzureAuthConfig azureAuthConfig;

    private DataProduct dataProduct;
    private Workload workload;
    private DatabricksJobWorkloadSpecific databricksJobWorkloadSpecific;

    private DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspace", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);
    private String workspaceName = "testWorkspace";

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        dataProduct = new DataProduct();
        databricksJobWorkloadSpecific = new DatabricksJobWorkloadSpecific();
        workload = new Workload();

        databricksJobWorkloadSpecific.setWorkspace(workspaceName);
        databricksJobWorkloadSpecific.setJobName("jobName");
        databricksJobWorkloadSpecific.setRepoPath("dataproduct/component");

        JobGitSpecific jobGitSpecific = new JobGitSpecific();
        jobGitSpecific.setGitRepoUrl("repoUrl");
        jobGitSpecific.setGitReference("main");
        jobGitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        jobGitSpecific.setGitPath("/src");
        databricksJobWorkloadSpecific.setGit(jobGitSpecific);

        JobClusterSpecific jobClusterSpecific = new JobClusterSpecific();
        jobClusterSpecific.setSpotBidMaxPrice(10D);
        jobClusterSpecific.setFirstOnDemand(5L);
        jobClusterSpecific.setSpotInstances(true);
        jobClusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        jobClusterSpecific.setDriverNodeTypeId("driverNodeTypeId");
        SparkConf sparkConf = new SparkConf();
        sparkConf.setName("spark.conf");
        sparkConf.setValue("value");
        jobClusterSpecific.setSparkConf(List.of(sparkConf));
        SparkEnvVar sparkEnvVar = new SparkEnvVar();
        sparkEnvVar.setName("spark.env.var");
        sparkEnvVar.setValue("value");
        jobClusterSpecific.setSparkEnvVars(List.of(sparkEnvVar));
        jobClusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        databricksJobWorkloadSpecific.setCluster(jobClusterSpecific);

        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setCronExpression("00 * * * * ?");
        schedulingSpecific.setJavaTimezoneId("UTC");
        databricksJobWorkloadSpecific.setScheduling(schedulingSpecific);
        databricksJobWorkloadSpecific.setWorkspace("workspace");
        databricksJobWorkloadSpecific.setMetastore("metastore");
        workload.setSpecific(databricksJobWorkloadSpecific);

        workload.setName("workload");
        dataProduct.setDataProductOwner("user:name.surname@company.it");
        dataProduct.setDevGroup("group:developers");
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
    public void provisionWorkload_Success() {
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
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

        RepoInfo repoInfo = mock(RepoInfo.class);
        when(workspaceClient.repos().create(any(CreateRepo.class))).thenReturn(repoInfo);
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
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        assertEquals(result.get(), "123");
    }

    @Test
    public void provisionWorkload_ErrorAttachingMetastore() {
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);
        mockJobAPI(workspaceClient);
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));

        List<MetastoreInfo> metastoresList = Arrays.asList(
                new MetastoreInfo().setName("metastore1").setMetastoreId("id"),
                new MetastoreInfo().setName("metastore2").setMetastoreId("id2"));

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(metastoresList);

        Either<FailedOperation, String> result =
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft().problems().get(0).description().contains("Details: Metastore not found");
    }

    @Test
    public void provisionWorkload_ErrorCreatingJob() {
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
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

        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.create(any(CreateRepo.class))).thenReturn(repoInfo);
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
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assertTrue(
                result.getLeft()
                        .problems()
                        .get(0)
                        .description()
                        .contains(
                                "Cannot invoke \"com.databricks.sdk.service.jobs.JobsAPI.create(com.databricks.sdk.service.jobs.CreateJob)\" because the return value of \"com.databricks.sdk.WorkspaceClient.jobs()\" is null"));
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

        MetastoresAPI metastoresAPI = mock(MetastoresAPI.class);
        when(workspaceClient.metastores()).thenReturn(metastoresAPI);
        when(metastoresAPI.list()).thenReturn(metastoresList);

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);
        Either<FailedOperation, String> result =
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("The subject wrong_user is neither a Witboost user nor a group");
    }

    // TODO: Temporarily removed. See annotation in JobWorkloadHandler.provisionWorkload

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
    //        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
    //                new ProvisionRequest<>(dataProduct, workload, false);
    //        Either<FailedOperation, String> result =
    //                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);
    //
    //        assert result.isLeft();
    //        assert result.getLeft()
    //                .problems()
    //                .get(0)
    //                .description()
    //                .contains("The subject wrong_group is neither a Witboost user nor a group");
    //    }

    @Test
    public void unprovisionWorkloadRemoveDataFalse_Success() {

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        List<BaseJob> baseJobList = Arrays.asList(new BaseJob().setJobId(1l), new BaseJob().setJobId(2l));
        Iterable<BaseJob> baseJobIterable = baseJobList;

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any())).thenReturn(baseJobIterable);

        Either<FailedOperation, Void> result =
                jobWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        verify(workspaceClient.jobs(), times(2)).delete(anyLong());
    }

    @Test
    public void unprovisionWorkloadRemoveDataTrue_Success() {

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
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
        RepoInfo repoInfo = mock(RepoInfo.class);
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
                jobWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        verify(workspaceClient.jobs(), times(2)).delete(anyLong());
        assert result.isRight();
    }

    @Test
    public void provisionWorkload_ErrorUpdatingUser() {
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
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

        RepoInfo repoInfo = mock(RepoInfo.class);
        when(workspaceClient.repos().create(any(CreateRepo.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123l);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));
        when(workspaceClient.users()).thenReturn(mock(UsersAPI.class));
        when(workspaceClient.groups()).thenReturn(mock(GroupsAPI.class));

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(workspaceClient.repos().getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());

        Either<FailedOperation, String> result =
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("User name.surname@company.it not found at Databricks account level.");
    }

    @Test
    public void provisionWorkload_ErrorUpdatingGroup() {
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
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

        RepoInfo repoInfo = mock(RepoInfo.class);
        when(workspaceClient.repos().create(any(CreateRepo.class))).thenReturn(repoInfo);
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
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("Group developers not found at Databricks account level.");
    }

    @Test
    public void provisionWorkload_Exception() {
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, new Workload(), false);
        try {
            jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);
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
    public void unprovisionWorkload_ErrorDeletingJobs() {

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
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
                jobWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

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
    public void unprovisionWorkload_ErrorGettingJobs() {

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);

        String expectedError = "exception while listing jobs";
        doThrow(new DatabricksException(expectedError)).when(jobsAPI).list(any());

        Either<FailedOperation, Void> result =
                jobWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft().problems().get(0).description().contains(expectedError);
    }
}
