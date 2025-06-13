package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
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
import it.agilelab.witboost.provisioning.databricks.bean.WorkspaceClientConfig;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManagerFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.SparkConf;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.SparkEnvVar;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.DatabricksJobWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.JobClusterSpecific;
import jakarta.validation.constraints.NotNull;
import java.util.*;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
@ExtendWith(MockitoExtension.class)
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

    @MockBean
    WorkspaceLevelManagerFactory workspaceLevelManagerFactory;

    @Mock
    WorkspaceLevelManager workspaceLevelManager;

    @MockBean
    Function<WorkspaceClientConfig.WorkspaceClientConfigParams, WorkspaceClient> workspaceClientFactory;

    private DataProduct dataProduct;
    private Workload<DatabricksJobWorkloadSpecific> workload;

    private final DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
            "workspace", "123", "https://example.com", "abc", "test", ProvisioningState.SUCCEEDED);

    private final String workspaceName = "testWorkspace";
    private final String runAsSPDisplayName = "service-principal-name";

    @BeforeEach
    public void setUp() {
        dataProduct = new DataProduct();
        dataProduct.setDataProductOwner("user:name.surname@company.it");
        dataProduct.setDevGroup("group:developers");
        dataProduct.setEnvironment("development");

        setUpWorkload();

        workspaceInfo.setManaged(true);
    }

    private void setUpWorkload() {
        workload = new Workload<>();
        workload.setName("workload");

        DatabricksJobWorkloadSpecific databricksJobWorkloadSpecific = new DatabricksJobWorkloadSpecific();
        databricksJobWorkloadSpecific.setWorkspace(workspaceName);
        databricksJobWorkloadSpecific.setJobName("jobName");
        databricksJobWorkloadSpecific.setRepoPath("dataproduct/component");
        databricksJobWorkloadSpecific.setRunAsPrincipalName(runAsSPDisplayName);

        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                new DatabricksJobWorkloadSpecific.JobGitSpecific();
        jobGitSpecific.setGitRepoUrl("repoUrl");
        jobGitSpecific.setGitReference("main");
        jobGitSpecific.setGitReferenceType(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);
        jobGitSpecific.setGitPath("/src");
        databricksJobWorkloadSpecific.setGit(jobGitSpecific);

        JobClusterSpecific jobClusterSpecific = getJobClusterSpecific();
        databricksJobWorkloadSpecific.setCluster(jobClusterSpecific);

        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific =
                new DatabricksJobWorkloadSpecific.SchedulingSpecific();
        schedulingSpecific.setCronExpression("00 * * * * ?");
        schedulingSpecific.setJavaTimezoneId("UTC");
        databricksJobWorkloadSpecific.setScheduling(schedulingSpecific);
        databricksJobWorkloadSpecific.setWorkspace("workspace");
        workload.setSpecific(databricksJobWorkloadSpecific);
    }

    @NotNull
    private static JobClusterSpecific getJobClusterSpecific() {
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
        SparkEnvVar sparkEnvVar = new SparkEnvVar("spark.env.var", "value");
        jobClusterSpecific.setSparkEnvVarsProduction(List.of(sparkEnvVar));
        jobClusterSpecific.setSparkEnvVarsQa(List.of(sparkEnvVar));
        jobClusterSpecific.setSparkEnvVarsDevelopment(List.of(sparkEnvVar));
        jobClusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        return jobClusterSpecific;
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

        when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(any(WorkspaceClient.class)))
                .thenReturn(workspaceLevelManager);
        when(workspaceLevelManager.setGitCredentials(any(), any())).thenReturn(Either.right(null));
        when(workspaceLevelManager.getServicePrincipalFromName(any()))
                .thenReturn(Either.right(new ServicePrincipal()
                        .setDisplayName("servicePrincipalDisplayName")
                        .setId("456")
                        .setApplicationId("servicePrincipalAppId")));
        when(workspaceLevelManager.generateSecretForServicePrincipal(anyLong(), anyString()))
                .thenReturn(Either.right(new AbstractMap.SimpleEntry<>("secret", "secretId")));
        when(workspaceClientFactory.apply(any())).thenReturn(workspaceClient);
        when(workspaceLevelManager.deleteServicePrincipalSecret(anyLong(), anyString()))
                .thenReturn(Either.right(null));

        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(workspaceClient.repos().create(any(CreateRepoRequest.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123L);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users = Collections.singletonList(
                new User().setUserName("name.surname@company.it").setId("123"));
        List<Group> groups = Collections.singletonList(
                new Group().setDisplayName("developers").setId("234"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(workspaceClient.repos().getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        Either<FailedOperation, String> result =
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        assertEquals("123", result.get());
    }

    @Test
    public void provisionWorkload_ErrorCreatingJob() {
        dataProduct.setEnvironment("qa");
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);

        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);

        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(reposAPI.create(any(CreateRepoRequest.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123L);

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(reposAPI.getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users = Collections.singletonList(
                new User().setUserName("name.surname@company.it").setId("123"));

        List<Group> groups = Collections.singletonList(
                new Group().setDisplayName("developers").setId("456"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);

        when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(any(WorkspaceClient.class)))
                .thenReturn(workspaceLevelManager);
        when(workspaceLevelManager.setGitCredentials(any(), any())).thenReturn(Either.right(null));
        when(workspaceLevelManager.getServicePrincipalFromName(any()))
                .thenReturn(Either.right(new ServicePrincipal()
                        .setDisplayName("servicePrincipalDisplayName")
                        .setId("456")
                        .setApplicationId("servicePrincipalAppId")));
        when(workspaceLevelManager.generateSecretForServicePrincipal(anyLong(), anyString()))
                .thenReturn(Either.right(new AbstractMap.SimpleEntry<>("secret", "secretId")));
        when(workspaceClientFactory.apply(any())).thenReturn(workspaceClient);
        when(workspaceLevelManager.deleteServicePrincipalSecret(anyLong(), anyString()))
                .thenReturn(Either.right(null));

        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        Either<FailedOperation, String> result =
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assertTrue(result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("because the return value of \"com.databricks.sdk.WorkspaceClient.jobs()\" is null"));
    }

    @Test
    public void provisionWorkload_ErrorMappingDpOwner() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        dataProduct.setDataProductOwner("wrong_user");

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

        Iterable<BaseJob> baseJobIterable = Arrays.asList(new BaseJob().setJobId(1L), new BaseJob().setJobId(2L));

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

        Iterable<BaseJob> baseJobIterable = Arrays.asList(new BaseJob().setJobId(1L), new BaseJob().setJobId(2L));

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
                        .setObjectId(1L),
                new ObjectInfo()
                        .setObjectType(ObjectType.REPO)
                        .setPath("/dataproduct/repo2")
                        .setObjectId(2L));

        when(workspaceClient.workspace().list("/dataproduct")).thenReturn(folderContent);

        Either<FailedOperation, Void> result =
                jobWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        verify(workspaceClient.jobs(), times(2)).delete(anyLong());
        assert result.isRight();
    }

    @Test
    public void unprovisionWorkloadRemoveDataTrue_Exception() {

        Iterable<BaseJob> baseJobIterable = Arrays.asList(new BaseJob().setJobId(1L), new BaseJob().setJobId(2L));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any())).thenReturn(baseJobIterable);

        ProvisionRequest<DatabricksJobWorkloadSpecific> mockProvisionRequest = mock(ProvisionRequest.class);
        Workload<DatabricksJobWorkloadSpecific> mockComponent = mock(Workload.class);
        DatabricksJobWorkloadSpecific mockSpecific = mock(DatabricksJobWorkloadSpecific.class);
        when(mockProvisionRequest.component()).thenReturn(mockComponent);
        when(mockComponent.getSpecific()).thenReturn(mockSpecific);
        when(mockSpecific.getGit()).thenThrow(new DatabricksException("Exception retrieving git infos"));

        Either<FailedOperation, Void> result =
                jobWorkloadHandler.unprovisionWorkload(mockProvisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft().problems().get(0).description().contains("Details: Exception retrieving git infos");
        verify(workspaceClient.jobs(), times(2)).delete(anyLong());
    }

    @Test
    public void provisionWorkload_ErrorUpdatingUser() {
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);
        mockJobAPI(workspaceClient);
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));

        when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(any(WorkspaceClient.class)))
                .thenReturn(workspaceLevelManager);
        when(workspaceLevelManager.setGitCredentials(any(), any())).thenReturn(Either.right(null));

        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(workspaceClient.repos().create(any(CreateRepoRequest.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123L);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

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
        AccountGroupsAPI accountGroupsAPIMock = mock(AccountGroupsAPI.class);
        when(accountClient.groups()).thenReturn(accountGroupsAPIMock);
        //        when(accountGroupsAPIMock.list(any())).thenReturn(List.of(new Group().setDisplayName("developers")));

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);
        mockJobAPI(workspaceClient);
        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        Either<FailedOperation, String> result =
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("Group 'developers' not found at Databricks account level.");
    }

    @Test
    public void provisionWorkload_ErrorGettingSparkEnvVars() {
        dataProduct.setEnvironment("INVALID");
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);
        mockJobAPI(workspaceClient);
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));

        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(workspaceClient.repos().create(any(CreateRepoRequest.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123L);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users = Collections.singletonList(
                new User().setUserName("name.surname@company.it").setId("123"));
        List<Group> groups = Collections.singletonList(
                new Group().setDisplayName("developers").setId("234"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(workspaceClient.repos().getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        lenient()
                .when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(any(WorkspaceClient.class)))
                .thenReturn(workspaceLevelManager);
        lenient().when(workspaceLevelManager.setGitCredentials(any(), any())).thenReturn(Either.right(null));
        lenient()
                .when(workspaceLevelManager.getServicePrincipalFromName(any()))
                .thenReturn(Either.right(new ServicePrincipal()
                        .setDisplayName("servicePrincipalDisplayName")
                        .setId("456")
                        .setApplicationId("servicePrincipalAppId")));
        when(workspaceLevelManager.generateSecretForServicePrincipal(anyLong(), anyString()))
                .thenReturn(Either.right(new AbstractMap.SimpleEntry<>("secret", "secretId")));
        when(workspaceClientFactory.apply(any())).thenReturn(workspaceClient);
        when(workspaceLevelManager.deleteServicePrincipalSecret(anyLong(), anyString()))
                .thenReturn(Either.right(null));

        Either<FailedOperation, String> result =
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isLeft();
        String expectedError =
                "An error occurred while getting the Spark environment variables for the job 'jobName' in the environment 'INVALID'. The specified environment is invalid. Available options are: DEVELOPMENT, QA, PRODUCTION.";
        assertTrue(result.getLeft().problems().get(0).description().contains(expectedError));
    }

    @Test
    public void provisionWorkload_Exception() {
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, new Workload<>(), false);
        try {
            jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);
        } catch (Exception e) {
            assertEquals(NullPointerException.class, e.getClass());
        }
    }

    @Test
    public void unprovisionWorkload_ErrorDeletingJobs() {

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        Iterable<BaseJob> baseJobIterable = Arrays.asList(new BaseJob().setJobId(1L), new BaseJob().setJobId(2L));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any())).thenReturn(baseJobIterable);

        String expectedError = "error while deleting job";
        doThrow(new DatabricksException(expectedError)).when(jobsAPI).delete(1L);
        doThrow(new DatabricksException(expectedError)).when(jobsAPI).delete(2L);

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
    public void unprovisionWorkload_ErrorRemovingRepo() {

        mockReposAPI(workspaceClient);
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, true);

        Iterable<BaseJob> baseJobIterable = Arrays.asList(new BaseJob().setJobId(1L), new BaseJob().setJobId(2L));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any())).thenReturn(baseJobIterable);

        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        String errorMessage = "This is a workspace list exception";
        when(workspaceAPI.list(anyString())).thenThrow(new RuntimeException(errorMessage));
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);

        Either<FailedOperation, Void> result =
                jobWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        verify(workspaceClient.jobs(), times(2)).delete(anyLong());

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("An error occurred while deleting the repo with path /dataproduct/component in workspace.");
        assert result.getLeft().problems().get(0).description().contains(errorMessage);
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

    @Test
    public void provisionWorkload_ToStringException() {
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);
        mockJobAPI(workspaceClient);
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));

        when(workspaceLevelManagerFactory.createDatabricksWorkspaceLevelManager(any(WorkspaceClient.class)))
                .thenReturn(workspaceLevelManager);
        when(workspaceLevelManager.setGitCredentials(any(), any())).thenReturn(Either.right(null));

        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(workspaceClient.repos().create(any(CreateRepoRequest.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123L);

        when(accountClient.users()).thenReturn(mock(AccountUsersAPI.class));
        when(accountClient.groups()).thenReturn(mock(AccountGroupsAPI.class));

        List<User> users = Collections.singletonList(
                new User().setUserName("name.surname@company.it").setId("123"));
        List<Group> groups = Collections.singletonList(
                new Group().setDisplayName("developers").setId("234"));

        when(accountClient.users().list(any())).thenReturn(users);
        when(accountClient.groups().list(any())).thenReturn(groups);

        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        when(workspaceClient.repos().getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        //        when(workspaceClient.jobs().create(any())).thenReturn(new CreateResponse().setJobId(null));
        Either<FailedOperation, String> result =
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        String expectedError =
                "An error occurred while provisioning component workload. Please try again and if the error persists contact the platform team.";

        assert result.isLeft();
        assert result.getLeft().problems().get(0).description().contains(expectedError);
    }

    private void mockJobAPI(WorkspaceClient workspaceClient) {
        JobsAPI jobsAPI = mock(JobsAPI.class);
        CreateResponse createResponse = mock(CreateResponse.class);
        Job job = mock(Job.class);
        lenient().when(workspaceClient.jobs()).thenReturn(jobsAPI);
        lenient().when(jobsAPI.create(any())).thenReturn(createResponse);
        lenient().when(createResponse.getJobId()).thenReturn(123L);
        lenient().when(jobsAPI.get(anyLong())).thenReturn(job);
    }

    private void mockReposAPI(WorkspaceClient workspaceClient) {
        ReposAPI reposAPI = mock(ReposAPI.class);
        lenient().when(workspaceClient.repos()).thenReturn(reposAPI);
    }
}
