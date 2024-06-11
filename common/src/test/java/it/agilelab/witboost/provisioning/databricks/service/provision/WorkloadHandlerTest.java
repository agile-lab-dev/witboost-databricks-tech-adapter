package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.API.Left;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import com.azure.resourcemanager.databricks.implementation.WorkspaceImpl;
import com.azure.resourcemanager.databricks.implementation.WorkspacesImpl;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.compute.AzureAvailability;
import com.databricks.sdk.service.compute.RuntimeEngine;
import com.databricks.sdk.service.iam.GroupsAPI;
import com.databricks.sdk.service.iam.UsersAPI;
import com.databricks.sdk.service.jobs.BaseJob;
import com.databricks.sdk.service.jobs.CreateResponse;
import com.databricks.sdk.service.jobs.JobsAPI;
import com.databricks.sdk.service.workspace.ReposAPI;
import com.databricks.sdk.service.workspace.WorkspaceAPI;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.DatabricksWorkspaceClientBean;
import it.agilelab.witboost.provisioning.databricks.client.AzureWorkspaceManager;
import it.agilelab.witboost.provisioning.databricks.client.JobManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
import it.agilelab.witboost.provisioning.databricks.client.SkuType;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.*;
import it.agilelab.witboost.provisioning.databricks.permissions.AzurePermissionsManager;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureClient;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
public class WorkloadHandlerTest {

    @MockBean
    private AzureDatabricksManager azureDatabricksManager;

    @MockBean
    private AzureWorkspaceManager azureWorkspaceManager;

    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @Autowired
    private DatabricksAuthConfig databricksAuthConfig;

    @Autowired
    private AzureAuthConfig azureAuthConfig;

    @Autowired
    private GitCredentialsConfig gitCredentialsConfig;

    @Autowired
    private WorkloadHandler workloadHandler;

    @Mock
    private RepoManager repoManager;

    @Mock
    private JobManager jobManager;

    @MockBean
    private DatabricksWorkspaceClientBean databricksWorkspaceClientBean;

    @MockBean
    private AzureClient azureClient;

    @MockBean
    private AzureMapper azureMapper;

    @MockBean
    private AzurePermissionsManager azurePermissionsManager;

    @Mock
    WorkspaceClient workspaceClient;

    @MockBean
    private AzureResourceManager azureResourceManager;

    private DataProduct dataProduct;
    private Workload workload;
    private DatabricksWorkloadSpecific databricksWorkloadSpecific;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        dataProduct = new DataProduct();
        databricksWorkloadSpecific = new DatabricksWorkloadSpecific();
        workload = new Workload();
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
        String workspaceName = "testWorkspace";
        String region = "westeurope";
        String existingResourceGroupName = String.format(
                "/subscriptions/%s/resourceGroups/%s-rg", azurePermissionsConfig.getSubscriptionId(), workspaceName);
        String managedResourceGroupId = azurePermissionsConfig.getResourceGroup();
        SkuType skuType = SkuType.TRIAL;

        databricksWorkloadSpecific.setWorkspace(workspaceName);
        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitRepoUrl("repoUrl");
        gitSpecific.setGitReference("main");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific.setGitPath("/src");
        databricksWorkloadSpecific.setGit(gitSpecific);

        ClusterSpecific clusterSpecific = new ClusterSpecific();
        clusterSpecific.setSpotBidMaxPrice(10D);
        clusterSpecific.setFirstOnDemand(5L);
        clusterSpecific.setSpotInstances(true);
        clusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        clusterSpecific.setDriverNodeTypeId("driverNodeTypeId");
        clusterSpecific.setSparkConf(new HashMap<>());
        clusterSpecific.setSparkEnvVars(new HashMap<>());
        clusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        databricksWorkloadSpecific.setCluster(clusterSpecific);

        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setCronExpression("00 * * * * ?");
        schedulingSpecific.setJavaTimezoneId("UTC");
        databricksWorkloadSpecific.setScheduling(schedulingSpecific);

        workload.setSpecific(databricksWorkloadSpecific);

        dataProduct.setDataProductOwner("user:name.surname@company.it");

        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(workspaceName)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(region)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(existingResourceGroupName))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(managedResourceGroupId))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        try {
            when(databricksWorkspaceClientBean.getObject(anyString(), anyString()))
                    .thenReturn(workspaceClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo(workspaceName, "test", "test", "test");
        when(azureWorkspaceManager.createWorkspace(eq(workspaceName), eq(region), anyString(), anyString(), any()))
                .thenReturn(Either.right(databricksWorkspaceInfo));

        when(repoManager.createRepo(anyString(), eq(GitProvider.GITLAB))).thenReturn(Either.right(null));

        Map<String, Either<Throwable, String>> mockres = new HashMap<>();
        mockres.put(dataProduct.getDataProductOwner(), Either.right("azureId"));
        when(azureMapper.map(anySet())).thenReturn(mockres);

        when(azurePermissionsManager.assignPermissions(anyString(), anyString(), anyString(), anyString(), any()))
                .thenReturn(Either.right(null));
        GroupsAPI groupsAPI = mock(GroupsAPI.class);
        when(workspaceClient.groups()).thenReturn(groupsAPI);

        UsersAPI usersAPI = mock(UsersAPI.class);
        when(workspaceClient.users()).thenReturn(usersAPI);

        when(jobManager.createJobWithNewCluster(anyString(), anyString(), anyString(), any(), any(), any()))
                .thenReturn(Either.right(123L));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        CreateResponse createResponse = mock(CreateResponse.class);

        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.create(any())).thenReturn(createResponse);
        when(createResponse.getJobId()).thenReturn(123L);

        Either<FailedOperation, String> result = workloadHandler.provisionWorkload(provisionRequest);

        assert result.isRight();
    }

    @Test
    public void provisionWorkload_Failure() {
        String workspaceName = "testWorkspace";
        String region = "westeurope";
        String existingResourceGroupName = String.format(
                "/subscriptions/%s/resourceGroups/%s-rg", azurePermissionsConfig.getSubscriptionId(), workspaceName);
        String managedResourceGroupId = azurePermissionsConfig.getResourceGroup();
        SkuType skuType = SkuType.TRIAL;

        databricksWorkloadSpecific.setWorkspace(workspaceName);
        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitRepoUrl("repoUrl");
        databricksWorkloadSpecific.setGit(gitSpecific);
        workload.setSpecific(databricksWorkloadSpecific);

        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(workspaceName)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(region)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(existingResourceGroupName))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(managedResourceGroupId))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        try {
            when(databricksWorkspaceClientBean.getObject(anyString(), anyString()))
                    .thenReturn(workspaceClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(azureWorkspaceManager.createWorkspace(eq(workspaceName), eq(region), anyString(), anyString(), any()))
                .thenReturn(Either.left(failedOperation));

        Either<FailedOperation, String> result = workloadHandler.provisionWorkload(provisionRequest);

        assert result.isLeft();
        assert result.equals(Left(failedOperation));
    }

    @Test
    public void provisionWorkload_FailureToGetWorkspaceName() {
        databricksWorkloadSpecific.setWorkspace(null);

        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        Either<FailedOperation, String> result = workloadHandler.provisionWorkload(provisionRequest);

        assert result.isLeft();
        FailedOperation failedOperation = result.getLeft();
        assertEquals(1, failedOperation.problems().size());
        assertEquals(
                "Failed to get workspace name",
                failedOperation.problems().get(0).description());
    }

    @Test
    public void provisionWorkload_Exception() {
        ProvisionRequest<Specific> provisionRequest = new ProvisionRequest<>(dataProduct, new Workload(), false);

        Either<FailedOperation, String> result = workloadHandler.provisionWorkload(provisionRequest);

        assert result.isLeft();
        FailedOperation failedOperation = result.getLeft();
        assertEquals(1, failedOperation.problems().size());
    }

    @Test
    public void createRepository_Exception() {
        ProvisionRequest<Specific> provisionRequest = new ProvisionRequest<>(dataProduct, new Workload(), false);

        try {
            when(databricksWorkspaceClientBean.getObject(anyString(), anyString()))
                    .thenThrow(new Exception("Exception"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Either<FailedOperation, String> result = workloadHandler.provisionWorkload(provisionRequest);

        assert result.isLeft();
        FailedOperation failedOperation = result.getLeft();
        assertEquals(1, failedOperation.problems().size());
    }

    @Test
    public void unprovisionWorkloadRemoveDataFalse_Success() {
        String workspaceName = "testWorkspace";
        String region = "westeurope";
        String existingResourceGroupName = String.format(
                "/subscriptions/%s/resourceGroups/%s-rg", azurePermissionsConfig.getSubscriptionId(), workspaceName);
        String managedResourceGroupId = azurePermissionsConfig.getResourceGroup();
        SkuType skuType = SkuType.TRIAL;

        databricksWorkloadSpecific.setWorkspace(workspaceName);
        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitRepoUrl("repoUrl");
        gitSpecific.setGitReference("main");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific.setGitPath("/src");
        databricksWorkloadSpecific.setGit(gitSpecific);

        ClusterSpecific clusterSpecific = new ClusterSpecific();
        clusterSpecific.setSpotBidMaxPrice(10D);
        clusterSpecific.setFirstOnDemand(5L);
        clusterSpecific.setSpotInstances(true);
        clusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        clusterSpecific.setDriverNodeTypeId("driverNodeTypeId");
        clusterSpecific.setSparkConf(new HashMap<>());
        clusterSpecific.setSparkEnvVars(new HashMap<>());
        clusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        databricksWorkloadSpecific.setCluster(clusterSpecific);

        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setCronExpression("00 * * * * ?");
        schedulingSpecific.setJavaTimezoneId("UTC");
        databricksWorkloadSpecific.setScheduling(schedulingSpecific);

        workload.setSpecific(databricksWorkloadSpecific);

        dataProduct.setDataProductOwner("user:name.surname@company.it");

        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(workspaceName)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(region)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(existingResourceGroupName))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(managedResourceGroupId))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        try {
            when(databricksWorkspaceClientBean.getObject(anyString(), anyString()))
                    .thenReturn(workspaceClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo(workspaceName, "test", "test", "test");

        Optional<DatabricksWorkspaceInfo> optionalDatabricksWorkspaceInfo = Optional.of(databricksWorkspaceInfo);

        when(azureWorkspaceManager.getWorkspace(eq(workspaceName), anyString()))
                .thenReturn(Either.right(optionalDatabricksWorkspaceInfo));

        Iterable<BaseJob> iterable = mock(Iterable.class);
        when(jobManager.listJobsWithGivenName(databricksWorkloadSpecific.getJobName()))
                .thenReturn(Either.right(iterable));
        when(jobManager.deleteJob(any())).thenReturn(Either.right(null));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        CreateResponse createResponse = mock(CreateResponse.class);

        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.create(any())).thenReturn(createResponse);
        when(createResponse.getJobId()).thenReturn(123L);

        Either<FailedOperation, Void> result = workloadHandler.unprovisionWorkload(provisionRequest);

        assert result.isRight();
    }

    @Test
    public void unprovisionWorkloadRemoveDataTrue_Success() {
        String workspaceName = "testWorkspace";
        String region = "westeurope";
        String existingResourceGroupName = String.format(
                "/subscriptions/%s/resourceGroups/%s-rg", azurePermissionsConfig.getSubscriptionId(), workspaceName);
        String managedResourceGroupId = azurePermissionsConfig.getResourceGroup();
        SkuType skuType = SkuType.TRIAL;

        databricksWorkloadSpecific.setWorkspace(workspaceName);
        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitRepoUrl("repoUrl");
        gitSpecific.setGitReference("main");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific.setGitPath("/src");
        databricksWorkloadSpecific.setGit(gitSpecific);

        ClusterSpecific clusterSpecific = new ClusterSpecific();
        clusterSpecific.setSpotBidMaxPrice(10D);
        clusterSpecific.setFirstOnDemand(5L);
        clusterSpecific.setSpotInstances(true);
        clusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        clusterSpecific.setDriverNodeTypeId("driverNodeTypeId");
        clusterSpecific.setSparkConf(new HashMap<>());
        clusterSpecific.setSparkEnvVars(new HashMap<>());
        clusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        databricksWorkloadSpecific.setCluster(clusterSpecific);

        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setCronExpression("00 * * * * ?");
        schedulingSpecific.setJavaTimezoneId("UTC");
        databricksWorkloadSpecific.setScheduling(schedulingSpecific);

        workload.setSpecific(databricksWorkloadSpecific);

        dataProduct.setDataProductOwner("user:name.surname@company.it");

        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, true);

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(workspaceName)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(region)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(existingResourceGroupName))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(managedResourceGroupId))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        try {
            when(databricksWorkspaceClientBean.getObject(anyString(), anyString()))
                    .thenReturn(workspaceClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo(workspaceName, "test", "test", "test");

        Optional<DatabricksWorkspaceInfo> optionalDatabricksWorkspaceInfo = Optional.of(databricksWorkspaceInfo);

        when(azureWorkspaceManager.getWorkspace(eq(workspaceName), anyString()))
                .thenReturn(Either.right(optionalDatabricksWorkspaceInfo));

        Iterable<BaseJob> iterable = mock(Iterable.class);
        when(jobManager.listJobsWithGivenName(databricksWorkloadSpecific.getJobName()))
                .thenReturn(Either.right(iterable));
        when(jobManager.deleteJob(any())).thenReturn(Either.right(null));

        JobsAPI jobsAPI = mock(JobsAPI.class);
        CreateResponse createResponse = mock(CreateResponse.class);

        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.create(any())).thenReturn(createResponse);
        when(createResponse.getJobId()).thenReturn(123L);

        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);

        when(repoManager.deleteRepo(anyString(), anyString())).thenReturn(Either.right(null));

        Either<FailedOperation, Void> result = workloadHandler.unprovisionWorkload(provisionRequest);

        assert result.isRight();
    }
}
