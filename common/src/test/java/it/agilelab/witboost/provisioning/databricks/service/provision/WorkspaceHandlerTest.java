package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.API.Left;
import static org.junit.jupiter.api.Assertions.*;
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
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.DatabricksWorkspaceClientBean;
import it.agilelab.witboost.provisioning.databricks.client.AzureWorkspaceManager;
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
public class WorkspaceHandlerTest {
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
    private WorkspaceHandler workspaceHandler;

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
    public void provisionWorkspace_Success() {

        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest = createProvisionRequest();

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        try {
            when(databricksWorkspaceClientBean.getObject(anyString(), anyString()))
                    .thenReturn(workspaceClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo("testWorkspace", "test", "test", "test", "test");
        when(azureWorkspaceManager.createWorkspace(
                        eq("testWorkspace"), eq("westeurope"), anyString(), anyString(), any()))
                .thenReturn(Either.right(databricksWorkspaceInfo));

        Map<String, Either<Throwable, String>> mockres = new HashMap<>();
        mockres.put(dataProduct.getDataProductOwner(), Either.right("azureId"));
        when(azureMapper.map(anySet())).thenReturn(mockres);

        when(azurePermissionsManager.assignPermissions(anyString(), anyString(), anyString(), anyString(), any()))
                .thenReturn(Either.right(null));
        GroupsAPI groupsAPI = mock(GroupsAPI.class);
        when(workspaceClient.groups()).thenReturn(groupsAPI);

        UsersAPI usersAPI = mock(UsersAPI.class);
        when(workspaceClient.users()).thenReturn(usersAPI);

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceHandler.provisionWorkspace(provisionRequest);

        assert result.isRight();
        assertEquals(result.get().getName(), databricksWorkspaceInfo.getName());
        assertEquals(result.get().getAzureResourceId(), databricksWorkspaceInfo.getAzureResourceId());
    }

    @Test
    public void provisionWorkspace_AzureMapperException() {

        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest = createProvisionRequest();

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);

        WorkspaceClient workspaceClient = mock(WorkspaceClient.class);
        try {
            when(databricksWorkspaceClientBean.getObject(anyString(), anyString()))
                    .thenReturn(workspaceClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo("testWorkspace", "test", "test", "test", "test");
        when(azureWorkspaceManager.createWorkspace(
                        eq("testWorkspace"), eq("westeurope"), anyString(), anyString(), any()))
                .thenReturn(Either.right(databricksWorkspaceInfo));

        Map<String, Either<Throwable, String>> mockres = new HashMap<>();
        mockres.put(dataProduct.getDataProductOwner(), Either.left(new Throwable("Error")));
        when(azureMapper.map(anySet())).thenReturn(mockres);

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceHandler.provisionWorkspace(provisionRequest);

        var expected = new FailedOperation(Collections.singletonList(new Problem("Failed to map user: Error")));

        assert result.isLeft();
        assert result.getLeft().equals(expected);
    }

    @Test
    public void provisionWorkspace_Failure() {
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

        try {
            when(databricksWorkspaceClientBean.getObject(anyString(), anyString()))
                    .thenReturn(workspaceClient);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(azureWorkspaceManager.createWorkspace(eq(workspaceName), eq(region), anyString(), anyString(), any()))
                .thenReturn(Either.left(failedOperation));

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceHandler.provisionWorkspace(provisionRequest);

        assert result.isLeft();
        assert result.equals(Left(failedOperation));
    }

    @Test
    public void provisionWorkspace_FailureToGetWorkspaceName() {
        databricksWorkloadSpecific.setWorkspace(null);

        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceHandler.provisionWorkspace(provisionRequest);

        assert result.isLeft();
        FailedOperation failedOperation = result.getLeft();
        assertEquals(1, failedOperation.problems().size());
        assertEquals(
                "Failed to get workspace name",
                failedOperation.problems().get(0).description());
    }

    @Test
    public void provisionWorkspace_Exception() {
        ProvisionRequest<Specific> provisionRequest = new ProvisionRequest<>(dataProduct, new Workload(), false);

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceHandler.provisionWorkspace(provisionRequest);

        assert result.isLeft();
        FailedOperation failedOperation = result.getLeft();
        assertEquals(1, failedOperation.problems().size());
    }

    @Test
    public void getWorkspaceClient_Success() throws Exception {

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo("testWorkspace", "test", "test", "test", "test");

        when(databricksWorkspaceClientBean.getObject(anyString(), anyString())).thenReturn(workspaceClient);

        Either<FailedOperation, WorkspaceClient> result = workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);

        assert result.isRight();
        assertEquals(result.get(), workspaceClient);
    }

    @Test
    public void getWorkspaceClient_Failure() throws Exception {

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo("testWorkspace", "test", "test", "test", "test");

        when(databricksWorkspaceClientBean.getObject(anyString(), anyString())).thenThrow(new Exception("Exception"));

        Either<FailedOperation, WorkspaceClient> result = workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);

        assert result.isLeft();
        assertEquals(result.getLeft(), new FailedOperation(Collections.singletonList(new Problem("Exception"))));
    }

    @Test
    public void testGetWorkspaceInfo_Success() {
        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest = createProvisionRequest();

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo("testWorkspace", "test", "test", "test", "test");

        when(azureWorkspaceManager.getWorkspace(anyString(), anyString()))
                .thenReturn(Either.right(Optional.of(databricksWorkspaceInfo)));

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> result =
                workspaceHandler.getWorkspaceInfo(provisionRequest);

        assertTrue(result.isRight());
        assertTrue(result.get().isPresent());
        assertEquals(databricksWorkspaceInfo, result.get().get());
        verify(azureWorkspaceManager, times(1))
                .getWorkspace("testWorkspace", "/subscriptions/testSubscriptionId/resourceGroups/testWorkspace-rg");
    }

    private ProvisionRequest<DatabricksWorkloadSpecific> createProvisionRequest() {
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

        return provisionRequest;
    }
}
