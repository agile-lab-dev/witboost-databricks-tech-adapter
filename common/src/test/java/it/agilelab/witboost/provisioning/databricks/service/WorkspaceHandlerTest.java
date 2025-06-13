package it.agilelab.witboost.provisioning.databricks.service;

import static io.vavr.API.Left;
import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.authorization.fluent.models.RoleAssignmentInner;
import com.azure.resourcemanager.authorization.models.PrincipalType;
import com.azure.resourcemanager.databricks.AzureDatabricksManager;
import com.azure.resourcemanager.databricks.implementation.WorkspaceImpl;
import com.azure.resourcemanager.databricks.implementation.WorkspacesImpl;
import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.compute.AzureAvailability;
import com.databricks.sdk.service.compute.RuntimeEngine;
import com.databricks.sdk.service.iam.GroupsAPI;
import com.databricks.sdk.service.iam.UsersAPI;
import com.databricks.sdk.service.jobs.Job;
import com.databricks.sdk.service.jobs.JobSettings;
import com.databricks.sdk.service.pipelines.PipelineClusterAutoscaleMode;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.WorkspaceClientConfig;
import it.agilelab.witboost.provisioning.databricks.client.AzureWorkspaceManager;
import it.agilelab.witboost.provisioning.databricks.client.SkuType;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.*;
import it.agilelab.witboost.provisioning.databricks.model.*;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.SparkConf;
import it.agilelab.witboost.provisioning.databricks.model.databricks.outputport.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.SparkEnvVar;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt.DLTClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt.DatabricksDLTWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.DatabricksJobWorkloadSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job.JobClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.permissions.AzurePermissionsManager;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureClient;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureMapper;
import java.util.*;
import java.util.function.Function;
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
    private Function<WorkspaceClientConfig.WorkspaceClientConfigParams, WorkspaceClient> workspaceClientFactory;

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
    private DatabricksJobWorkloadSpecific databricksJobWorkloadSpecific;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        dataProduct = new DataProduct();
        databricksJobWorkloadSpecific = new DatabricksJobWorkloadSpecific();
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
    public void provisionWorkspaceJob_Success() {

        azureAuthConfig.setSkuType("PREMIUM");
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest = createJobProvisionRequest();

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);
        when(azureWorkspaceManager.getWorkspace(eq("testWorkspace"), anyString()))
                .thenReturn(right(Optional.empty()));

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);
        when(workspaceClientFactory.apply(any(WorkspaceClientConfig.WorkspaceClientConfigParams.class)))
                .thenReturn(workspaceClient);

        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(
                "testWorkspace", "test", "test", "test", "test", ProvisioningState.SUCCEEDED);
        when(azureWorkspaceManager.createIfNotExistsWorkspace(
                        eq("testWorkspace"), eq("westeurope"), anyString(), anyString(), any()))
                .thenReturn(right(databricksWorkspaceInfo));

        Map<String, Either<Throwable, String>> mockres = new HashMap<>();
        mockres.put(dataProduct.getDataProductOwner(), right("azureId"));
        mockres.put(dataProduct.getDevGroup(), right("azureGroupId"));
        when(azureMapper.map(anySet())).thenReturn(mockres);

        when(azurePermissionsManager.assignPermissions(anyString(), anyString(), anyString(), anyString(), any()))
                .thenReturn(right(null));
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
    public void provisionWorkspace_DLT_Success() {

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest = createDLTProvisionRequest();

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);
        when(azureWorkspaceManager.getWorkspace(eq("testWorkspace"), anyString()))
                .thenReturn(right(Optional.empty()));

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);
        when(workspaceClientFactory.apply(any(WorkspaceClientConfig.WorkspaceClientConfigParams.class)))
                .thenReturn(workspaceClient);

        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(
                "testWorkspace", "test", "test", "test", "test", ProvisioningState.SUCCEEDED);
        when(azureWorkspaceManager.createIfNotExistsWorkspace(
                        eq("testWorkspace"), eq("westeurope"), anyString(), anyString(), any()))
                .thenReturn(right(databricksWorkspaceInfo));

        Map<String, Either<Throwable, String>> mockres = new HashMap<>();
        mockres.put(dataProduct.getDataProductOwner(), right("azureId"));
        mockres.put(dataProduct.getDevGroup(), right("azureGroupId"));
        when(azureMapper.map(anySet())).thenReturn(mockres);

        when(azurePermissionsManager.assignPermissions(anyString(), anyString(), anyString(), anyString(), any()))
                .thenReturn(right(null));
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
    public void provisionWorkspace_Workflow_Success() {

        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest = createWorkflowProvisionRequest();

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);
        when(azureWorkspaceManager.getWorkspace(eq("testWorkspace"), anyString()))
                .thenReturn(right(Optional.empty()));

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);
        when(workspaceClientFactory.apply(any(WorkspaceClientConfig.WorkspaceClientConfigParams.class)))
                .thenReturn(workspaceClient);

        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(
                "testWorkspace", "test", "test", "test", "test", ProvisioningState.SUCCEEDED);
        when(azureWorkspaceManager.createIfNotExistsWorkspace(
                        eq("testWorkspace"), eq("westeurope"), anyString(), anyString(), any()))
                .thenReturn(right(databricksWorkspaceInfo));

        Map<String, Either<Throwable, String>> mockres = new HashMap<>();
        mockres.put(dataProduct.getDataProductOwner(), right("azureId"));
        mockres.put(dataProduct.getDevGroup(), right("azureGroupId"));
        when(azureMapper.map(anySet())).thenReturn(mockres);

        when(azurePermissionsManager.assignPermissions(anyString(), anyString(), anyString(), anyString(), any()))
                .thenReturn(right(null));
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
    public void provisionWorkspace_OutputPort_Success() {

        ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest = createOPProvisionRequest();

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);
        when(azureWorkspaceManager.getWorkspace(eq("testWorkspace"), anyString()))
                .thenReturn(right(Optional.empty()));

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);
        when(workspaceClientFactory.apply(any(WorkspaceClientConfig.WorkspaceClientConfigParams.class)))
                .thenReturn(workspaceClient);

        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(
                "testWorkspace", "test", "test", "test", "test", ProvisioningState.SUCCEEDED);
        when(azureWorkspaceManager.createIfNotExistsWorkspace(
                        eq("testWorkspace"), eq("westeurope"), anyString(), anyString(), any()))
                .thenReturn(right(databricksWorkspaceInfo));

        Map<String, Either<Throwable, String>> mockres = new HashMap<>();
        mockres.put(dataProduct.getDataProductOwner(), right("azureId"));
        mockres.put(dataProduct.getDevGroup(), right("azureGroupId"));
        when(azureMapper.map(anySet())).thenReturn(mockres);

        when(azurePermissionsManager.assignPermissions(anyString(), anyString(), anyString(), anyString(), any()))
                .thenReturn(right(null));
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
    public void provisionWorkspace_WrongSpecificType() {

        workload.setSpecific(new Specific());
        ProvisionRequest<Specific> provisionRequest = new ProvisionRequest<>(dataProduct, workload, false);

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);
        when(azureWorkspaceManager.getWorkspace(eq("testWorkspace"), anyString()))
                .thenReturn(right(Optional.empty()));

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);
        when(workspaceClientFactory.apply(any(WorkspaceClientConfig.WorkspaceClientConfigParams.class)))
                .thenReturn(workspaceClient);

        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(
                "testWorkspace", "test", "test", "test", "test", ProvisioningState.SUCCEEDED);
        when(azureWorkspaceManager.createIfNotExistsWorkspace(
                        eq("testWorkspace"), eq("westeurope"), anyString(), anyString(), any()))
                .thenReturn(right(databricksWorkspaceInfo));

        Map<String, Either<Throwable, String>> mockres = new HashMap<>();
        mockres.put(dataProduct.getDataProductOwner(), right("azureId"));
        when(azureMapper.map(anySet())).thenReturn(mockres);

        when(azurePermissionsManager.assignPermissions(anyString(), anyString(), anyString(), anyString(), any()))
                .thenReturn(right(null));
        GroupsAPI groupsAPI = mock(GroupsAPI.class);
        when(workspaceClient.groups()).thenReturn(groupsAPI);

        UsersAPI usersAPI = mock(UsersAPI.class);
        when(workspaceClient.users()).thenReturn(usersAPI);

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceHandler.provisionWorkspace(provisionRequest);

        assert result.isLeft();
        assertTrue(
                result.getLeft()
                        .problems()
                        .get(0)
                        .description()
                        .contains(
                                "The specific section of the component 'null' is not a valid type. Only the following types are accepted: DatabricksJobWorkloadSpecific, DatabricksDLTWorkloadSpecific, DatabricksOutputPortSpecific, DatabricksWorkflowWorkloadSpecific"));
    }

    @Test
    public void provisionWorkspace_AzureMapperException() {

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest = createJobProvisionRequest();

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);
        when(workspaceClientFactory.apply(any(WorkspaceClientConfig.WorkspaceClientConfigParams.class)))
                .thenReturn(workspaceClient);

        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(
                "testWorkspace", "test", "test", "test", "test", ProvisioningState.SUCCEEDED);
        when(azureWorkspaceManager.getWorkspace(eq("testWorkspace"), anyString()))
                .thenReturn(right(Optional.empty()));
        when(azureWorkspaceManager.createIfNotExistsWorkspace(
                        eq("testWorkspace"), eq("westeurope"), anyString(), anyString(), any()))
                .thenReturn(right(databricksWorkspaceInfo));

        Map<String, Either<Throwable, String>> mockres = new HashMap<>();
        mockres.put(dataProduct.getDataProductOwner(), Either.left(new Throwable("Error")));
        when(azureMapper.map(anySet())).thenReturn(mockres);

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceHandler.provisionWorkspace(provisionRequest);

        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains("Failed to get AzureID of: user:name.surname@company.it.");
    }

    @Test
    public void provisionWorkspace_Failure() {
        String workspaceName = "testWorkspace";
        String region = "westeurope";
        String existingResourceGroupName = String.format(
                "/subscriptions/%s/resourceGroups/%s-rg", azurePermissionsConfig.getSubscriptionId(), workspaceName);
        String managedResourceGroupId = azurePermissionsConfig.getResourceGroup();

        databricksJobWorkloadSpecific.setWorkspace(workspaceName);
        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                new DatabricksJobWorkloadSpecific.JobGitSpecific();
        jobGitSpecific.setGitRepoUrl("repoUrl");
        databricksJobWorkloadSpecific.setGit(jobGitSpecific);

        workload.setSpecific(databricksJobWorkloadSpecific);

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);
        when(azureWorkspaceManager.getWorkspace(eq("testWorkspace"), anyString()))
                .thenReturn(right(Optional.empty()));

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(workspaceName)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(region)).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(existingResourceGroupName))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(managedResourceGroupId))
                .thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);
        when(workspaceClientFactory.apply(any(WorkspaceClientConfig.WorkspaceClientConfigParams.class)))
                .thenReturn(workspaceClient);

        var failedOperation = new FailedOperation(Collections.singletonList(new Problem("error")));
        when(azureWorkspaceManager.createIfNotExistsWorkspace(
                        eq(workspaceName), eq(region), anyString(), anyString(), any()))
                .thenReturn(Either.left(failedOperation));

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceHandler.provisionWorkspace(provisionRequest);

        assert result.isLeft();
        assert result.equals(Left(failedOperation));
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

        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(
                "testWorkspace", "test", "test", "test", "test", ProvisioningState.SUCCEEDED);

        when(workspaceClientFactory.apply(any(WorkspaceClientConfig.WorkspaceClientConfigParams.class)))
                .thenReturn(workspaceClient);

        Either<FailedOperation, WorkspaceClient> result = workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);

        assert result.isRight();
        assertEquals(result.get(), workspaceClient);
    }

    @Test
    public void getWorkspaceClient_Failure() throws Exception {

        String errorMessage = "This is an exception";
        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(
                "testWorkspace", "test", "test", "test", "test", ProvisioningState.SUCCEEDED);

        when(workspaceClientFactory.apply(any(WorkspaceClientConfig.WorkspaceClientConfigParams.class)))
                .thenThrow(new RuntimeException(errorMessage));

        Either<FailedOperation, WorkspaceClient> result = workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);

        assert result.isLeft();
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void testGetWorkspaceInfo_Success() {
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest = createJobProvisionRequest();

        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(
                "testWorkspace", "test", "test", "test", "test", ProvisioningState.SUCCEEDED);

        when(azureWorkspaceManager.getWorkspace(anyString(), anyString()))
                .thenReturn(right(Optional.of(databricksWorkspaceInfo)));

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> result =
                workspaceHandler.getWorkspaceInfo(provisionRequest);

        assertTrue(result.isRight());
        assertTrue(result.get().isPresent());
        assertEquals(databricksWorkspaceInfo, result.get().get());
        verify(azureWorkspaceManager, times(1))
                .getWorkspace("testWorkspace", "/subscriptions/testSubscriptionId/resourceGroups/testWorkspace-rg");
    }

    @Test
    public void testHandleNoPermissions_Success() {
        DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
                "testWorkspace", "test", "test", "test", "test", ProvisioningState.SUCCEEDED);
        RoleAssignmentInner roleAssignmentInner = mock(RoleAssignmentInner.class);
        when(roleAssignmentInner.id()).thenReturn("/subscriptions/test/workspaces/testWorkspace");

        when(azurePermissionsManager.getPrincipalRoleAssignmentsOnResource(
                        anyString(), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(right(List.of(roleAssignmentInner)));

        when(azurePermissionsManager.deleteRoleAssignment(anyString(), anyString()))
                .thenReturn(right(null));
        Either<FailedOperation, Void> result = workspaceHandler.handleNoPermissions(workspaceInfo, "testEntityId");

        assertTrue(result.isRight());
        verify(azurePermissionsManager, times(1)).deleteRoleAssignment(eq("testWorkspace"), anyString());
    }

    @Test
    public void testManageAzurePermissions_ExceptionHandling() {
        DatabricksWorkspaceInfo workspaceInfo = new DatabricksWorkspaceInfo(
                "testWorkspace", "test", "test", "test", "test", ProvisioningState.SUCCEEDED);

        when(azureMapper.map(anySet())).thenThrow(new RuntimeException("Mapping error"));

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest = createJobProvisionRequest();

        Either<FailedOperation, Void> result = workspaceHandler.manageAzurePermissions(
                workspaceInfo, "entityName", "roleDefinitionId", PrincipalType.USER);

        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while handling permissions of entityName for the Azure resource testWorkspace. Details: Mapping error",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testCreateDatabricksWorkspace_ExceptionHandling() {
        when(azureWorkspaceManager.createIfNotExistsWorkspace(
                        anyString(), anyString(), anyString(), anyString(), any(SkuType.class)))
                .thenThrow(new RuntimeException("Workspace creation failed"));

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest = createJobProvisionRequest();

        Either<FailedOperation, DatabricksWorkspaceInfo> result =
                workspaceHandler.createIfNotExistsDatabricksWorkspace(provisionRequest);

        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while creating workspace for component null. Please try again and if the error persists contact the platform team. Details: Workspace creation failed",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void provisionWorkspace_NotManagedWorkspace_Success() {

        azureAuthConfig.setSkuType("PREMIUM");
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest = createJobProvisionRequest();
        provisionRequest.component().getSpecific().setWorkspace("adb-123456789012.12.azuredatabricks.net");

        WorkspacesImpl mockWorkspaces = mock(WorkspacesImpl.class);
        when(azureDatabricksManager.workspaces()).thenReturn(mockWorkspaces);

        WorkspaceImpl mockWorkspaceImpl = mock(WorkspaceImpl.class);
        when(mockWorkspaces.define(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withRegion(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withExistingResourceGroup(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withManagedResourceGroupId(anyString())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.withSku(any())).thenReturn(mockWorkspaceImpl);
        when(mockWorkspaceImpl.create()).thenReturn(mockWorkspaceImpl);
        when(workspaceClientFactory.apply(any(WorkspaceClientConfig.WorkspaceClientConfigParams.class)))
                .thenReturn(workspaceClient);

        DatabricksWorkspaceInfo databricksWorkspaceInfo = new DatabricksWorkspaceInfo(
                "adb-123456789012.12.azuredatabricks.net", "123456789012", null, "test", ProvisioningState.SUCCEEDED);

        Map<String, Either<Throwable, String>> mockres = new HashMap<>();
        mockres.put(dataProduct.getDataProductOwner(), right("azureId"));
        mockres.put(dataProduct.getDevGroup(), right("azureGroupId"));
        when(azureMapper.map(anySet())).thenReturn(mockres);

        Either<FailedOperation, DatabricksWorkspaceInfo> result = workspaceHandler.provisionWorkspace(provisionRequest);

        assertTrue(result.isRight());
        assertEquals(result.get().getName(), databricksWorkspaceInfo.getName());
        assertEquals(result.get().getId(), databricksWorkspaceInfo.getId());
    }

    private ProvisionRequest<DatabricksJobWorkloadSpecific> createJobProvisionRequest() {
        String workspaceName = "testWorkspace";
        String region = "westeurope";
        String existingResourceGroupName = String.format(
                "/subscriptions/%s/resourceGroups/%s-rg", azurePermissionsConfig.getSubscriptionId(), workspaceName);
        String managedResourceGroupId = azurePermissionsConfig.getResourceGroup();

        databricksJobWorkloadSpecific.setWorkspace(workspaceName);
        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                new DatabricksJobWorkloadSpecific.JobGitSpecific();
        jobGitSpecific.setGitRepoUrl("repoUrl");
        jobGitSpecific.setGitReference("main");
        jobGitSpecific.setGitReferenceType(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);
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
        SparkEnvVar sparkEnvVar = new SparkEnvVar("spark.env.var", "value");
        jobClusterSpecific.setSparkEnvVarsProduction(List.of(sparkEnvVar));
        jobClusterSpecific.setSparkEnvVarsQa(List.of(sparkEnvVar));
        jobClusterSpecific.setSparkEnvVarsDevelopment(List.of(sparkEnvVar));
        jobClusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        databricksJobWorkloadSpecific.setCluster(jobClusterSpecific);

        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific =
                new DatabricksJobWorkloadSpecific.SchedulingSpecific();
        schedulingSpecific.setCronExpression("00 * * * * ?");
        schedulingSpecific.setJavaTimezoneId("UTC");
        databricksJobWorkloadSpecific.setScheduling(schedulingSpecific);

        workload.setSpecific(databricksJobWorkloadSpecific);

        dataProduct.setDataProductOwner("user:name.surname@company.it");
        dataProduct.setDevGroup("group:devGroup");

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        return provisionRequest;
    }

    private ProvisionRequest<DatabricksDLTWorkloadSpecific> createDLTProvisionRequest() {

        DLTClusterSpecific cluster = new DLTClusterSpecific();
        cluster.setMode(PipelineClusterAutoscaleMode.LEGACY);
        cluster.setWorkerType("Standard_DS3_v2");
        cluster.setDriverType("Standard_DS3_v2");
        cluster.setPolicyId("policyId");

        DatabricksDLTWorkloadSpecific specific = new DatabricksDLTWorkloadSpecific();
        specific.setWorkspace("testWorkspace");
        specific.setPipelineName("pipelineName");
        specific.setProductEdition(DatabricksDLTWorkloadSpecific.ProductEdition.CORE);
        specific.setContinuous(true);
        specific.setNotebooks(List.of("notebook1", "notebook2"));
        specific.setFiles(List.of("file1", "file2"));
        specific.setMetastore("metastore");
        specific.setCatalog("catalog");
        specific.setTarget("target");
        specific.setPhoton(true);
        List notifications = new ArrayList();
        notifications.add(new DatabricksDLTWorkloadSpecific.PipelineNotification(
                "email@email.com", Collections.singletonList("on-update-test")));
        specific.setChannel(DatabricksDLTWorkloadSpecific.PipelineChannel.CURRENT);
        specific.setCluster(cluster);

        DatabricksWorkflowWorkloadSpecific.GitSpecific dltGitSpecific =
                new DatabricksWorkflowWorkloadSpecific.GitSpecific();
        dltGitSpecific.setGitRepoUrl("https://github.com/repo.git");
        specific.setGit(dltGitSpecific);

        workload.setSpecific(specific);

        dataProduct.setDataProductOwner("user:name.surname@company.it");
        dataProduct.setDevGroup("group:devGroup");

        ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        return provisionRequest;
    }

    private ProvisionRequest<DatabricksWorkflowWorkloadSpecific> createWorkflowProvisionRequest() {
        String workspaceName = "testWorkspace";
        DatabricksWorkflowWorkloadSpecific databricksWorkflowWorkloadSpecific =
                new DatabricksWorkflowWorkloadSpecific();
        databricksWorkflowWorkloadSpecific.setWorkspace(workspaceName);
        databricksWorkflowWorkloadSpecific.setRepoPath("this/is/a/repo");

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

        ProvisionRequest<DatabricksWorkflowWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        return provisionRequest;
    }

    private ProvisionRequest<DatabricksOutputPortSpecific> createOPProvisionRequest() {

        DatabricksOutputPortSpecific databricksOutputPortSpecific = new DatabricksOutputPortSpecific();
        databricksOutputPortSpecific.setWorkspace("testWorkspace");
        databricksOutputPortSpecific.setMetastore("metastore");
        databricksOutputPortSpecific.setCatalogName("catalog");
        databricksOutputPortSpecific.setSchemaName("schema");
        databricksOutputPortSpecific.setTableName("t");
        databricksOutputPortSpecific.setSqlWarehouseName("sql_wh");
        databricksOutputPortSpecific.setWorkspaceOP("testWorkspace");
        databricksOutputPortSpecific.setCatalogNameOP("catalog_op");
        databricksOutputPortSpecific.setSchemaNameOP("schema_op");
        databricksOutputPortSpecific.setViewNameOP("view");

        OutputPort outputPort = new OutputPort();
        outputPort.setName("testOP");
        outputPort.setSpecific(databricksOutputPortSpecific);
        dataProduct.setDataProductOwner("user:name.surname@company.it");
        dataProduct.setDevGroup("group:developers");

        ProvisionRequest provisionRequest = new ProvisionRequest<>(dataProduct, outputPort, false);
        return provisionRequest;
    }
}
