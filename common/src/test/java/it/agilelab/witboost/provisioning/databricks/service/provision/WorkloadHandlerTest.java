package it.agilelab.witboost.provisioning.databricks.service.provision;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.AzureResourceManager;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.compute.AzureAvailability;
import com.databricks.sdk.service.compute.RuntimeEngine;
import com.databricks.sdk.service.jobs.BaseJob;
import com.databricks.sdk.service.jobs.CreateResponse;
import com.databricks.sdk.service.jobs.Job;
import com.databricks.sdk.service.jobs.JobsAPI;
import com.databricks.sdk.service.workspace.ObjectInfo;
import com.databricks.sdk.service.workspace.RepoInfo;
import com.databricks.sdk.service.workspace.ReposAPI;
import com.databricks.sdk.service.workspace.WorkspaceAPI;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.model.DataProduct;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.Specific;
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.*;
import java.util.HashMap;
import java.util.Iterator;
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
    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @Autowired
    private WorkloadHandler workloadHandler;

    @Mock
    WorkspaceClient workspaceClient;

    @MockBean
    private AzureResourceManager azureResourceManager;

    private DataProduct dataProduct;
    private Workload workload;
    private DatabricksWorkloadSpecific databricksWorkloadSpecific;

    private DatabricksWorkspaceInfo workspaceInfo =
            new DatabricksWorkspaceInfo("workspace", "123", "https://example.com", "abc", "test");
    private String workspaceName = "testWorkspace";
    private String existingResourceGroupName;
    private String managedResourceGroupId;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        dataProduct = new DataProduct();
        databricksWorkloadSpecific = new DatabricksWorkloadSpecific();
        workload = new Workload();

        existingResourceGroupName = String.format(
                "/subscriptions/%s/resourceGroups/%s-rg", azurePermissionsConfig.getSubscriptionId(), workspaceName);
        managedResourceGroupId = azurePermissionsConfig.getResourceGroup();

        databricksWorkloadSpecific.setWorkspace(workspaceName);
        databricksWorkloadSpecific.setJobName("jobName");
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
        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);
        mockJobAPI(workspaceClient);

        Either<FailedOperation, String> result =
                workloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        assertEquals(result.get(), "123");
    }

    @Test
    public void unprovisionWorkloadRemoveDataFalse_Success() {

        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo(workspaceName, "test", "test", "test", "test");

        Iterable<BaseJob> iterable = mock(Iterable.class);
        Iterator<BaseJob> iterator = mock(Iterator.class);
        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any())).thenReturn(iterable);
        when(iterable.iterator()).thenReturn(iterator);

        BaseJob job1 = mock(BaseJob.class);
        BaseJob job2 = mock(BaseJob.class);

        when(iterator.hasNext()).thenReturn(true, true, false);
        when(iterator.next()).thenReturn(job1, job2);

        Either<FailedOperation, Void> result =
                workloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
    }

    @Test
    public void unprovisionWorkloadRemoveDataTrue_Success() {

        ProvisionRequest<DatabricksWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, true);

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo(workspaceName, "test", "test", "test", "test");

        Optional<DatabricksWorkspaceInfo> optionalDatabricksWorkspaceInfo = Optional.of(databricksWorkspaceInfo);

        Iterable<BaseJob> iterable = mock(Iterable.class);
        JobsAPI jobsAPI = mock(JobsAPI.class);
        when(workspaceClient.jobs()).thenReturn(jobsAPI);
        when(jobsAPI.list(any())).thenReturn(iterable);

        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        Iterable<ObjectInfo> objectInfos = mock(Iterable.class);
        when(workspaceAPI.list(anyString())).thenReturn(objectInfos);
        RepoInfo repoInfo = mock(RepoInfo.class);
        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        when(reposAPI.get(anyLong())).thenReturn(repoInfo);

        Either<FailedOperation, Void> result =
                workloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
    }

    @Test
    public void provisionWorkload_Exception() {
        ProvisionRequest<Specific> provisionRequest = new ProvisionRequest<>(dataProduct, new Workload(), false);
        try {
            workloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);
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
}
