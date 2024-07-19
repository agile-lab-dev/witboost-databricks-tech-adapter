package it.agilelab.witboost.provisioning.databricks.service.provision;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.AzureResourceManager;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
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
import it.agilelab.witboost.provisioning.databricks.model.Workload;
import it.agilelab.witboost.provisioning.databricks.model.databricks.*;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.*;
import java.util.*;
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
public class JobWorkloadHandlerTest {
    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @Autowired
    private JobWorkloadHandler jobWorkloadHandler;

    @Mock
    WorkspaceClient workspaceClient;

    @MockBean
    private AzureResourceManager azureResourceManager;

    private DataProduct dataProduct;
    private Workload workload;
    private DatabricksJobWorkloadSpecific databricksJobWorkloadSpecific;

    private DatabricksWorkspaceInfo workspaceInfo =
            new DatabricksWorkspaceInfo("workspace", "123", "https://example.com", "abc", "test");
    private String workspaceName = "testWorkspace";

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        dataProduct = new DataProduct();
        databricksJobWorkloadSpecific = new DatabricksJobWorkloadSpecific();
        workload = new Workload();

        databricksJobWorkloadSpecific.setWorkspace(workspaceName);
        databricksJobWorkloadSpecific.setJobName("jobName");
        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitRepoUrl("repoUrl");
        gitSpecific.setGitReference("main");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific.setGitPath("/src");
        databricksJobWorkloadSpecific.setGit(gitSpecific);

        JobClusterSpecific jobClusterSpecific = new JobClusterSpecific();
        jobClusterSpecific.setSpotBidMaxPrice(10D);
        jobClusterSpecific.setFirstOnDemand(5L);
        jobClusterSpecific.setSpotInstances(true);
        jobClusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        jobClusterSpecific.setDriverNodeTypeId("driverNodeTypeId");
        jobClusterSpecific.setSparkConf(new HashMap<>());
        jobClusterSpecific.setSparkEnvVars(new HashMap<>());
        jobClusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);
        databricksJobWorkloadSpecific.setCluster(jobClusterSpecific);

        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setCronExpression("00 * * * * ?");
        schedulingSpecific.setJavaTimezoneId("UTC");
        databricksJobWorkloadSpecific.setScheduling(schedulingSpecific);

        workload.setSpecific(databricksJobWorkloadSpecific);

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
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);
        mockJobAPI(workspaceClient);

        Either<FailedOperation, String> result =
                jobWorkloadHandler.provisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        assert result.isRight();
        assertEquals(result.get(), "123");
    }

    @Test
    public void provisionWorkload_ErrorCreatingJob() {
        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        mockReposAPI(workspaceClient);

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
    public void unprovisionWorkloadRemoveDataFalse_Success() {

        ProvisionRequest<DatabricksJobWorkloadSpecific> provisionRequest =
                new ProvisionRequest<>(dataProduct, workload, false);

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo(workspaceName, "test", "test", "test", "test");

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
                new DatabricksWorkspaceInfo(workspaceName, "test", "test", "test", "test");

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

        Either<FailedOperation, Void> result =
                jobWorkloadHandler.unprovisionWorkload(provisionRequest, workspaceClient, workspaceInfo);

        verify(workspaceClient.jobs(), times(2)).delete(anyLong());
        assert result.isRight();
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

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo(workspaceName, "test", "test", "test", "test");

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

        DatabricksWorkspaceInfo databricksWorkspaceInfo =
                new DatabricksWorkspaceInfo(workspaceName, "test", "test", "test", "test");

        List<BaseJob> baseJobList = Arrays.asList(new BaseJob().setJobId(1l), new BaseJob().setJobId(2l));
        Iterable<BaseJob> baseJobIterable = baseJobList;

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
