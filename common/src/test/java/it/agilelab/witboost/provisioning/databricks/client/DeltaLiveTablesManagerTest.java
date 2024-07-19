package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.error.platform.ResourceDoesNotExist;
import com.databricks.sdk.service.pipelines.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DLTClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.PipelineChannel;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.ProductEdition;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
@ExtendWith(MockitoExtension.class)
public class DeltaLiveTablesManagerTest {

    @Mock
    WorkspaceClient workspaceClient;

    DeltaLiveTablesManager deltaLiveTablesManager;

    DatabricksConfig mockDatabricksConfig;

    @BeforeEach
    public void setUp() {
        deltaLiveTablesManager = new DeltaLiveTablesManager(workspaceClient, "workspace");
        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
    }

    @Test
    public void testCreatePipeline_LEGACYAutoscale() {
        String workspaceName = "example_workspace";
        DeltaLiveTablesManager manager = new DeltaLiveTablesManager(workspaceClient, workspaceName);

        String pipelineName = "example_pipeline";
        ProductEdition productEdition = ProductEdition.CORE;
        Boolean continuous = true;
        List<String> notebooks = Arrays.asList("/Workspace/Notebook1", "/Workspace/Notebook2");
        List<String> files = Arrays.asList("/Workspace/File1", "/Workspace/File2");
        String catalog = "example_catalog";
        String target = "example_target";
        Boolean photon = true;
        Collection<String> notificationsMails = Arrays.asList("user@example.com");
        Collection<String> notificationsAlerts = Arrays.asList("alert1", "alert2");
        PipelineChannel channel = PipelineChannel.CURRENT;

        CreatePipelineResponse createResponse = new CreatePipelineResponse().setPipelineId("pipelineId");

        when(workspaceClient.pipelines().create(any())).thenReturn(createResponse);

        DLTClusterSpecific dltClusterSpecific = new DLTClusterSpecific();
        dltClusterSpecific.setMode(PipelineClusterAutoscaleMode.LEGACY);
        dltClusterSpecific.setMinWorkers(1l);
        dltClusterSpecific.setMaxWorkers(3l);
        dltClusterSpecific.setWorkerType("Worker");
        dltClusterSpecific.setDriverType("Driver");
        dltClusterSpecific.setPolicyId("PolicyID");

        var result = deltaLiveTablesManager.createDLTPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notificationsMails,
                notificationsAlerts,
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertEquals(createResponse.getPipelineId(), result.get());
    }

    @Test
    public void testCreatePipeline_ENHANCEDAutoscale() {
        String workspaceName = "example_workspace";
        DeltaLiveTablesManager manager = new DeltaLiveTablesManager(workspaceClient, workspaceName);

        String pipelineName = "example_pipeline";
        ProductEdition productEdition = ProductEdition.CORE;
        Boolean continuous = true;

        List<String> notebooks = Arrays.asList("/Workspace/Notebook1", "/Workspace/Notebook2");
        List<String> files = Arrays.asList("/Workspace/File1", "/Workspace/File2");

        String catalog = "example_catalog";
        String target = "example_target";
        Boolean photon = true;

        Collection<String> notificationsMails = Arrays.asList("user@example.com");
        Collection<String> notificationsAlerts = Arrays.asList("alert1", "alert2");

        PipelineChannel channel = PipelineChannel.CURRENT;

        CreatePipelineResponse createResponse = new CreatePipelineResponse().setPipelineId("pipelineId");

        when(workspaceClient.pipelines().create(any())).thenReturn(createResponse);

        DLTClusterSpecific dltClusterSpecific = new DLTClusterSpecific();
        dltClusterSpecific.setMode(PipelineClusterAutoscaleMode.ENHANCED);
        dltClusterSpecific.setMinWorkers(1l);
        dltClusterSpecific.setMaxWorkers(3l);
        dltClusterSpecific.setWorkerType("Worker");
        dltClusterSpecific.setDriverType("Driver");
        dltClusterSpecific.setPolicyId("PolicyID");

        var result = deltaLiveTablesManager.createDLTPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notificationsMails,
                notificationsAlerts,
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertEquals(createResponse.getPipelineId(), result.get());
    }

    @Test
    public void testCreatePipeline_AutoscaleNull() {
        String workspaceName = "example_workspace";
        DeltaLiveTablesManager manager = new DeltaLiveTablesManager(workspaceClient, workspaceName);
        String pipelineName = "example_pipeline";
        ProductEdition productEdition = ProductEdition.CORE;
        Boolean continuous = true;
        List<String> notebooks = Arrays.asList("/Workspace/Notebook1", "/Workspace/Notebook2");
        List<String> files = Arrays.asList("/Workspace/File1", "/Workspace/File2");
        String catalog = "example_catalog";
        String target = "example_target";
        Boolean photon = true;
        Map<String, String> cluster_tags = new HashMap<>();
        cluster_tags.put("env", "test");
        Collection<String> notificationsMails = Arrays.asList("user@example.com");
        PipelineChannel channel = PipelineChannel.CURRENT;

        CreatePipelineResponse createResponse = new CreatePipelineResponse().setPipelineId("pipelineId");

        when(workspaceClient.pipelines().create(any())).thenReturn(createResponse);

        DLTClusterSpecific dltClusterSpecific = new DLTClusterSpecific();
        dltClusterSpecific.setMode(null);
        dltClusterSpecific.setMinWorkers(1l);
        dltClusterSpecific.setMaxWorkers(3l);
        dltClusterSpecific.setWorkerType("Worker");
        dltClusterSpecific.setDriverType("Driver");
        dltClusterSpecific.setPolicyId("PolicyID");

        var result = deltaLiveTablesManager.createDLTPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notificationsMails,
                new ArrayList<>(),
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertEquals(createResponse.getPipelineId(), result.get());
    }

    @Test
    public void testCreatePipeline_Exception() {
        String workspaceName = "example_workspace";
        DeltaLiveTablesManager manager = new DeltaLiveTablesManager(workspaceClient, workspaceName);

        String pipelineName = "example_pipeline";
        ProductEdition productEdition = ProductEdition.CORE;
        Boolean continuous = true;

        List<String> notebooks = Arrays.asList("/Workspace/Notebook1", "/Workspace/Notebook2");
        List<String> files = Arrays.asList("/Workspace/File1", "/Workspace/File2");

        String catalog = "example_catalog";
        String target = "example_target";
        String cluster_policyId = "example_policy_id";
        PipelineClusterAutoscaleMode cluster_mode = PipelineClusterAutoscaleMode.ENHANCED;
        Long cluster_minWorkers = 2L;
        Long cluster_maxWorkers = 10L;
        Long cluster_workers = 5L;
        Boolean photon = true;
        Map<String, String> cluster_tags = new HashMap<>();
        cluster_tags.put("env", "test");

        Collection<String> notificationsMails = Arrays.asList("user@example.com");
        Collection<String> notificationsAlerts = Arrays.asList("alert1", "alert2");

        PipelineChannel channel = PipelineChannel.CURRENT;
        String workerType = "worker_type_id";
        String driverType = "driver_type_id";
        Map<String, String> sparkConf = new HashMap<>();
        sparkConf.put("spark.executor.memory", "4g");

        CreatePipelineResponse createResponse = new CreatePipelineResponse().setPipelineId("pipelineId");

        when(workspaceClient.pipelines().create(any())).thenThrow(new RuntimeException("Exception"));

        DLTClusterSpecific dltClusterSpecific = new DLTClusterSpecific();
        dltClusterSpecific.setMode(PipelineClusterAutoscaleMode.ENHANCED);
        dltClusterSpecific.setMinWorkers(1l);
        dltClusterSpecific.setMaxWorkers(3l);
        dltClusterSpecific.setWorkerType("Worker");
        dltClusterSpecific.setDriverType("Driver");
        dltClusterSpecific.setPolicyId("PolicyID");

        var result = deltaLiveTablesManager.createDLTPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notificationsMails,
                notificationsAlerts,
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while creating the DLT Pipeline example_pipeline. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testdeletePipeline() {
        String pipelineId = "pipelineId";

        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        doNothing().when(mockPipelines).delete(pipelineId);

        Either<FailedOperation, Void> result = deltaLiveTablesManager.deletePipeline(pipelineId);
        assertTrue(result.isRight());
    }

    @Test
    public void testdeleteResourceDoesNotExists() {
        String pipelineId = "pipelineId";
        String expectedError = "resource does not exists";
        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        doThrow(new ResourceDoesNotExist(expectedError, new ArrayList<>()))
                .when(mockPipelines)
                .delete(pipelineId);

        Either<FailedOperation, Void> result = deltaLiveTablesManager.deletePipeline(pipelineId);

        assertTrue(result.isRight());
        verify(workspaceClient.pipelines(), times(1)).delete(anyString());
    }

    @Test
    public void testdeletePipeline_Exception() {
        String pipelineId = "pipelineId";

        when(workspaceClient.pipelines()).thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, Void> result = deltaLiveTablesManager.deletePipeline(pipelineId);
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while deleting the DLT Pipeline pipelineId. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testListPipelines() {
        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(new PipelineStateInfo().setName("pipelineName"));
        Iterable<PipelineStateInfo> pipelineStateInfoIterable = pipelineStateInfos;

        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        when(workspaceClient.pipelines().listPipelines(any())).thenReturn(pipelineStateInfoIterable);

        var result = deltaLiveTablesManager.listPipelinesWithGivenName("pipelineName");
        assertEquals(pipelineStateInfoIterable, result.get());
    }

    @Test
    public void testListPipelines_Exception() {
        when(workspaceClient.pipelines()).thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, Iterable<PipelineStateInfo>> result =
                deltaLiveTablesManager.listPipelinesWithGivenName("pipelineName");
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while getting the list of DLT Pipelines named pipelineName. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }
}
