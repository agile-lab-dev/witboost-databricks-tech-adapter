package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.error.platform.ResourceConflict;
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

    private static final String workspaceName = "example_workspace";
    private final DeltaLiveTablesManager manager = new DeltaLiveTablesManager(workspaceClient, workspaceName);
    private static final String pipelineId = "pipelineId";
    private static final String pipelineName = "example_pipeline";
    private static final ProductEdition productEdition = ProductEdition.CORE;
    private static final Boolean continuous = true;
    private static final List<String> notebooks = Arrays.asList("/Workspace/Notebook1", "/Workspace/Notebook2");
    private static final List<String> files = Arrays.asList("/Workspace/File1", "/Workspace/File2");
    private static final String catalog = "example_catalog";
    private static final String target = "example_target";
    private static final Boolean photon = true;
    private static final Collection<String> notificationsMails = Arrays.asList("user@example.com");
    private static final Collection<String> notificationsAlerts = Arrays.asList("alert1", "alert2");
    private static final PipelineChannel channel = PipelineChannel.CURRENT;
    private static final CreatePipelineResponse createResponse =
            new CreatePipelineResponse().setPipelineId("pipelineId");
    private static final Map<String, Collection<String>> notifications = new HashMap<>();

    private DLTClusterSpecific createDLTClusterSpecific(PipelineClusterAutoscaleMode pipelineClusterAutoscaleMode) {
        DLTClusterSpecific dltClusterSpecific = new DLTClusterSpecific();
        dltClusterSpecific.setMode(pipelineClusterAutoscaleMode);
        dltClusterSpecific.setMinWorkers(1l);
        dltClusterSpecific.setMaxWorkers(3l);
        dltClusterSpecific.setWorkerType("Worker");
        dltClusterSpecific.setDriverType("Driver");
        dltClusterSpecific.setPolicyId("PolicyID");
        return dltClusterSpecific;
    }

    @BeforeEach
    public void setUp() {
        deltaLiveTablesManager = new DeltaLiveTablesManager(workspaceClient, "workspace");
        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);

        notifications.clear();
        notifications.put("email@example.com", List.of("alert1", "alert2"));
    }

    @Test
    public void testCreatePipeline_LEGACYAutoscale() {
        when(workspaceClient.pipelines().create(any())).thenReturn(createResponse);

        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.LEGACY);

        var result = deltaLiveTablesManager.createOrUpdateDltPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertEquals(createResponse.getPipelineId(), result.get());
    }

    public void testCreatePipeline_ENHANCEDAutoscale() {
        when(workspaceClient.pipelines().create(any())).thenReturn(createResponse);

        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);

        var result = deltaLiveTablesManager.createOrUpdateDltPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertEquals(createResponse.getPipelineId(), result.get());
    }

    @Test
    public void testCreatePipeline_AutoscaleNull() {

        when(workspaceClient.pipelines().create(any())).thenReturn(createResponse);

        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(null);

        notifications.put("email@email.com", List.of("alert1", "alert2"));

        var result = deltaLiveTablesManager.createOrUpdateDltPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertEquals(createResponse.getPipelineId(), result.get());
    }

    @Test
    public void testCreatePipeline_Exception() {
        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);
        when(workspaceClient.pipelines().create(any())).thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, String> result = deltaLiveTablesManager.createOrUpdateDltPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while creating the DLT Pipeline example_pipeline in workspace. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testdeletePipeline() {
        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        doNothing().when(mockPipelines).delete(pipelineId);

        Either<FailedOperation, Void> result = deltaLiveTablesManager.deletePipeline(pipelineId);
        assertTrue(result.isRight());
    }

    @Test
    public void testdeleteResourceDoesNotExists() {
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
        when(workspaceClient.pipelines()).thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, Void> result = deltaLiveTablesManager.deletePipeline(pipelineId);
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while deleting the DLT Pipeline pipelineId in workspace. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testListPipelines() {
        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(new PipelineStateInfo().setName("pipelineName"));

        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        when(workspaceClient.pipelines().listPipelines(any())).thenReturn(pipelineStateInfos);

        var result = deltaLiveTablesManager.listPipelinesWithGivenName("pipelineName");
        assertEquals(pipelineStateInfos, result.get());
    }

    @Test
    public void testListPipelines_Exception() {
        when(workspaceClient.pipelines()).thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, Iterable<PipelineStateInfo>> result =
                deltaLiveTablesManager.listPipelinesWithGivenName("pipelineName");
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while getting the list of DLT Pipelines named pipelineName in workspace. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testUpdatePipeline() {
        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);

        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(
                new PipelineStateInfo().setName("example_pipeline").setPipelineId(pipelineId));

        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        when(workspaceClient.pipelines().listPipelines(any())).thenReturn(pipelineStateInfos);

        Either<FailedOperation, String> result = deltaLiveTablesManager.createOrUpdateDltPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).update(any(EditPipeline.class));
        assertTrue(result.isRight());
        assertEquals(pipelineId, result.get());
    }

    @Test
    public void testUpdatePipeline_Exception() {
        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);

        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(
                new PipelineStateInfo().setName("example_pipeline").setPipelineId(pipelineId));

        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        when(workspaceClient.pipelines().listPipelines(any())).thenReturn(pipelineStateInfos);
        doThrow(new DatabricksException("Exception")).when(mockPipelines).update(any(EditPipeline.class));

        Either<FailedOperation, String> result = deltaLiveTablesManager.createOrUpdateDltPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).update(any(EditPipeline.class));
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while updating the DLT Pipeline example_pipeline in workspace. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testDeletePipeline_InvalidId() {
        String invalidPipelineId = "invalid_pipelineId";

        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        doThrow(new ResourceDoesNotExist("Pipeline not found", new ArrayList<>()))
                .when(mockPipelines)
                .delete(invalidPipelineId);

        Either<FailedOperation, Void> result = deltaLiveTablesManager.deletePipeline(invalidPipelineId);
        assertTrue(result.isRight());
    }

    @Test
    public void testUpdatePipeline_NotUnique() {

        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);
        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(
                new PipelineStateInfo().setName("example_pipeline").setPipelineId(pipelineId),
                new PipelineStateInfo().setName("example_pipeline").setPipelineId("id2"));

        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        when(workspaceClient.pipelines().listPipelines(any())).thenReturn(pipelineStateInfos);

        Either<FailedOperation, String> result = deltaLiveTablesManager.createOrUpdateDltPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                dltClusterSpecific);

        assertTrue(result.isLeft());
        assertEquals(
                "Error trying to update the pipeline 'example_pipeline'. The pipeline name is not unique in workspace.",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testCreatePipeline_EmptyLibrary() {
        List<String> notebooks = null;
        List<String> files = null;

        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(null);
        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);

        Either<FailedOperation, String> result = deltaLiveTablesManager.createOrUpdateDltPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                dltClusterSpecific);

        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while creating the DLT Pipeline example_pipeline in workspace, Details: it is mandatory to have at least one notebook or file.",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testCreatePipeline_ResourceConflictException_PipelineNotUnique() {
        when(workspaceClient.pipelines().create(any()))
                .thenThrow(new ResourceConflict(
                        "This check can be skipped by setting `allow_duplicate_names = true` in the request.",
                        List.of()));

        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);

        Either<FailedOperation, String> result = deltaLiveTablesManager.createOrUpdateDltPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertTrue(result.isLeft());
        assertEquals(
                "Error creating the pipeline 'example_pipeline'. The pipeline name is not unique in workspace.",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testCreatePipeline_ResourceConflictException() {
        when(workspaceClient.pipelines().create(any())).thenThrow(new ResourceConflict("Exception", List.of()));

        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);
        Either<FailedOperation, String> result = deltaLiveTablesManager.createOrUpdateDltPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                dltClusterSpecific);

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while creating the DLT Pipeline example_pipeline in workspace. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testUpdatePipeline_EmptyLibrary() {
        List<String> notebooks = null;
        List<String> files = null;
        Boolean photon = true;

        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);
        List<PipelineStateInfo> pipelineStateInfos = Arrays.asList(
                new PipelineStateInfo().setName("example_pipeline").setPipelineId(pipelineId));

        when(workspaceClient.pipelines().listPipelines(any())).thenReturn(pipelineStateInfos);

        Either<FailedOperation, String> result = deltaLiveTablesManager.createOrUpdateDltPipeline(
                pipelineName,
                productEdition,
                continuous,
                notebooks,
                files,
                catalog,
                target,
                photon,
                notifications,
                channel,
                dltClusterSpecific);

        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while updating the DLT Pipeline example_pipeline in workspace, Details: it is mandatory to have at least one notebook or file.",
                result.getLeft().problems().get(0).description());
    }
}
