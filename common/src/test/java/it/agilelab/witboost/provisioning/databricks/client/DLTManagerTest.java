package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.error.platform.ResourceConflict;
import com.databricks.sdk.core.error.platform.ResourceDoesNotExist;
import com.databricks.sdk.service.pipelines.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.SparkEnvVar;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt.DLTClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt.DatabricksDLTWorkloadSpecific;
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
public class DLTManagerTest {

    @Mock
    WorkspaceClient workspaceClient;

    DLTManager DLTManager;

    private static final String pipelineId = "pipelineId";
    private static final String pipelineName = "example_pipeline";
    private static final DatabricksDLTWorkloadSpecific.ProductEdition productEdition =
            DatabricksDLTWorkloadSpecific.ProductEdition.CORE;
    private static final Boolean continuous = true;
    private static final List<String> notebooks = Arrays.asList("/Workspace/Notebook1", "/Workspace/Notebook2");
    private static final List<String> files = Arrays.asList("/Workspace/File1", "/Workspace/File2");
    private static final String catalog = "example_catalog";
    private static final String target = "example_target";
    private static final Boolean photon = true;
    private static final DatabricksDLTWorkloadSpecific.PipelineChannel channel =
            DatabricksDLTWorkloadSpecific.PipelineChannel.CURRENT;
    private static final CreatePipelineResponse createResponse =
            new CreatePipelineResponse().setPipelineId("pipelineId");
    private static final Map<String, Collection<String>> notifications = new HashMap<>();

    private DLTClusterSpecific createDLTClusterSpecific(PipelineClusterAutoscaleMode pipelineClusterAutoscaleMode) {
        DLTClusterSpecific dltClusterSpecific = new DLTClusterSpecific();
        dltClusterSpecific.setMode(pipelineClusterAutoscaleMode);
        dltClusterSpecific.setMinWorkers(1L);
        dltClusterSpecific.setMaxWorkers(3L);
        dltClusterSpecific.setWorkerType("Worker");
        dltClusterSpecific.setDriverType("Driver");
        dltClusterSpecific.setPolicyId("PolicyID");
        return dltClusterSpecific;
    }

    @BeforeEach
    public void setUp() {
        DLTManager = new DLTManager(workspaceClient, "workspace");
        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        lenient().when(workspaceClient.pipelines()).thenReturn(mockPipelines);

        notifications.clear();
        notifications.put("email@example.com", List.of("alert1", "alert2"));
    }

    @Test
    public void testCreatePipeline_LEGACYAutoscale() {
        when(workspaceClient.pipelines().create(any())).thenReturn(createResponse);

        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.LEGACY);

        var result = DLTManager.createOrUpdateDltPipeline(
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
                dltClusterSpecific,
                "development");

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertEquals(createResponse.getPipelineId(), result.get());
    }

    @Test
    public void testCreatePipeline_ENHANCEDAutoscale() {
        when(workspaceClient.pipelines().create(any())).thenReturn(createResponse);

        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);

        var result = DLTManager.createOrUpdateDltPipeline(
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
                dltClusterSpecific,
                "development");

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertEquals(createResponse.getPipelineId(), result.get());
    }

    @Test
    public void testCreatePipeline_AutoscaleNull() {

        when(workspaceClient.pipelines().create(any())).thenReturn(createResponse);

        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(null);

        notifications.put("email@email.com", List.of("alert1", "alert2"));

        var result = DLTManager.createOrUpdateDltPipeline(
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
                dltClusterSpecific,
                "development");

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertEquals(createResponse.getPipelineId(), result.get());
    }

    @Test
    public void testCreatePipeline_Exception() {
        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);
        when(workspaceClient.pipelines().create(any())).thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, String> result = DLTManager.createOrUpdateDltPipeline(
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
                dltClusterSpecific,
                "development");

        verify(workspaceClient.pipelines(), times(1)).create(any());
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while creating the DLT Pipeline example_pipeline in workspace. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testDeletePipeline() {
        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        doNothing().when(mockPipelines).delete(pipelineId);

        Either<FailedOperation, Void> result = DLTManager.deletePipeline(pipelineId);
        assertTrue(result.isRight());
    }

    @Test
    public void testDeleteResourceDoesNotExists() {
        String expectedError = "resource does not exists";
        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        doThrow(new ResourceDoesNotExist(expectedError, new ArrayList<>()))
                .when(mockPipelines)
                .delete(pipelineId);

        Either<FailedOperation, Void> result = DLTManager.deletePipeline(pipelineId);

        assertTrue(result.isRight());
        verify(workspaceClient.pipelines(), times(1)).delete(anyString());
    }

    @Test
    public void testDeletePipeline_Exception() {
        when(workspaceClient.pipelines()).thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, Void> result = DLTManager.deletePipeline(pipelineId);
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while deleting the DLT Pipeline pipelineId in workspace. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testListPipelines() {
        List<PipelineStateInfo> pipelineStateInfos =
                Collections.singletonList(new PipelineStateInfo().setName("pipelineName"));

        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        when(workspaceClient.pipelines().listPipelines(any())).thenReturn(pipelineStateInfos);

        var result = DLTManager.listPipelinesWithGivenName("pipelineName");
        assertEquals(pipelineStateInfos, result.get());
    }

    @Test
    public void testListPipelines_Exception() {
        when(workspaceClient.pipelines()).thenThrow(new RuntimeException("Exception"));

        Either<FailedOperation, Iterable<PipelineStateInfo>> result =
                DLTManager.listPipelinesWithGivenName("pipelineName");
        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while getting the list of DLT Pipelines named pipelineName in workspace. Please try again and if the error persists contact the platform team. Details: Exception",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testUpdatePipeline() {
        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);

        List<PipelineStateInfo> pipelineStateInfos = Collections.singletonList(
                new PipelineStateInfo().setName("example_pipeline").setPipelineId(pipelineId));

        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        when(workspaceClient.pipelines().listPipelines(any())).thenReturn(pipelineStateInfos);

        Either<FailedOperation, String> result = DLTManager.createOrUpdateDltPipeline(
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
                dltClusterSpecific,
                "development");

        verify(workspaceClient.pipelines(), times(1)).update(any(EditPipeline.class));
        assertTrue(result.isRight());
        assertEquals(pipelineId, result.get());
    }

    @Test
    public void testUpdatePipeline_Exception() {
        DLTClusterSpecific dltClusterSpecific = createDLTClusterSpecific(PipelineClusterAutoscaleMode.ENHANCED);

        List<PipelineStateInfo> pipelineStateInfos = Collections.singletonList(
                new PipelineStateInfo().setName("example_pipeline").setPipelineId(pipelineId));

        PipelinesAPI mockPipelines = mock(PipelinesAPI.class);
        when(workspaceClient.pipelines()).thenReturn(mockPipelines);
        when(workspaceClient.pipelines().listPipelines(any())).thenReturn(pipelineStateInfos);
        doThrow(new DatabricksException("Exception")).when(mockPipelines).update(any(EditPipeline.class));

        Either<FailedOperation, String> result = DLTManager.createOrUpdateDltPipeline(
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
                dltClusterSpecific,
                "development");

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

        Either<FailedOperation, Void> result = DLTManager.deletePipeline(invalidPipelineId);
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

        Either<FailedOperation, String> result = DLTManager.createOrUpdateDltPipeline(
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
                dltClusterSpecific,
                "development");

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

        Either<FailedOperation, String> result = DLTManager.createOrUpdateDltPipeline(
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
                dltClusterSpecific,
                "development");

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

        Either<FailedOperation, String> result = DLTManager.createOrUpdateDltPipeline(
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
                dltClusterSpecific,
                "development");

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
        Either<FailedOperation, String> result = DLTManager.createOrUpdateDltPipeline(
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
                dltClusterSpecific,
                "development");

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
        List<PipelineStateInfo> pipelineStateInfos = Collections.singletonList(
                new PipelineStateInfo().setName("example_pipeline").setPipelineId(pipelineId));

        when(workspaceClient.pipelines().listPipelines(any())).thenReturn(pipelineStateInfos);

        Either<FailedOperation, String> result = DLTManager.createOrUpdateDltPipeline(
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
                dltClusterSpecific,
                "development");

        assertTrue(result.isLeft());
        assertEquals(
                "An error occurred while updating the DLT Pipeline example_pipeline in workspace, Details: it is mandatory to have at least one notebook or file.",
                result.getLeft().problems().get(0).description());
    }

    @Test
    public void testGetSparkEnvVarsForEnvironment_ValidEnvironment_Development() {
        DLTClusterSpecific mockDLTClusterSpecific = mock(DLTClusterSpecific.class);
        List<SparkEnvVar> devEnvVars = List.of(new SparkEnvVar("dev_key1", "dev_value1"));
        when(mockDLTClusterSpecific.getSparkEnvVarsDevelopment()).thenReturn(devEnvVars);

        Either<FailedOperation, Map<String, String>> result =
                DLTManager.getSparkEnvVarsForEnvironment("development", mockDLTClusterSpecific, "pipeline_name");

        assertTrue(result.isRight());
        assertEquals(Map.of("dev_key1", "dev_value1"), result.get());
    }

    @Test
    public void testGetSparkEnvVarsForEnvironment_ValidEnvironment_Qa() {
        DLTClusterSpecific mockDLTClusterSpecific = mock(DLTClusterSpecific.class);
        List<SparkEnvVar> qaEnvVars = List.of(new SparkEnvVar("qa_key1", "qa_value1"));
        when(mockDLTClusterSpecific.getSparkEnvVarsQa()).thenReturn(qaEnvVars);

        Either<FailedOperation, Map<String, String>> result =
                DLTManager.getSparkEnvVarsForEnvironment("qa", mockDLTClusterSpecific, "pipeline_name");

        assertTrue(result.isRight());
        assertEquals(Map.of("qa_key1", "qa_value1"), result.get());
    }

    @Test
    public void testGetSparkEnvVarsForEnvironment_ValidEnvironment_Prod() {
        DLTClusterSpecific mockDLTClusterSpecific = mock(DLTClusterSpecific.class);
        List<SparkEnvVar> prodEnvVars = List.of(new SparkEnvVar("prod_key1", "prod_value1"));
        when(mockDLTClusterSpecific.getSparkEnvVarsProduction()).thenReturn(prodEnvVars);

        Either<FailedOperation, Map<String, String>> result =
                DLTManager.getSparkEnvVarsForEnvironment("production", mockDLTClusterSpecific, "pipeline_name");

        assertTrue(result.isRight());
        assertEquals(Map.of("prod_key1", "prod_value1"), result.get());
    }

    @Test
    public void testGetSparkEnvVarsForEnvironment_InvalidEnvironment() {
        DLTClusterSpecific mockDLTClusterSpecific = mock(DLTClusterSpecific.class);

        Either<FailedOperation, Map<String, String>> result =
                DLTManager.getSparkEnvVarsForEnvironment("INVALID", mockDLTClusterSpecific, "pipeline_name");

        assertTrue(result.isLeft());
        assert (result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains(
                        "An error occurred while getting the Spark environment variables for the pipeline 'pipeline_name' in the environment 'INVALID'. The specified environment is invalid. Available options are: DEVELOPMENT, QA, PRODUCTION. "
                                + "Details: No enum constant it.agilelab.witboost.provisioning.databricks.model.Environment.INVALID"));
    }

    @Test
    public void testGetSparkEnvVarsForEnvironment_NullEnvironment() {
        DLTClusterSpecific mockDLTClusterSpecific = mock(DLTClusterSpecific.class);

        Either<FailedOperation, Map<String, String>> result =
                DLTManager.getSparkEnvVarsForEnvironment(null, mockDLTClusterSpecific, "pipeline_name");

        assertTrue(result.isLeft());
        assert (result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains(
                        "An error occurred while getting the Spark environment variables for the pipeline 'pipeline_name' in the environment 'null'. The specified environment is invalid. Available options are: DEVELOPMENT, QA, PRODUCTION."));
    }
}
