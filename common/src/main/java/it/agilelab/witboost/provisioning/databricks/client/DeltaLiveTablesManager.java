package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.ResourceDoesNotExist;
import com.databricks.sdk.service.pipelines.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DLTClusterSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.PipelineChannel;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.ProductEdition;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to manage Databricks Delta Live Pipelines.
 */
public class DeltaLiveTablesManager {
    private final Logger logger = LoggerFactory.getLogger(DeltaLiveTablesManager.class);
    private final WorkspaceClient workspaceClient;
    private final String workspaceName;

    public DeltaLiveTablesManager(WorkspaceClient workspaceClient, String workspaceName) {
        this.workspaceClient = workspaceClient;
        this.workspaceName = workspaceName;
    }

    public Either<FailedOperation, String> createDLTPipeline(
            String pipelineName,
            ProductEdition productEdition,
            Boolean continuous,
            List<String> notebooks,
            List<String> files,
            String catalog,
            String target,
            Boolean photon,
            Map<String, Collection<String>> notifications,
            PipelineChannel channel,
            DLTClusterSpecific clusterSpecific) {

        try {

            Collection<PipelineLibrary> libraries = new ArrayList<>();
            if (notebooks != null && !notebooks.isEmpty())
                notebooks.forEach(notebook ->
                        libraries.add(new PipelineLibrary().setNotebook(new NotebookLibrary().setPath(notebook))));
            if (files != null && !files.isEmpty())
                files.forEach(file -> libraries.add(new PipelineLibrary().setFile(new FileLibrary().setPath(file))));

            Collection<PipelineCluster> clusters = new ArrayList<>();

            //      Temporary functionality. Dots had to be replaced with underscores in the template
            Map<String, String> sparkConfNew = new HashMap<>();
            if (clusterSpecific.getSparkConf() != null)
                clusterSpecific.getSparkConf().keySet().forEach(key -> {
                    sparkConfNew.put(
                            key.replace("_", "."),
                            clusterSpecific.getSparkConf().get(key));
                });
            // ----

            PipelineCluster pipelineCluster = new PipelineCluster()
                    .setPolicyId(clusterSpecific.getPolicyId())
                    .setCustomTags(clusterSpecific.getTags())
                    .setDriverNodeTypeId(clusterSpecific.getDriverType())
                    .setNodeTypeId(clusterSpecific.getWorkerType())
                    .setSparkConf(sparkConfNew);

            PipelineClusterAutoscaleMode mode = clusterSpecific.getMode();
            if (mode != null
                    && (mode.equals(PipelineClusterAutoscaleMode.ENHANCED)
                            || mode.equals(PipelineClusterAutoscaleMode.LEGACY)))
                pipelineCluster.setAutoscale(new PipelineClusterAutoscale()
                        .setMinWorkers(clusterSpecific.getMinWorkers())
                        .setMaxWorkers(clusterSpecific.getMaxWorkers())
                        .setMode(clusterSpecific.getMode()));
            else pipelineCluster.setNumWorkers(clusterSpecific.getNumWorkers());

            clusters.add(pipelineCluster);

            CreatePipeline createPipeline = new CreatePipeline()
                    .setName(pipelineName)
                    .setEdition(productEdition.getValue())
                    .setContinuous(continuous)
                    .setLibraries(libraries)
                    .setCatalog(catalog)
                    .setTarget(target)
                    .setClusters(clusters)
                    .setPhoton(photon)
                    .setChannel(channel.getValue())
                    .setAllowDuplicateNames(true);

            if (notifications != null) {
                Collection<Notifications> not = new ArrayList<>();

                notifications.keySet().forEach(key -> {
                    not.add(new Notifications()
                            .setAlerts(notifications.get(key))
                            .setEmailRecipients(Collections.singletonList(key)));
                });

                createPipeline.setNotifications(not);
            }

            CreatePipelineResponse createPipelineResponse =
                    workspaceClient.pipelines().create(createPipeline);

            return right(createPipelineResponse.getPipelineId());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating the DLT Pipeline %s. Please try again and if the error persists contact the platform team. Details: %s",
                    pipelineName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    public Either<FailedOperation, Void> deletePipeline(String pipelineId) {
        try {
            logger.info(String.format("Deleting pipeline with ID: %s in %s", pipelineId, workspaceName));
            workspaceClient.pipelines().delete(pipelineId);
            return right(null);
        } catch (ResourceDoesNotExist e) {
            logger.info(String.format("Pipeline with ID not found in %s. Deletion skipped", pipelineId, workspaceName));
            return right(null);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while deleting the DLT Pipeline %s. Please try again and if the error persists contact the platform team. Details: %s",
                    pipelineId, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    public Either<FailedOperation, Iterable<PipelineStateInfo>> listPipelinesWithGivenName(String pipelineName) {
        try {

            String filter = String.format("name LIKE '%s'", pipelineName);
            Iterable<PipelineStateInfo> list =
                    workspaceClient.pipelines().listPipelines(new ListPipelinesRequest().setFilter(filter));

            return right(list);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while getting the list of DLT Pipelines named %s. Please try again and if the error persists contact the platform team. Details: %s",
                    pipelineName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
