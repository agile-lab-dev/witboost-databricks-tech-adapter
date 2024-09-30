package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.ResourceConflict;
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

    /**
     * Creates or updates a Delta Live Table (DLT) pipeline.
     * If no pipeline with the given name exists, a new one is created.
     * If a pipeline already exists, it updates the existing pipeline.
     * Returns an error if more than one pipeline with the same name exists.
     *
     * @param pipelineName The name of the pipeline.
     * @param productEdition The product edition for the pipeline (ADVANCED, CORE, PRO).
     * @param continuous Indicates whether the pipeline runs continuously.
     * @param notebooks List of notebook paths associated with the pipeline.
     * @param files List of file paths associated with the pipeline.
     * @param catalog The unity catalog associated with the pipeline.
     * @param target The target schema or database.
     * @param photon Indicates whether Photon is enabled.
     * @param notifications Notifications settings (email recipients and alerts).
     * @param channel The pipeline channel (current or preview).
     * @param clusterSpecific Cluster-specific configurations.
     * @return Either a failed operation or the pipeline ID (if creation or update succeeds).
     */
    public Either<FailedOperation, String> createOrUpdateDltPipeline(
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

        Either<FailedOperation, Iterable<PipelineStateInfo>> eitherGetPipelines =
                listPipelinesWithGivenName(pipelineName);

        if (eitherGetPipelines.isLeft()) return left(eitherGetPipelines.getLeft());

        Iterable<PipelineStateInfo> pipelines = eitherGetPipelines.get();
        List<PipelineStateInfo> pipelineList = new ArrayList<>();
        pipelines.forEach(pipelineList::add);

        if (pipelineList.isEmpty())
            return createDLTPipeline(
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
                    clusterSpecific);

        if (pipelineList.size() != 1) {
            String errorMessage = String.format(
                    "Error trying to update the pipeline '%s'. The pipeline name is not unique in %s.",
                    pipelineName, workspaceName);
            FailedOperation failedOperation = new FailedOperation(Collections.singletonList(new Problem(errorMessage)));
            return left(failedOperation);
        }

        PipelineStateInfo pipelineStateInfo = pipelineList.get(0);

        return updateDLTPipeline(
                pipelineStateInfo.getPipelineId(),
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
                clusterSpecific);
    }

    /**
     * Creates a new Delta Live Table (DLT) pipeline.
     * Validates that there is at least one notebook or file provided; if none are present, returns an error.
     * Handles potential resource conflicts, such as duplicate pipeline names.
     *
     * @param pipelineName The name of the pipeline.
     * @param productEdition The product edition for the pipeline (ADVANCED, CORE, PRO).
     * @param continuous Indicates whether the pipeline runs continuously.
     * @param notebooks List of notebook paths associated with the pipeline.
     * @param files List of file paths associated with the pipeline.
     * @param catalog The catalog associated with the pipeline.
     * @param target The target schema or database.
     * @param photon Indicates whether Photon is enabled.
     * @param notifications Notifications settings (email recipients and alerts).
     * @param channel The pipeline channel (current or preview).
     * @param clusterSpecific Cluster-specific configurations.
     * @return Either a failed operation or the pipeline ID (if creation succeeds).
     */
    private Either<FailedOperation, String> createDLTPipeline(
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
            logger.info(String.format("Creating pipeline %s in %s", pipelineName, workspaceName));

            Collection<PipelineLibrary> libraries = new ArrayList<>();
            Optional.ofNullable(notebooks)
                    .ifPresent(nbs -> nbs.forEach(notebook ->
                            libraries.add(new PipelineLibrary().setNotebook(new NotebookLibrary().setPath(notebook)))));
            Optional.ofNullable(files)
                    .ifPresent(fls -> fls.forEach(
                            file -> libraries.add(new PipelineLibrary().setFile(new FileLibrary().setPath(file)))));

            if (libraries.isEmpty()) {
                String errorMessage = String.format(
                        "An error occurred while creating the DLT Pipeline %s in %s, Details: it is mandatory to have at least one notebook or file.",
                        pipelineName, workspaceName);
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }

            CreatePipeline createPipeline = new CreatePipeline()
                    .setName(pipelineName)
                    .setEdition(productEdition.getValue())
                    .setContinuous(continuous)
                    .setLibraries(libraries)
                    .setCatalog(catalog)
                    .setTarget(target)
                    .setClusters(buildClusters(clusterSpecific))
                    .setPhoton(photon)
                    .setChannel(channel.getValue())
                    .setAllowDuplicateNames(false)
                    .setNotifications(buildNotifications(notifications));

            CreatePipelineResponse createPipelineResponse =
                    workspaceClient.pipelines().create(createPipeline);

            return right(createPipelineResponse.getPipelineId());

        } catch (ResourceConflict resourceConflict) {
            if (resourceConflict
                    .getMessage()
                    .contains("This check can be skipped by setting `allow_duplicate_names = true` in the request.")) {
                String errorMessage = String.format(
                        "Error creating the pipeline '%s'. The pipeline name is not unique in %s.",
                        pipelineName, workspaceName);
                logger.error(errorMessage, resourceConflict);
                return left(
                        new FailedOperation(Collections.singletonList(new Problem(errorMessage, resourceConflict))));
            }

            String errorMessage = String.format(
                    "An error occurred while creating the DLT Pipeline %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    pipelineName, workspaceName, resourceConflict.getMessage());
            logger.error(errorMessage, resourceConflict);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, resourceConflict))));
        } catch (Exception e) {

            String errorMessage = String.format(
                    "An error occurred while creating the DLT Pipeline %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    pipelineName, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Deletes an existing Delta Live Table (DLT) pipeline by its ID.
     * If the pipeline does not exist, the method skips deletion and returns success.
     *
     * @param pipelineId The ID of the pipeline to be deleted.
     * @return Either a successful result (void) or a failed operation in case of errors.
     */
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
                    "An error occurred while deleting the DLT Pipeline %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    pipelineId, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Retrieves a list of Delta Live Table (DLT) pipelines that match the specified name.
     * Returns an error if there is an issue retrieving the pipelines.
     *
     * @param pipelineName The name of the pipeline(s) to search for.
     * @return Either a list of matching pipelines or a failed operation.
     */
    public Either<FailedOperation, Iterable<PipelineStateInfo>> listPipelinesWithGivenName(String pipelineName) {
        try {

            String filter = String.format("name LIKE '%s'", pipelineName);
            Iterable<PipelineStateInfo> list =
                    workspaceClient.pipelines().listPipelines(new ListPipelinesRequest().setFilter(filter));

            return right(list);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while getting the list of DLT Pipelines named %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    pipelineName, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Updates an existing Delta Live Table (DLT) pipeline.
     * Validates that at least one notebook or file is provided. If none are present, it returns an error.
     * Updates pipeline configurations.
     *
     * @param pipelineId The ID of the pipeline to be updated.
     * @param pipelineName The name of the pipeline.
     * @param productEdition The product edition for the pipeline (ADVANCED, CORE, PRO).
     * @param continuous Indicates whether the pipeline runs continuously.
     * @param notebooks List of notebook paths associated with the pipeline.
     * @param files List of file paths associated with the pipeline.
     * @param catalog The catalog associated with the pipeline.
     * @param target The target schema or database.
     * @param photon Indicates whether Photon is enabled.
     * @param notifications Notifications settings (email recipients and alerts).
     * @param channel The pipeline channel (current or preview).
     * @param clusterSpecific Cluster-specific configurations.
     * @return Either a failed operation or the pipeline ID (if the update succeeds).
     */
    private Either<FailedOperation, String> updateDLTPipeline(
            String pipelineId,
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

            logger.info(String.format("Updating pipeline %s in %s", pipelineName, workspaceName));

            Collection<PipelineLibrary> libraries = buildPipelineLibraries(notebooks, files);

            if (libraries.isEmpty()) {
                String errorMessage = String.format(
                        "An error occurred while updating the DLT Pipeline %s in %s, Details: it is mandatory to have at least one notebook or file.",
                        pipelineName, workspaceName);
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }

            EditPipeline editPipeline = new EditPipeline()
                    .setPipelineId(pipelineId)
                    .setName(pipelineName)
                    .setEdition(productEdition.getValue())
                    .setContinuous(continuous)
                    .setLibraries(libraries)
                    .setCatalog(catalog)
                    .setTarget(target)
                    .setClusters(buildClusters(clusterSpecific))
                    .setPhoton(photon)
                    .setChannel(channel.getValue())
                    .setAllowDuplicateNames(false)
                    .setNotifications(buildNotifications(notifications));

            workspaceClient.pipelines().update(editPipeline);

            return right(pipelineId);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while updating the DLT Pipeline %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    pipelineName, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Builds the notification settings for a Delta Live Table (DLT) pipeline.
     *
     * @param notifications A map where the key is the email recipient, and the value is a collection of alerts.
     * @return A collection of `Notifications` objects representing the notification settings for the pipeline.
     */
    private Collection<Notifications> buildNotifications(Map<String, Collection<String>> notifications) {
        Collection<Notifications> notificationList = new ArrayList<>();
        if (notifications != null) {
            notifications.forEach((key, alerts) -> notificationList.add(
                    new Notifications().setAlerts(alerts).setEmailRecipients(Collections.singletonList(key))));
        }
        return notificationList;
    }

    /**
     * Constructs the cluster configuration for a Delta Live Table (DLT) pipeline.
     *
     * @param clusterSpecific Cluster-specific configurations.
     * @return A collection of `PipelineCluster` objects representing the cluster configuration.
     */
    private Collection<PipelineCluster> buildClusters(DLTClusterSpecific clusterSpecific) {
        Collection<PipelineCluster> clusters = new ArrayList<>();
        Map<String, String> sparkConfNew = new HashMap<>();
        if (clusterSpecific.getSparkConf() != null) {
            clusterSpecific
                    .getSparkConf()
                    .forEach(sparkConf -> sparkConfNew.put(sparkConf.getName(), sparkConf.getValue()));
        }

        PipelineCluster pipelineCluster = new PipelineCluster()
                .setPolicyId(clusterSpecific.getPolicyId())
                .setCustomTags(clusterSpecific.getTags())
                .setDriverNodeTypeId(clusterSpecific.getDriverType())
                .setNodeTypeId(clusterSpecific.getWorkerType())
                .setSparkConf(sparkConfNew);

        PipelineClusterAutoscaleMode mode = clusterSpecific.getMode();
        if (mode != null
                && (mode.equals(PipelineClusterAutoscaleMode.ENHANCED)
                        || mode.equals(PipelineClusterAutoscaleMode.LEGACY))) {
            pipelineCluster.setAutoscale(new PipelineClusterAutoscale()
                    .setMinWorkers(clusterSpecific.getMinWorkers())
                    .setMaxWorkers(clusterSpecific.getMaxWorkers())
                    .setMode(clusterSpecific.getMode()));
        } else {
            pipelineCluster.setNumWorkers(clusterSpecific.getNumWorkers());
        }

        clusters.add(pipelineCluster);
        return clusters;
    }

    /**
     * Builds the libraries for the Delta Live Table (DLT) pipeline.
     * Accepts a list of notebooks and files and adds them as libraries to be used by the pipeline.
     *
     * @param notebooks List of notebook paths associated with the pipeline.
     * @param files List of file paths associated with the pipeline.
     * @return A collection of `PipelineLibrary` objects representing the libraries for the pipeline.
     */
    private Collection<PipelineLibrary> buildPipelineLibraries(List<String> notebooks, List<String> files) {
        Collection<PipelineLibrary> libraries = new ArrayList<>();
        Optional.ofNullable(notebooks)
                .ifPresent(nbs -> nbs.forEach(notebook ->
                        libraries.add(new PipelineLibrary().setNotebook(new NotebookLibrary().setPath(notebook)))));
        Optional.ofNullable(files)
                .ifPresent(fls -> fls.forEach(
                        file -> libraries.add(new PipelineLibrary().setFile(new FileLibrary().setPath(file)))));
        return libraries;
    }
}
