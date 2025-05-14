package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.pipelines.PipelineStateInfo;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.DLTManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.client.WorkspaceLevelManagerFactory;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksPermissionsConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt.DatabricksDLTWorkloadSpecific;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class DLTWorkloadHandler extends BaseWorkloadHandler {

    private final Logger logger = LoggerFactory.getLogger(DLTWorkloadHandler.class);

    @Autowired
    public DLTWorkloadHandler(
            AzureAuthConfig azureAuthConfig,
            GitCredentialsConfig gitCredentialsConfig,
            DatabricksPermissionsConfig databricksPermissionsConfig,
            AccountClient accountClient,
            WorkspaceLevelManagerFactory workspaceLevelManagerFactory) {
        super(
                azureAuthConfig,
                gitCredentialsConfig,
                databricksPermissionsConfig,
                accountClient,
                workspaceLevelManagerFactory);
    }

    /**
     * Provisions a Databricks Delta Live Tables (DLT) pipeline workload.
     *
     * @param provisionRequest the request containing the details for provisioning
     * @param workspaceClient the Databricks workspace client
     * @param databricksWorkspaceInfo information about the Databricks workspace
     * @return Either a FailedOperation or a String containing the ID of the created pipeline if successful
     */
    public Either<FailedOperation, String> provisionWorkload(
            ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {

        try {
            DatabricksDLTWorkloadSpecific databricksDLTWorkloadSpecific =
                    provisionRequest.component().getSpecific();

            var unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

            Either<FailedOperation, Void> eitherAttachedMetastore =
                    unityCatalogManager.attachMetastore(databricksDLTWorkloadSpecific.getMetastore());

            if (eitherAttachedMetastore.isLeft()) return left(eitherAttachedMetastore.getLeft());

            Either<FailedOperation, Void> eitherCreatedCatalog =
                    unityCatalogManager.createCatalogIfNotExists(databricksDLTWorkloadSpecific.getCatalog());

            if (eitherCreatedCatalog.isLeft()) return left(eitherCreatedCatalog.getLeft());

            Either<FailedOperation, Map<String, String>> eitherPrincipalsMapping = mapPrincipals(provisionRequest);
            if (eitherPrincipalsMapping.isLeft()) return Either.left(eitherPrincipalsMapping.getLeft());

            Map<String, String> principalsMapping = eitherPrincipalsMapping.get();
            String dpOwnerDatabricksId =
                    principalsMapping.get(provisionRequest.dataProduct().getDataProductOwner());

            // TODO: This is a temporary solution. Remove or update this logic in the future.
            String devGroup = provisionRequest.dataProduct().getDevGroup();

            if (!devGroup.startsWith("group:")) devGroup = "group:" + devGroup;

            String dpDevGroupDatabricksId = principalsMapping.get(devGroup);

            Either<FailedOperation, Void> eitherCreatedRepo = createRepositoryWithPermissions(
                    provisionRequest,
                    workspaceClient,
                    databricksWorkspaceInfo,
                    dpOwnerDatabricksId,
                    dpDevGroupDatabricksId);
            if (eitherCreatedRepo.isLeft()) {
                return left(eitherCreatedRepo.getLeft());
            }

            var dltManager = new DLTManager(workspaceClient, databricksWorkspaceInfo.getName());

            List<String> notebooks = new ArrayList<>();

            Optional.ofNullable(databricksDLTWorkloadSpecific.getNotebooks())
                    .ifPresent(nbs -> nbs.forEach(notebook -> notebooks.add(String.format("/Workspace/%s", notebook))));

            Map<String, Collection<String>> notifications = new HashMap<>();
            if (databricksDLTWorkloadSpecific.getNotifications() != null) {
                databricksDLTWorkloadSpecific.getNotifications().forEach(notification -> {
                    notifications.put(notification.getMail(), notification.getAlert());
                });
            }

            Either<FailedOperation, String> eitherCreatedPipeline = dltManager.createOrUpdateDltPipeline(
                    databricksDLTWorkloadSpecific.getPipelineName(),
                    databricksDLTWorkloadSpecific.getProductEdition(),
                    databricksDLTWorkloadSpecific.getContinuous(),
                    notebooks,
                    databricksDLTWorkloadSpecific.getFiles(),
                    databricksDLTWorkloadSpecific.getCatalog(),
                    databricksDLTWorkloadSpecific.getTarget(),
                    databricksDLTWorkloadSpecific.getPhoton(),
                    notifications,
                    databricksDLTWorkloadSpecific.getChannel(),
                    databricksDLTWorkloadSpecific.getCluster());
            if (eitherCreatedPipeline.isLeft()) return left(eitherCreatedPipeline.getLeft());

            String pipelineUrl = "https://" + databricksWorkspaceInfo.getDatabricksHost() + "/pipelines/"
                    + eitherCreatedPipeline.get();
            logger.info(String.format(
                    "New pipeline linked to component %s available at %s",
                    provisionRequest.component().getName(), pipelineUrl));

            return right(eitherCreatedPipeline.get());

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while provisioning component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Unprovisions a Databricks Delta Live Tables (DLT) pipeline workload.
     *
     * @param provisionRequest the request containing the details for unprovisioning
     * @param workspaceClient the Databricks workspace client
     * @param databricksWorkspaceInfo information about the Databricks workspace
     * @return Either a FailedOperation or Void if successful
     */
    public Either<FailedOperation, Void> unprovisionWorkload(
            ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {
        try {
            DatabricksDLTWorkloadSpecific databricksDLTWorkloadSpecific =
                    provisionRequest.component().getSpecific();

            var deltaLiveTablesManager = new DLTManager(workspaceClient, databricksWorkspaceInfo.getName());

            Either<FailedOperation, Iterable<PipelineStateInfo>> eitherGetPipelines =
                    deltaLiveTablesManager.listPipelinesWithGivenName(databricksDLTWorkloadSpecific.getPipelineName());

            if (eitherGetPipelines.isLeft()) return left(eitherGetPipelines.getLeft());

            Iterable<PipelineStateInfo> pipelines = eitherGetPipelines.get();

            List<Problem> problems = new ArrayList<>();

            pipelines.forEach(pipelineStateInfo -> {
                Either<FailedOperation, Void> result =
                        deltaLiveTablesManager.deletePipeline(pipelineStateInfo.getPipelineId());
                if (result.isLeft()) problems.addAll(result.getLeft().problems());
            });

            if (!problems.isEmpty()) {
                return Either.left(new FailedOperation(problems));
            }

            if (provisionRequest.removeData()) {
                var repoManager = new RepoManager(workspaceClient, databricksWorkspaceInfo.getName());

                String repoPath = databricksDLTWorkloadSpecific.getRepoPath();
                if (!repoPath.startsWith("/")) repoPath = "/" + repoPath;

                Either<FailedOperation, Void> eitherDeletedRepo = repoManager.deleteRepo(
                        provisionRequest.component().getSpecific().getGit().getGitRepoUrl(), repoPath);

                if (eitherDeletedRepo.isLeft()) return left(eitherDeletedRepo.getLeft());
            } else
                logger.info(
                        "The repository with URL '{}' associated with component '{}' will not be removed from the Workspace because the 'removeData' flag is set to 'false'. Provision request details: [Component: {}, Repo URL: {}, Remove Data: {}].",
                        provisionRequest.component().getSpecific().getGit().getGitRepoUrl(),
                        provisionRequest.component().getName(),
                        provisionRequest.component().getName(),
                        provisionRequest.component().getSpecific().getGit().getGitRepoUrl(),
                        provisionRequest.removeData());

            return right(null);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while unprovisioning component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
