package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.pipelines.PipelineStateInfo;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.DeltaLiveTablesManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.dlt.DatabricksDLTWorkloadSpecific;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class DLTWorkloadHandler {

    private final Logger logger = LoggerFactory.getLogger(DLTWorkloadHandler.class);

    private final AzureAuthConfig azureAuthConfig;
    private final GitCredentialsConfig gitCredentialsConfig;

    @Autowired
    public DLTWorkloadHandler(AzureAuthConfig azureAuthConfig, GitCredentialsConfig gitCredentialsConfig) {
        this.azureAuthConfig = azureAuthConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
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

            Either<FailedOperation, Void> eitherAttachedMetastore = unityCatalogManager.attachMetastore(
                    databricksDLTWorkloadSpecific.getMetastore(), databricksDLTWorkloadSpecific.getCatalog());

            if (eitherAttachedMetastore.isLeft()) return left(eitherAttachedMetastore.getLeft());

            Either<FailedOperation, Void> eitherCreatedRepo =
                    createRepository(provisionRequest, workspaceClient, databricksWorkspaceInfo.getName());
            if (eitherCreatedRepo.isLeft()) {
                return left(eitherCreatedRepo.getLeft());
            }

            var dltManager = new DeltaLiveTablesManager(workspaceClient, databricksWorkspaceInfo.getName());

            Either<FailedOperation, String> eitherCreatedPipeline = dltManager.createDLTPipeline(
                    databricksDLTWorkloadSpecific.getPipelineName(),
                    databricksDLTWorkloadSpecific.getProductEdition(),
                    databricksDLTWorkloadSpecific.getContinuous(),
                    databricksDLTWorkloadSpecific.getNotebooks(),
                    databricksDLTWorkloadSpecific.getFiles(),
                    databricksDLTWorkloadSpecific.getCatalog(),
                    databricksDLTWorkloadSpecific.getTarget(),
                    databricksDLTWorkloadSpecific.getPhoton(),
                    databricksDLTWorkloadSpecific.getNotificationsMails(),
                    databricksDLTWorkloadSpecific.getNotificationsAlerts(),
                    databricksDLTWorkloadSpecific.getChannel(),
                    databricksDLTWorkloadSpecific.getCluster());
            if (eitherCreatedPipeline.isLeft()) return left(eitherCreatedPipeline.getLeft());

            String pipelineUrl = "https://" + databricksWorkspaceInfo.getDatabricksHost() + "/pipelines/"
                    + eitherCreatedPipeline.get();
            logger.info(String.format("New pipeline available at %s", pipelineUrl));

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

            var deltaLiveTablesManager = new DeltaLiveTablesManager(workspaceClient, databricksWorkspaceInfo.getName());

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
                repoPath = String.format("/Users/%s/%s", azureAuthConfig.getClientId(), repoPath);

                Either<FailedOperation, Void> eitherDeletedRepo = repoManager.deleteRepo(
                        provisionRequest.component().getSpecific().getGit().getGitRepoUrl(), repoPath);

                if (eitherDeletedRepo.isLeft()) return left(eitherDeletedRepo.getLeft());
            }

            return right(null);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while unprovisioning component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Creates a repository in the Databricks workspace.
     *
     * @param provisionRequest the request containing the details for creating the repository
     * @param workspaceClient the Databricks workspace client
     * @param workspaceName the name of the Databricks workspace
     * @return Either a FailedOperation or Void if successful
     */
    private Either<FailedOperation, Void> createRepository(
            ProvisionRequest<DatabricksDLTWorkloadSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            String workspaceName) {
        try {
            DatabricksDLTWorkloadSpecific databricksDLTWorkloadSpecific =
                    provisionRequest.component().getSpecific();

            String gitRepo = databricksDLTWorkloadSpecific.getGit().getGitRepoUrl();
            String repoPath = databricksDLTWorkloadSpecific.getRepoPath();

            int lastIndex = repoPath.lastIndexOf('/');
            String folderPath = repoPath.substring(0, lastIndex);
            workspaceClient
                    .workspace()
                    .mkdirs(String.format("/Users/%s/%s", azureAuthConfig.getClientId(), folderPath));

            repoPath = String.format("/Users/%s/%s", azureAuthConfig.getClientId(), repoPath);

            var repoManager = new RepoManager(workspaceClient, workspaceName);
            return repoManager.createRepo(gitRepo, gitCredentialsConfig.getProvider(), repoPath);

        } catch (Exception e) {
            DatabricksDLTWorkloadSpecific specific =
                    provisionRequest.component().getSpecific();
            String errorMessage = String.format(
                    "An error occurred while creating repository %s in %s for component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    specific.getGit().getGitRepoUrl(),
                    workspaceName,
                    provisionRequest.component().getName(),
                    e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
