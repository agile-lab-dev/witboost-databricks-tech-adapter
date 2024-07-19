package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.ResourceAlreadyExists;
import com.databricks.sdk.service.workspace.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepoManager {

    private WorkspaceClient workspaceClient;
    private String workspaceName;

    public RepoManager(WorkspaceClient workspaceClient, String workspaceName) {
        this.workspaceClient = workspaceClient;
        this.workspaceName = workspaceName;
    }

    private final Logger logger = LoggerFactory.getLogger(RepoManager.class);

    /**
     * Creates a new repository in the Databricks workspace.
     *
     * @param gitUrl   The URL of the Git repository to be linked to the Databricks repo.
     * @param provider The Git provider (e.g., GitHub, GitLab).
     * @return Either a FailedOperation if an exception occurs, or Void if successful.
     */
    public Either<FailedOperation, Void> createRepo(String gitUrl, String provider) {
        try {
            logger.info("Creating repo with Git url: {} in {}", gitUrl, workspaceName);
            workspaceClient.repos().create(new CreateRepo().setUrl(gitUrl).setProvider(provider));
            logger.info("Repo with url {} created successfully in {}.", gitUrl, workspaceName);

            return right(null);
        } catch (ResourceAlreadyExists e) {
            logger.warn("Repo with url {} already exists in {}, creation skipped.", gitUrl, workspaceName);
            return right(null);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating the repo with URL %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    gitUrl, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Deletes a repository from the Databricks workspace.
     *
     * @param gitUrl  The URL of the Git repository to be deleted.
     * @param account The account under which the repository exists.
     * @return Either a FailedOperation if an exception occurs, or Void if successful.
     */
    public Either<FailedOperation, Void> deleteRepo(String gitUrl, String account) {
        try {

            logger.info(
                    "Attempting to delete repo with Git url: {} for account: {} in {}", gitUrl, account, workspaceName);

            AtomicBoolean deleted = new AtomicBoolean(false);
            List<RepoInfo> repos = new ArrayList<>();

            // List all objects in the user's workspace
            Iterable<ObjectInfo> workspaceContent =
                    workspaceClient.workspace().list(String.format("/Workspace/Users/%s", account));

            if (workspaceContent != null) {
                workspaceContent.forEach(workspace -> {
                    if (workspace.getObjectType() == ObjectType.REPO) {
                        RepoInfo repoInfo = workspaceClient.repos().get(workspace.getObjectId());
                        repos.add(repoInfo);
                    }
                });
            } else {
                logger.warn("Empty workspace found for account: {} in {}", account, workspaceName);
            }

            // Find and delete the repo with the specified URL
            repos.stream()
                    .filter(repoInfo -> repoInfo.getUrl().equalsIgnoreCase(gitUrl))
                    .findFirst()
                    .ifPresent(repoInfo -> {
                        logger.info("Deleting repo with ID: {}", repoInfo.getId());
                        workspaceClient.repos().delete(new DeleteRepoRequest().setRepoId(repoInfo.getId()));
                        logger.info("Repo with url {} in {} deleted successfully.", gitUrl, workspaceName);
                        deleted.set(true);
                    });

            if (!deleted.get()) {
                logger.info("Repo with url {} not found in {} . Deletion skipped.", gitUrl, workspaceName);
            }

            return right(null);
        } catch (Exception e) {

            String errorMessage = String.format(
                    "An error occurred while deleting the repo with URL %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    gitUrl, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
