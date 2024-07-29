package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.ResourceAlreadyExists;
import com.databricks.sdk.service.workspace.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import java.util.Collections;
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
     * @param gitUrl         The URL of the Git repository to be linked to the Databricks repo.
     * @param provider       The Git provider (e.g., GitHub, GitLab).
     * @param absoluteRepoPath The absolute path where the repository will be created in the Databricks workspace.
     * @return Either a FailedOperation if an exception occurs, or Void if successful.
     */
    public Either<FailedOperation, Void> createRepo(String gitUrl, String provider, String absoluteRepoPath) {
        try {
            logger.info(
                    String.format("Creating repo with URL %s at %s in %s", gitUrl, absoluteRepoPath, workspaceName));
            workspaceClient
                    .repos()
                    .create(new CreateRepo()
                            .setUrl(gitUrl)
                            .setProvider(provider)
                            .setPath(absoluteRepoPath));
            logger.info(String.format(
                    "Repo with URL %s created successfully at %s in %s", gitUrl, absoluteRepoPath, workspaceName));

            return right(null);
        } catch (ResourceAlreadyExists e) {

            logger.warn(String.format(
                    "Creation of repo with URL %s skipped. It already exists in the folder %s (workspace %s).",
                    gitUrl, absoluteRepoPath, workspaceName));

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
     * @param gitUrl       The URL of the Git repository to be deleted.
     * @param absoluteRepoPath The absolute path of the repository to be deleted in the Databricks workspace.
     * @return Either a FailedOperation if an exception occurs, or Void if successful.
     */
    public Either<FailedOperation, Void> deleteRepo(String gitUrl, String absoluteRepoPath) {
        try {

            logger.info(String.format(
                    "Attempting to delete repo with path %s and URL %s in %s.",
                    absoluteRepoPath, gitUrl, workspaceName));

            int lastIndex = absoluteRepoPath.lastIndexOf('/');
            String dataProductFolderPath = absoluteRepoPath.substring(0, lastIndex);

            Iterable<ObjectInfo> dataProductFolderContent =
                    workspaceClient.workspace().list(dataProductFolderPath);

            if (dataProductFolderContent != null) {
                for (ObjectInfo objectInfo : dataProductFolderContent) {
                    if (objectInfo.getObjectType().equals(ObjectType.REPO)) {
                        RepoInfo repoInfo = workspaceClient.repos().get(objectInfo.getObjectId());
                        if (repoInfo.getUrl().equalsIgnoreCase(gitUrl)
                                && repoInfo.getPath().equalsIgnoreCase(absoluteRepoPath)) {
                            workspaceClient.repos().delete(new DeleteRepoRequest().setRepoId(repoInfo.getId()));
                            logger.info(String.format(
                                    "Repo %s (id: %s) in %s deleted successfully.",
                                    absoluteRepoPath, repoInfo.getId(), workspaceName));
                            return right(null);
                        }
                    }
                }
            }

            logger.info(String.format(
                    "Repo with path %s and URL %s not found in %s. Deletion skipped.",
                    absoluteRepoPath, gitUrl, workspaceName));
            return right(null);

        } catch (Exception e) {

            String errorMessage = String.format(
                    "An error occurred while deleting the repo with path %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    absoluteRepoPath, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
