package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.BadRequest;
import com.databricks.sdk.core.error.platform.ResourceAlreadyExists;
import com.databricks.sdk.core.error.platform.ResourceDoesNotExist;
import com.databricks.sdk.service.workspace.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepoManager {

    private final WorkspaceClient workspaceClient;
    private final String workspaceName;

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
     * @return Either a FailedOperation if an exception occurs, or Long (repository ID) if successful.
     */
    public Either<FailedOperation, Long> createRepo(String gitUrl, String provider, String absoluteRepoPath) {
        try {
            logger.info(
                    String.format("Creating repo with URL %s at %s in %s", gitUrl, absoluteRepoPath, workspaceName));
            var repoInfo = workspaceClient
                    .repos()
                    .create(new CreateRepo()
                            .setUrl(gitUrl)
                            .setProvider(provider)
                            .setPath(absoluteRepoPath));
            logger.info(String.format(
                    "Repo with URL %s created successfully at %s in %s", gitUrl, absoluteRepoPath, workspaceName));

            return right(repoInfo.getId());

        } catch (BadRequest e) {
            if (e.getMessage().contains("RESOURCE_ALREADY_EXISTS")) {
                return handleExistingRepository(gitUrl, absoluteRepoPath);
            }
            String errorMessage = String.format(
                    "An error occurred while creating the repo with URL %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    gitUrl, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        } catch (ResourceAlreadyExists e) {
            return handleExistingRepository(gitUrl, absoluteRepoPath);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating the repo with URL %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    gitUrl, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    @NotNull
    private Either<FailedOperation, Long> handleExistingRepository(String gitUrl, String absoluteRepoPath) {
        logger.warn(String.format(
                "Creation of repo with URL %s skipped. It already exists in the folder %s (workspace %s).",
                gitUrl, absoluteRepoPath, workspaceName));

        int lastIndex = absoluteRepoPath.lastIndexOf('/');
        String dataProductFolderPath = absoluteRepoPath.substring(0, lastIndex);

        Iterable<ObjectInfo> dataProductFolderContent =
                workspaceClient.workspace().list(dataProductFolderPath);

        if (dataProductFolderContent != null) {
            for (ObjectInfo objectInfo : dataProductFolderContent) {
                if (objectInfo.getObjectType().equals(ObjectType.REPO)) {
                    RepoInfo repoInfo = workspaceClient.repos().get(objectInfo.getObjectId());
                    if (repoInfo.getPath().equalsIgnoreCase(absoluteRepoPath)) {
                        return right(repoInfo.getId());
                    }
                }
            }
        }

        String errorMessage = String.format(
                "An error occurred while creating the repo with URL %s in %s. Please try again and if the error persists contact the platform team. Details: seems that the repository already exists but it is impossible to retrieve information about it.",
                gitUrl, workspaceName);
        return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
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

        } catch (ResourceDoesNotExist e) {
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

    public Either<FailedOperation, Void> assignPermissionsToUser(
            String repoId, String username, RepoPermissionLevel permissionLevel) {
        return assignPermissions(repoId, username, null, permissionLevel);
    }

    public Either<FailedOperation, Void> removePermissionsToUser(String repoId, String username) {
        return removePermissions(repoId, username, null);
    }

    public Either<FailedOperation, Void> assignPermissionsToGroup(
            String repoId, String groupName, RepoPermissionLevel permissionLevel) {
        return assignPermissions(repoId, null, groupName, permissionLevel);
    }

    public Either<FailedOperation, Void> removePermissionsToGroup(String repoId, String groupName) {
        return removePermissions(repoId, null, groupName);
    }

    /**
     * Assigns permissions to a user or group for a repository.
     *
     * @param repoId           The ID of the repository.
     * @param username         The username to assign permissions to, or null if assigning to a group.
     * @param groupName        The group name to assign permissions to, or null if assigning to a user.
     * @param permissionLevel  The permission level to assign.
     * @return Either a FailedOperation if an exception occurs, or Void if successful.
     */
    private Either<FailedOperation, Void> assignPermissions(
            String repoId, String username, String groupName, RepoPermissionLevel permissionLevel) {
        try {
            String logMessage = String.format(
                    "Assigning configured permissions %s to %s for repository %s (workspace %s)",
                    permissionLevel, username != null ? username : groupName, repoId, workspaceName);
            logger.info(logMessage);
            RepoPermissions repoPermissions = workspaceClient.repos().getPermissions(repoId);
            Collection<RepoAccessControlRequest> accessControlRequests = getAccessControlRequests(repoPermissions);
            accessControlRequests.add(new RepoAccessControlRequest()
                    .setUserName(username)
                    .setGroupName(groupName)
                    .setPermissionLevel(permissionLevel));

            workspaceClient
                    .repos()
                    .setPermissions(
                            new RepoPermissionsRequest().setRepoId(repoId).setAccessControlList(accessControlRequests));
            return right(null);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error assigning configured permissions %s to %s for repository %s (workspace %s). Details: %s",
                    permissionLevel, username != null ? username : groupName, repoId, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Removes permissions from a user or group for a repository.
     *
     * @param repoId    The ID of the repository.
     * @param username  The username to remove permissions from, or null if removing from a group.
     * @param groupName The group name to remove permissions from, or null if removing from a user.
     * @return Either a FailedOperation if an exception occurs, or Void if successful.
     */
    private Either<FailedOperation, Void> removePermissions(String repoId, String username, String groupName) {
        try {
            String logMessage = String.format(
                    "Removing permissions to %s for repository %s (workspace %s)",
                    username != null ? username : groupName, repoId, workspaceName);
            logger.info(logMessage);
            RepoPermissions repoPermissions = workspaceClient.repos().getPermissions(repoId);
            Collection<RepoAccessControlRequest> accessControlRequests = getAccessControlRequests(repoPermissions);
            accessControlRequests.removeIf(request -> username != null
                    ? username.equals(request.getUserName())
                    : groupName.equals(request.getGroupName()));
            workspaceClient
                    .repos()
                    .setPermissions(
                            new RepoPermissionsRequest().setRepoId(repoId).setAccessControlList(accessControlRequests));
            return right(null);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error removing permissions to %s for repository %s (workspace %s). Details: %s",
                    username != null ? username : groupName, repoId, workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Converts the current repository permissions to a collection of RepoAccessControlRequest objects.
     *
     * @param repoPermissions The current permissions of the repository.
     * @return A collection of RepoAccessControlRequest objects.
     */
    private Collection<RepoAccessControlRequest> getAccessControlRequests(RepoPermissions repoPermissions) {
        Collection<RepoAccessControlRequest> accessControlRequests = new ArrayList<>();
        Collection<RepoAccessControlResponse> accessControlResponses = repoPermissions.getAccessControlList();
        if (accessControlResponses != null) {
            accessControlRequests.addAll(accessControlResponses.stream()
                    .map(this::convertRepoResponseToRequest)
                    .toList());
        }
        return accessControlRequests;
    }

    /**
     * Converts a RepoAccessControlResponse object to a RepoAccessControlRequest object.
     *
     * @param response The RepoAccessControlResponse object to convert.
     * @return A new RepoAccessControlRequest object.
     */
    private RepoAccessControlRequest convertRepoResponseToRequest(RepoAccessControlResponse response) {
        return new RepoAccessControlRequest()
                .setUserName(response.getUserName())
                .setGroupName(response.getGroupName())
                .setPermissionLevel(
                        response.getAllPermissions().stream().findFirst().get().getPermissionLevel())
                .setServicePrincipalName(response.getServicePrincipalName());
    }
}
