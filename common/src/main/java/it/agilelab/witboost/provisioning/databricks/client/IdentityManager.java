package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.service.iam.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import java.util.*;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IdentityManager {

    private final AccountClient accountClient;
    private final DatabricksWorkspaceInfo databricksWorkspaceInfo;

    public IdentityManager(AccountClient accountClient, DatabricksWorkspaceInfo databricksWorkspaceInfo) {
        this.accountClient = accountClient;
        this.databricksWorkspaceInfo = databricksWorkspaceInfo;
    }

    private final Logger logger = LoggerFactory.getLogger(IdentityManager.class);

    /**
     * Creates or updates a user in the workspace based on the user details from the account. User will have ADMIN
     * privileges.
     *
     * This method first checks if the user exists in the account. If the user exists, it creates or update the user in the
     * workspace importing it from the account.
     *
     * @param username The username of the user to create or update.
     * @return An Either object containing a FailedOperation if the operation failed, or Void if the operation succeeded.
     */
    public Either<FailedOperation, Void> createOrUpdateUserWithAdminPrivileges(String username) {

        try {

            logger.info(String.format("Importing/updating user %s in %s", username, databricksWorkspaceInfo.getName()));
            String filter = String.format("username eq '%s'", username);
            Collection<WorkspacePermission> workspacePermissions = new ArrayList<>();
            workspacePermissions.add(WorkspacePermission.ADMIN);
            // Check user in account
            Optional<User> accountUser = StreamSupport.stream(
                            accountClient
                                    .users()
                                    .list(new ListAccountUsersRequest().setFilter(filter))
                                    .spliterator(),
                            false)
                    .findFirst();

            if (accountUser.isPresent()) {
                accountClient
                        .workspaceAssignment()
                        .update(new UpdateWorkspaceAssignments()
                                .setPrincipalId(Long.parseLong(accountUser.get().getId()))
                                .setPermissions(workspacePermissions)
                                .setWorkspaceId(Long.parseLong(databricksWorkspaceInfo.getId())));

            } else {
                String errorMessage = String.format("User %s not found at Databricks account level.", username);
                logger.warn(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }

            return Either.right(null);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating/updating user %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    username, databricksWorkspaceInfo.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Creates or updates a group in the workspace based on the user details from the account. Group will have USER
     * permissions.
     *
     * This method first checks if the group exists in the account. If the group exists, it creates or update the group in the
     * workspace importing it from the account.
     *
     * @param groupName The username of the user to create or update.
     * @return An Either object containing a FailedOperation if the operation failed, or Void if the operation succeeded.
     */
    public Either<FailedOperation, Void> createOrUpdateGroupWithUserPrivileges(String groupName) {

        try {
            logger.info(
                    String.format("Importing/updating group %s in %s", groupName, databricksWorkspaceInfo.getName()));

            String filter = String.format("displayName eq '%s'", groupName);
            Collection<WorkspacePermission> workspacePermissions = new ArrayList<>();
            workspacePermissions.add(WorkspacePermission.USER);
            // Check group in account
            Optional<Group> accountGroup = StreamSupport.stream(
                            accountClient
                                    .groups()
                                    .list(new ListAccountGroupsRequest().setFilter(filter))
                                    .spliterator(),
                            false)
                    .findFirst();

            if (accountGroup.isPresent()) {
                accountClient
                        .workspaceAssignment()
                        .update(new UpdateWorkspaceAssignments()
                                .setPrincipalId(
                                        Long.parseLong(accountGroup.get().getId()))
                                .setPermissions(workspacePermissions)
                                .setWorkspaceId(Long.parseLong(databricksWorkspaceInfo.getId())));

            } else {
                String errorMessage = String.format("Group %s not found at Databricks account level.", groupName);
                logger.warn(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }

            return Either.right(null);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while creating/updating group %s in %s. Please try again and if the error persists contact the platform team. Details: %s",
                    groupName, databricksWorkspaceInfo.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }
}
