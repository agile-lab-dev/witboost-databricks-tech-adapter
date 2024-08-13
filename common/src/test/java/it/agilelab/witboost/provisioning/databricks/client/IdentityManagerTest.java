package it.agilelab.witboost.provisioning.databricks.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.azure.resourcemanager.databricks.models.ProvisioningState;
import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.iam.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class IdentityManagerTest {

    @Mock
    private WorkspaceClient workspaceClient;

    @Mock
    private AccountClient accountClient;

    @Mock
    private UsersAPI usersAPI;

    @Mock
    AccountUsersAPI accountUsersAPI;

    @Mock
    private GroupsAPI groupsAPI;

    @Mock
    private AccountGroupsAPI accountGroupsAPI;

    private IdentityManager identityManager;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        identityManager = new IdentityManager(
                accountClient,
                new DatabricksWorkspaceInfo(
                        "workspaceName", "123", "test", "test", "test", ProvisioningState.SUCCEEDED));
        when(workspaceClient.users()).thenReturn(usersAPI);
        when(workspaceClient.groups()).thenReturn(groupsAPI);
        when(accountClient.groups()).thenReturn(accountGroupsAPI);
        when(accountClient.users()).thenReturn(accountUsersAPI);
    }

    @Test
    public void createOrUpdateUser_Success() {
        String username = "testuser";
        User accountUser = new User().setUserName(username).setId("123");

        when(accountClient.users().list(any(ListAccountUsersRequest.class)))
                .thenReturn(Collections.singleton(accountUser));
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));
        Either<FailedOperation, Void> result = identityManager.createOrUpdateUserWithAdminPrivileges(username);

        assertTrue(result.isRight());
        verify(accountClient.workspaceAssignment(), times(1)).update(any());
    }

    @Test
    public void createOrUpdateUser_UserNotFound() {
        String username = "testuser";

        when(accountClient.users().list(any(ListAccountUsersRequest.class))).thenReturn(Collections.emptyList());

        Either<FailedOperation, Void> result = identityManager.createOrUpdateUserWithAdminPrivileges(username);

        assertTrue(result.isLeft());
        assertEquals(
                "User testuser not found at Databricks account level.",
                result.getLeft().problems().get(0).description());
        verify(usersAPI, never()).create(any(User.class));
        verify(usersAPI, never()).update(any(User.class));
    }

    @Test
    public void createOrUpdateUser_Exception() {
        String username = "testuser";
        String errorMessage = "This is an exception";

        when(accountClient.users().list(any(ListAccountUsersRequest.class)))
                .thenThrow(new RuntimeException(errorMessage));

        Either<FailedOperation, Void> result = identityManager.createOrUpdateUserWithAdminPrivileges(username);

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void createOrUpdateGroup_Success() {
        String groupName = "testgroup";
        Group accountGroup = new Group().setDisplayName(groupName).setId("123");

        when(accountClient.groups().list(any(ListAccountGroupsRequest.class)))
                .thenReturn(Collections.singleton(accountGroup));
        when(groupsAPI.list(any(ListGroupsRequest.class))).thenReturn(Collections.emptyList());
        when(accountClient.workspaceAssignment()).thenReturn(mock(WorkspaceAssignmentAPI.class));

        Either<FailedOperation, Void> result = identityManager.createOrUpdateGroupWithUserPrivileges(groupName);

        assertTrue(result.isRight());
        verify(accountClient.workspaceAssignment(), times(1)).update(any());
    }

    @Test
    public void createOrUpdateGroup_GroupNotFound() {
        String groupName = "testgroup";

        when(accountClient.groups().list(any(ListAccountGroupsRequest.class))).thenReturn(Collections.emptyList());

        Either<FailedOperation, Void> result = identityManager.createOrUpdateGroupWithUserPrivileges(groupName);

        assertTrue(result.isLeft());
        assertEquals(
                "Group testgroup not found at Databricks account level.",
                result.getLeft().problems().get(0).description());
        verify(groupsAPI, never()).create(any(Group.class));
        verify(groupsAPI, never()).update(any(Group.class));
    }

    @Test
    public void createOrUpdateGroup_Exception() {
        String groupName = "testgroup";
        String errorMessage = "This is an exception";

        when(accountClient.groups().list(any(ListAccountGroupsRequest.class)))
                .thenThrow(new RuntimeException(errorMessage));

        Either<FailedOperation, Void> result = identityManager.createOrUpdateGroupWithUserPrivileges(groupName);

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }
}
