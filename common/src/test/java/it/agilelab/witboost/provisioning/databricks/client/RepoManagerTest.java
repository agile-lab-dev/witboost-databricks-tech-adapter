package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.API.Right;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.BadRequest;
import com.databricks.sdk.core.error.platform.ResourceAlreadyExists;
import com.databricks.sdk.service.workspace.*;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@EnableConfigurationProperties
@Import(TestConfig.class)
public class RepoManagerTest {

    @Mock
    WorkspaceClient workspaceClient;

    RepoManager repoManager;

    @Mock
    ReposAPI reposAPI;

    @BeforeEach
    public void setUp() {
        repoManager = new RepoManager(workspaceClient, "workspace");
        when(workspaceClient.repos()).thenReturn(reposAPI);
    }

    @Test
    public void createRepo_Success() {
        CreateRepoResponse repoInfo = mock(CreateRepoResponse.class);
        when(reposAPI.create(any(CreateRepoRequest.class))).thenReturn(repoInfo);
        when(repoInfo.getId()).thenReturn(123L);

        var result = repoManager.createRepo("gitUrl", "GITLAB", "/Users/testaccount/testfolder/testcomponent");

        assert result.isRight();
        assertEquals(Right(123L), result);
    }

    @Test
    public void createRepo_ResourceAlreadyExists_CreationSkipped() {
        when(reposAPI.create(any(CreateRepoRequest.class))).thenThrow(new ResourceAlreadyExists("error", null));
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));
        List<ObjectInfo> objectInfos = Arrays.asList(new ObjectInfo()
                .setObjectType(ObjectType.REPO)
                .setObjectId(123L)
                .setPath("/Users/testaccount/testfolder/testcomponent"));

        Iterable<ObjectInfo> objectInfoIterable = objectInfos;
        when(reposAPI.get(123L))
                .thenReturn(new GetRepoResponse()
                        .setPath("/Users/testaccount/testfolder/testcomponent")
                        .setId(123L));

        when(workspaceClient.workspace().list(anyString())).thenReturn(objectInfoIterable);
        var result = repoManager.createRepo("gitUrl", "GITLAB", "/Users/testaccount/testfolder/testcomponent");

        assert result.isRight();
        assertEquals(Right(123L), result);
    }

    @Test
    public void createRepo_BadRequest_ResourceAlreadyExists__CreationSkipped() {
        when(reposAPI.create(any(CreateRepoRequest.class)))
                .thenThrow(new BadRequest("RESOURCE_ALREADY_EXISTS error", null));
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));
        List<ObjectInfo> objectInfos = Arrays.asList(new ObjectInfo()
                .setObjectType(ObjectType.REPO)
                .setObjectId(123L)
                .setPath("/Users/testaccount/testfolder/testcomponent"));

        Iterable<ObjectInfo> objectInfoIterable = objectInfos;
        when(reposAPI.get(123L))
                .thenReturn(new GetRepoResponse()
                        .setPath("/Users/testaccount/testfolder/testcomponent")
                        .setId(123L));

        when(workspaceClient.workspace().list(anyString())).thenReturn(objectInfoIterable);
        var result = repoManager.createRepo("gitUrl", "GITLAB", "/Users/testaccount/testfolder/testcomponent");

        assert result.isRight();
        assertEquals(Right(123L), result);
    }

    @Test
    public void createRepo_ResourceAlreadyExists_ErrorRetrievingInfos() {
        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        when(reposAPI.create(any(CreateRepoRequest.class))).thenThrow(new ResourceAlreadyExists("error", null));
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));

        var result = repoManager.createRepo("gitUrl", "GITLAB", "/Users/testaccount/testfolder/testcomponent");
        assert result.isLeft();
        assert result.getLeft()
                .problems()
                .get(0)
                .description()
                .contains(
                        "Details: seems that the repository already exists but it is impossible to retrieve information about it.");
    }

    @Test
    public void createRepo_BadRequest() {
        when(reposAPI.create(any(CreateRepoRequest.class))).thenThrow(new BadRequest("error", null));
        when(workspaceClient.workspace()).thenReturn(mock(WorkspaceAPI.class));
        List<ObjectInfo> objectInfos = Arrays.asList(new ObjectInfo()
                .setObjectType(ObjectType.REPO)
                .setObjectId(123L)
                .setPath("/Users/testaccount/testfolder/testcomponent"));

        Iterable<ObjectInfo> objectInfoIterable = objectInfos;
        when(reposAPI.get(123L))
                .thenReturn(new GetRepoResponse()
                        .setPath("/Users/testaccount/testfolder/testcomponent")
                        .setId(123L));

        when(workspaceClient.workspace().list(anyString())).thenReturn(objectInfoIterable);
        var result = repoManager.createRepo("gitUrl", "GITLAB", "/Users/testaccount/testfolder/testcomponent");

        assert result.isLeft();
        String errorMessage = "An error occurred while creating the repo with URL gitUrl in workspace.";
        assert (result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void createRepo_Exception() {
        String errorMessage = "This is an exception ";
        when(reposAPI.create(any(CreateRepoRequest.class))).thenThrow(new RuntimeException(errorMessage));

        var result = repoManager.createRepo("gitUrl", "GITLAB", "/Users/testaccount/testfolder/testcomponent");
        assert result.isLeft();
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void deleteRepo_Success() {
        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        List<ObjectInfo> objectInfos =
                Arrays.asList(new ObjectInfo().setObjectType(ObjectType.REPO).setObjectId(123L));
        Iterable<ObjectInfo> objectInfoIterable = objectInfos;

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        when(workspaceAPI.list(anyString())).thenReturn(objectInfoIterable);

        GetRepoResponse repoInfo = mock(GetRepoResponse.class);
        when(reposAPI.get(anyLong())).thenReturn(repoInfo);
        when(repoInfo.getUrl()).thenReturn("gitUrl");
        when(repoInfo.getPath()).thenReturn("/Users/user/dataproduct/component");

        var result = repoManager.deleteRepo("gitUrl", "/Users/user/dataproduct/component");

        assertTrue(result.isRight());
        assertEquals(result, Right(null));
        verify(workspaceClient.repos(), times(1)).delete(any(DeleteRepoRequest.class));
    }

    @Test
    public void deleteRepo_EmptyWorkspace() {
        ReposAPI reposAPI = mock(ReposAPI.class);
        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        GetRepoResponse repoInfo = mock(GetRepoResponse.class);
        when(reposAPI.get(anyLong())).thenReturn(repoInfo);
        when(repoInfo.getUrl()).thenReturn("gitUrl");

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        when(workspaceAPI.list(anyString())).thenReturn(null);
        when(workspaceClient.repos()).thenReturn(reposAPI);

        var result = repoManager.deleteRepo("testaccount", "/Users/user/dataproduct/component");

        assertTrue(result.isRight());
        assertEquals(result, Right(null));
    }

    @Test
    public void deleteRepo_NotFound() {
        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        Iterable<ObjectInfo> objectInfos = mock(Iterable.class);
        Iterator<ObjectInfo> iterator = mock(Iterator.class);

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        when(workspaceAPI.list(anyString())).thenReturn(objectInfos);
        when(objectInfos.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);

        var result = repoManager.deleteRepo("testaccount", "/Users/user/dataproduct/error");

        assertTrue(result.isRight());
        assertEquals(result, Right(null));
    }

    @Test
    public void deleteRepo_WorkspaceListException() {
        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        String errorMessage = "This is a workspace list exception";
        when(workspaceAPI.list(anyString())).thenThrow(new RuntimeException(errorMessage));
        var result = repoManager.deleteRepo("testaccount", "/Users/user/dataproduct/component");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void assignPermissionsToUser_Success() {
        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        RepoAccessControlRequest controlRequest = new RepoAccessControlRequest()
                .setUserName("testUser")
                .setPermissionLevel(RepoPermissionLevel.CAN_MANAGE);

        when(reposAPI.getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());

        var result = repoManager.assignPermissionsToUser("repoId", "testUser", RepoPermissionLevel.CAN_MANAGE);

        assertTrue(result.isRight());
        verify(reposAPI, times(1)).setPermissions(any(RepoPermissionsRequest.class));
    }

    @Test
    public void removePermissionsToUser_Success() {
        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        RepoAccessControlResponse controlResponse = new RepoAccessControlResponse()
                .setUserName("testUser")
                .setAllPermissions(Collections.singletonList(
                        new RepoPermission().setPermissionLevel(RepoPermissionLevel.CAN_MANAGE)));

        List<RepoAccessControlResponse> responses = new ArrayList<>();
        responses.add(controlResponse);

        when(reposAPI.getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(responses);

        var result = repoManager.removePermissionsToUser("repoId", "testUser");

        assertTrue(result.isRight());
        verify(reposAPI, times(1)).setPermissions(any(RepoPermissionsRequest.class));
    }

    @Test
    public void assignPermissionsToGroup_Success() {
        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        RepoAccessControlRequest controlRequest = new RepoAccessControlRequest()
                .setGroupName("testGroup")
                .setPermissionLevel(RepoPermissionLevel.CAN_MANAGE);

        when(reposAPI.getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(Collections.emptyList());

        var result = repoManager.assignPermissionsToGroup("repoId", "testGroup", RepoPermissionLevel.CAN_MANAGE);

        assertTrue(result.isRight());
        verify(reposAPI, times(1)).setPermissions(any(RepoPermissionsRequest.class));
    }

    @Test
    public void removePermissionsToGroup_Success() {
        RepoPermissions repoPermissions = mock(RepoPermissions.class);
        RepoAccessControlResponse controlResponse = new RepoAccessControlResponse()
                .setGroupName("testGroup")
                .setAllPermissions(Collections.singletonList(
                        new RepoPermission().setPermissionLevel(RepoPermissionLevel.CAN_MANAGE)));

        List<RepoAccessControlResponse> responses = new ArrayList<>();
        responses.add(controlResponse);

        when(reposAPI.getPermissions(anyString())).thenReturn(repoPermissions);
        when(repoPermissions.getAccessControlList()).thenReturn(responses);

        var result = repoManager.removePermissionsToGroup("repoId", "testGroup");

        assertTrue(result.isRight());
        verify(reposAPI, times(1)).setPermissions(any(RepoPermissionsRequest.class));
    }

    @Test
    public void assignPermissions_Exception() {
        String errorMessage = "This is an exception";
        when(reposAPI.getPermissions(anyString())).thenThrow(new RuntimeException(errorMessage));

        var result = repoManager.assignPermissionsToUser("repoId", "testUser", RepoPermissionLevel.CAN_MANAGE);

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void removePermissions_Exception() {
        String errorMessage = "This is an exception";
        when(reposAPI.getPermissions(anyString())).thenThrow(new RuntimeException(errorMessage));

        var result = repoManager.removePermissionsToUser("repoId", "testUser");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }
}
