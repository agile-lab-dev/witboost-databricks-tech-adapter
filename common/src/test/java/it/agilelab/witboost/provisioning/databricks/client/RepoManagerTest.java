package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.API.Right;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.ResourceAlreadyExists;
import com.databricks.sdk.service.workspace.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
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

    @InjectMocks
    RepoManager repoManager;

    @Test
    public void createRepo_Success() {

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.create(any(CreateRepo.class))).thenReturn(repoInfo);

        Either<FailedOperation, Void> result = repoManager.createRepo("gitUrl", "GITLAB");

        assert result.isRight();
        assertEquals(result, Right(null));
    }

    @Test
    public void createRepo_ResourceAlreadyExists() {

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        when(reposAPI.create(any(CreateRepo.class))).thenThrow(new ResourceAlreadyExists("error", null));

        Either<FailedOperation, Void> result = repoManager.createRepo("gitUrl", "GITLAB");

        assert result.isRight();
        assertNull(result.get());
    }

    @Test
    public void createRepo_Exception() {
        String errorMessage = "This is an exception ";
        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        when(reposAPI.create(any(CreateRepo.class))).thenThrow(new RuntimeException(errorMessage));

        Either<FailedOperation, Void> result = repoManager.createRepo("gitUrl", "GITLAB");

        assert result.isLeft();

        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }

    @Test
    public void deleteRepo_Success() {
        ReposAPI reposAPI = mock(ReposAPI.class);
        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.get(anyLong())).thenReturn(repoInfo);
        when(repoInfo.getUrl()).thenReturn("gitUrl");

        List<ObjectInfo> objectInfos =
                Arrays.asList(new ObjectInfo().setObjectType(ObjectType.REPO).setObjectId(123l));
        Iterable<ObjectInfo> objectInfoIterable = objectInfos;

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        when(workspaceAPI.list(anyString())).thenReturn(objectInfoIterable);
        when(workspaceClient.repos()).thenReturn(reposAPI);

        Either<FailedOperation, Void> result = repoManager.deleteRepo("gitUrl", "testaccount");

        assertTrue(result.isRight());
        assertEquals(result, Right(null));
    }

    @Test
    public void deleteRepo_EmptyWorkspace() {
        ReposAPI reposAPI = mock(ReposAPI.class);
        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.get(anyLong())).thenReturn(repoInfo);
        when(repoInfo.getUrl()).thenReturn("gitUrl");

        List<ObjectInfo> objectInfos =
                Arrays.asList(new ObjectInfo().setObjectType(ObjectType.REPO).setObjectId(123l));
        Iterable<ObjectInfo> objectInfoIterable = objectInfos;

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        when(workspaceAPI.list(anyString())).thenReturn(null);
        when(workspaceClient.repos()).thenReturn(reposAPI);

        Either<FailedOperation, Void> result = repoManager.deleteRepo("gitUrl", "testaccount");

        assertTrue(result.isRight());
        assertEquals(result, Right(null));
    }

    @Test
    public void deleteRepo_NotFound() {
        ReposAPI reposAPI = mock(ReposAPI.class);
        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        Iterable<ObjectInfo> objectInfos = mock(Iterable.class);
        Iterator<ObjectInfo> iterator = mock(Iterator.class);

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        when(workspaceAPI.list(anyString())).thenReturn(objectInfos);
        when(objectInfos.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(false);

        Either<FailedOperation, Void> result = repoManager.deleteRepo("nonexistentGitUrl", "testaccount");

        assertTrue(result.isRight());
        assertEquals(result, Right(null));
    }

    @Test
    public void deleteRepo_WorkspaceListException() {
        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        String errorMessage = "This is a workspace list exception";
        when(workspaceAPI.list(anyString())).thenThrow(new RuntimeException(errorMessage));
        Either<FailedOperation, Void> result = repoManager.deleteRepo("gitUrl", "testaccount");

        assertTrue(result.isLeft());
        assertTrue(result.getLeft().problems().get(0).description().contains(errorMessage));
    }
}
