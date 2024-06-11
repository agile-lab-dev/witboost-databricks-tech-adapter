package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.API.Left;
import static io.vavr.API.Right;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.ResourceAlreadyExists;
import com.databricks.sdk.service.workspace.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitProvider;
import java.util.Collections;
import java.util.Iterator;
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

        Either<FailedOperation, Void> result = repoManager.createRepo("gitUrl", GitProvider.GITLAB);

        assert result.isRight();
        assertEquals(result, Right(null));
    }

    @Test
    public void createRepo_ResourceAlreadyExists() {

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        when(reposAPI.create(any(CreateRepo.class))).thenThrow(new ResourceAlreadyExists("error", null));

        Either<FailedOperation, Void> result = repoManager.createRepo("gitUrl", GitProvider.GITLAB);

        assert result.isRight();
        assertNull(result.get());
    }

    @Test
    public void createRepo_Exception() {

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        when(reposAPI.create(any(CreateRepo.class))).thenThrow(new RuntimeException("This is an exception"));

        Either<FailedOperation, Void> result = repoManager.createRepo("gitUrl", GitProvider.GITLAB);

        FailedOperation expectedRes =
                new FailedOperation(Collections.singletonList(new Problem("This is an exception")));
        assert result.isLeft();
        assertEquals(result, Left(expectedRes));
    }

    @Test
    public void deleteRepo_Success() {
        ReposAPI reposAPI = mock(ReposAPI.class);
        WorkspaceAPI workspaceAPI = mock(WorkspaceAPI.class);
        Iterable<ObjectInfo> objectInfos = mock(Iterable.class);
        Iterator<ObjectInfo> iterator = mock(Iterator.class);
        ObjectInfo objectInfo = mock(ObjectInfo.class);
        RepoInfo repoInfo = mock(RepoInfo.class);

        when(workspaceClient.workspace()).thenReturn(workspaceAPI);
        when(workspaceAPI.list(anyString())).thenReturn(objectInfos);
        when(objectInfos.iterator()).thenReturn(iterator);
        when(iterator.hasNext()).thenReturn(true, false);
        when(iterator.next()).thenReturn(objectInfo);
        when(objectInfo.getObjectType()).thenReturn(ObjectType.REPO);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        when(reposAPI.get(anyLong())).thenReturn(repoInfo);
        when(repoInfo.getUrl()).thenReturn("gitUrl");

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
        when(workspaceAPI.list(anyString())).thenThrow(new RuntimeException("This is a workspace list exception"));

        Either<FailedOperation, Void> result = repoManager.deleteRepo("gitUrl", "testaccount");

        FailedOperation expectedRes =
                new FailedOperation(Collections.singletonList(new Problem("This is a workspace list exception")));
        assertTrue(result.isLeft());
        assertEquals(result, Left(expectedRes));
    }
}
