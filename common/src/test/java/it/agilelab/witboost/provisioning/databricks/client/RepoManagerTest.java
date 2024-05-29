package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.API.Left;
import static io.vavr.API.Right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.ResourceAlreadyExists;
import com.databricks.sdk.service.workspace.CreateRepo;
import com.databricks.sdk.service.workspace.RepoInfo;
import com.databricks.sdk.service.workspace.ReposAPI;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitProvider;
import java.util.Collections;
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
        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.create(any(CreateRepo.class))).thenThrow(new ResourceAlreadyExists("error", null));

        Either<FailedOperation, Void> result = repoManager.createRepo("gitUrl", GitProvider.GITLAB);

        assert result.isRight();
        assertNull(result.get());
    }

    @Test
    public void createRepo_Exception() {

        ReposAPI reposAPI = mock(ReposAPI.class);
        when(workspaceClient.repos()).thenReturn(reposAPI);
        RepoInfo repoInfo = mock(RepoInfo.class);
        when(reposAPI.create(any(CreateRepo.class))).thenThrow(new RuntimeException("This is an exception"));

        Either<FailedOperation, Void> result = repoManager.createRepo("gitUrl", GitProvider.GITLAB);

        FailedOperation expectedRes =
                new FailedOperation(Collections.singletonList(new Problem("This is an exception")));
        assert result.isLeft();
        assertEquals(result, Left(expectedRes));
    }
}
