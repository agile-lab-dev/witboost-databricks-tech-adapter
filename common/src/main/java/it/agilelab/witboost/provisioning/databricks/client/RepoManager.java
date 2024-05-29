package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.ResourceAlreadyExists;
import com.databricks.sdk.service.workspace.CreateRepo;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitProvider;
import java.util.Collections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RepoManager {

    WorkspaceClient workspaceClient;

    public RepoManager(WorkspaceClient workspaceClient) {
        this.workspaceClient = workspaceClient;
    }

    private final Logger logger = LoggerFactory.getLogger(RepoManager.class);

    public Either<FailedOperation, Void> createRepo(String gitUrl, GitProvider provider) {
        try {
            logger.info("Creating repo with Git url: {}", gitUrl);

            workspaceClient.repos().create(new CreateRepo().setUrl(gitUrl).setProvider(provider.toString()));
            logger.info("Repo with url {} created successfully.", gitUrl);

            return right(null);
        } catch (ResourceAlreadyExists e) {
            logger.warn("Repo with url {} already exists, creation skipped.", gitUrl);
            return right(null);
        } catch (Exception e) {
            logger.error("Failed to create repo with url {}", gitUrl, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(e.getMessage()))));
        }
    }
}
