package it.agilelab.witboost.provisioning.databricks.client;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class WorkspaceLevelManagerFactory {

    private final AccountClient accountClient;

    public WorkspaceLevelManagerFactory(AccountClient accountClient) {
        this.accountClient = accountClient;
    }

    public WorkspaceLevelManager createDatabricksWorkspaceLevelManager(WorkspaceClient workspaceClient) {
        return new WorkspaceLevelManager(workspaceClient, accountClient);
    }
}
