package it.agilelab.witboost.provisioning.databricks.bean;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.workspace.CreateCredentials;
import com.databricks.sdk.service.workspace.CredentialInfo;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.GitProvider;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

public class DatabricksWorkspaceClientBean implements FactoryBean<WorkspaceClient> {

    private WorkspaceClient workspaceClient;
    private String workspaceHost;
    private DatabricksAuthConfig databricksAuthConfig;
    private AzureAuthConfig azureAuthConfig;
    private GitCredentialsConfig gitCredentialsConfig;
    private final Logger logger = LoggerFactory.getLogger(DatabricksWorkspaceClientBean.class);

    public DatabricksWorkspaceClientBean(
            String workspaceHost,
            DatabricksAuthConfig databricksAuthConfig,
            AzureAuthConfig azureAuthConfig,
            GitCredentialsConfig gitCredentialsConfig) {
        this.workspaceHost = workspaceHost;
        this.databricksAuthConfig = databricksAuthConfig;
        this.azureAuthConfig = azureAuthConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
    }

    @Override
    public WorkspaceClient getObject() {
        setWorkspaceClient(initializeWorkspaceClient());
        return workspaceClient;
    }

    @Override
    public Class<?> getObjectType() {
        return WorkspaceClient.class;
    }

    private WorkspaceClient initializeWorkspaceClient() {
        try {
            DatabricksConfig config = new DatabricksConfig()
                    .setHost(workspaceHost)
                    .setAccountId(databricksAuthConfig.getAccountId())
                    .setAzureTenantId(azureAuthConfig.getTenantId())
                    .setAzureClientId(azureAuthConfig.getClientId())
                    .setAzureClientSecret(azureAuthConfig.getClientSecret());

            workspaceClient = new WorkspaceClient(config);

            setGitCredentials(gitCredentialsConfig.getToken(), gitCredentialsConfig.getUsername(), GitProvider.GITLAB);

            return workspaceClient;

        } catch (Exception e) {
            logger.error("Error initializing the workspaceClient: {}", e.getMessage(), e);
            throw new RuntimeException("Unable to initialize workspace client", e);
        }
    }

    protected void setGitCredentials(String personalAccessToken, String gitUsername, GitProvider gitProvider) {
        try {

            Iterable<CredentialInfo> listCredentials =
                    workspaceClient.gitCredentials().list();

            boolean credentialsExist = false;

            if (listCredentials != null) {
                credentialsExist = StreamSupport.stream(listCredentials.spliterator(), false)
                        .anyMatch(credentialInfo ->
                                credentialInfo.getGitProvider().equalsIgnoreCase(gitProvider.name()));
            }

            if (!credentialsExist)
                workspaceClient
                        .gitCredentials()
                        .create(new CreateCredentials()
                                .setPersonalAccessToken(personalAccessToken)
                                .setGitUsername(gitUsername)
                                .setGitProvider(gitProvider.name()));
            else {
                logger.warn(
                        "Credentials for {} already exist, skipping creation.",
                        gitProvider.toString().toUpperCase());
            }
        } catch (Exception e) {
            logger.error("Error setting Git credentials: {}", e.getMessage(), e);
            throw new RuntimeException("Unable to set Git credentials", e);
        }
    }

    public void setWorkspaceClient(WorkspaceClient workspaceClient) {
        this.workspaceClient = workspaceClient;
    }

    public void setWorkspaceHost(String workspaceHost) {
        this.workspaceHost = workspaceHost;
    }
}
