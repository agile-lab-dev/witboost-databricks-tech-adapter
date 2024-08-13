package it.agilelab.witboost.provisioning.databricks.bean;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.workspace.CreateCredentials;
import com.databricks.sdk.service.workspace.CredentialInfo;
import com.databricks.sdk.service.workspace.UpdateCredentials;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import java.util.Optional;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;

public class DatabricksWorkspaceClientBean implements FactoryBean<WorkspaceClient> {

    private WorkspaceClient workspaceClient;
    private String workspaceHost;
    private String workspaceName;
    private DatabricksAuthConfig databricksAuthConfig;
    private AzureAuthConfig azureAuthConfig;
    private GitCredentialsConfig gitCredentialsConfig;
    private final Logger logger = LoggerFactory.getLogger(DatabricksWorkspaceClientBean.class);

    public DatabricksWorkspaceClientBean(
            String workspaceHost,
            String workspaceName,
            DatabricksAuthConfig databricksAuthConfig,
            AzureAuthConfig azureAuthConfig,
            GitCredentialsConfig gitCredentialsConfig) {
        this.workspaceHost = workspaceHost;
        this.workspaceName = workspaceName;
        this.databricksAuthConfig = databricksAuthConfig;
        this.azureAuthConfig = azureAuthConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
    }

    @Override
    public WorkspaceClient getObject() {
        setWorkspaceClient(initializeWorkspaceClient());
        return workspaceClient;
    }

    public WorkspaceClient getObject(String workspaceHost, String workspaceName) throws Exception {
        setWorkspaceHost(workspaceHost);
        setWorkspaceName(workspaceName);
        setWorkspaceClient(initializeWorkspaceClient());

        return workspaceClient;
    }

    @Override
    public Class<?> getObjectType() {
        return WorkspaceClient.class;
    }

    private synchronized WorkspaceClient initializeWorkspaceClient() {
        try {
            DatabricksConfig config = new DatabricksConfig()
                    .setHost(workspaceHost)
                    .setAccountId(databricksAuthConfig.getAccountId())
                    .setAzureTenantId(azureAuthConfig.getTenantId())
                    .setAzureClientId(azureAuthConfig.getClientId())
                    .setAzureClientSecret(azureAuthConfig.getClientSecret());

            workspaceClient = new WorkspaceClient(config);

            setGitCredentials(
                    gitCredentialsConfig.getToken(),
                    gitCredentialsConfig.getUsername(),
                    gitCredentialsConfig.getProvider());

            return workspaceClient;

        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error initializing the workspaceClient for %s. Please try again and if the error persists contact the platform team. Details: %s",
                    workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            throw new RuntimeException("Unable to initialize workspace client", e);
        }
    }

    protected void setGitCredentials(String personalAccessToken, String gitUsername, String gitProvider) {
        try {

            Iterable<CredentialInfo> listCredentials =
                    workspaceClient.gitCredentials().list();

            Optional<CredentialInfo> optionalCredentialInfo = Optional.empty();

            if (listCredentials != null) {
                optionalCredentialInfo = StreamSupport.stream(listCredentials.spliterator(), false)
                        .filter(credentialInfo ->
                                credentialInfo.getGitProvider().equalsIgnoreCase(gitProvider))
                        .findFirst();
            }

            if (optionalCredentialInfo.isEmpty())
                workspaceClient
                        .gitCredentials()
                        .create(new CreateCredentials()
                                .setPersonalAccessToken(personalAccessToken)
                                .setGitUsername(gitUsername)
                                .setGitProvider(gitProvider));
            else {

                logger.warn(
                        "Credentials for {} for the workspace {} already exists. Updating them with the ones provided",
                        gitProvider.toString().toUpperCase(),
                        workspaceName);

                workspaceClient
                        .gitCredentials()
                        .update(new UpdateCredentials()
                                .setCredentialId(optionalCredentialInfo.get().getCredentialId())
                                .setGitUsername(gitUsername)
                                .setPersonalAccessToken(personalAccessToken)
                                .setGitProvider(gitProvider.toUpperCase()));
            }
        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error setting Git credentials for %s. Please try again and if the error persists contact the platform team. Details: %s",
                    workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            throw new RuntimeException("Unable to set Git credentials", e);
        }
    }

    public void setWorkspaceClient(WorkspaceClient workspaceClient) {
        this.workspaceClient = workspaceClient;
    }

    public void setWorkspaceHost(String workspaceHost) {
        this.workspaceHost = workspaceHost;
    }

    public void setWorkspaceName(String workspaceName) {
        this.workspaceName = workspaceName;
    }
}
