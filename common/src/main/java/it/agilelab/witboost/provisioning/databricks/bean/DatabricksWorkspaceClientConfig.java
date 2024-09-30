package it.agilelab.witboost.provisioning.databricks.bean;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.workspace.CreateCredentials;
import com.databricks.sdk.service.workspace.UpdateCredentials;
import it.agilelab.witboost.provisioning.databricks.bean.params.WorkspaceClientConfigParams;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class DatabricksWorkspaceClientConfig {

    private static final Logger logger = LoggerFactory.getLogger(DatabricksWorkspaceClientConfig.class);

    @Bean
    public Function<WorkspaceClientConfigParams, WorkspaceClient> workspaceClientFactory() {
        return arg -> createWorkspaceClient(arg);
    }

    @Bean
    @Scope(value = "prototype")
    public WorkspaceClient createWorkspaceClient(WorkspaceClientConfigParams workspaceClientConfigParams) {

        try {
            DatabricksConfig config = buildDatabricksConfig(
                    workspaceClientConfigParams.getDatabricksAuthConfig(),
                    workspaceClientConfigParams.getAzureAuthConfig(),
                    workspaceClientConfigParams.getWorkspaceHost());
            WorkspaceClient workspaceClient = new WorkspaceClient(config);

            setGitCredentials(
                    workspaceClient,
                    workspaceClientConfigParams.getGitCredentialsConfig(),
                    workspaceClientConfigParams.getWorkspaceName());
            return workspaceClient;

        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error initializing the workspaceClient for %s. Please try again and if the error persists contact the platform team. Details: %s",
                    workspaceClientConfigParams.getWorkspaceName(), e.getMessage());
            logger.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    protected DatabricksConfig buildDatabricksConfig(
            DatabricksAuthConfig databricksAuthConfig, AzureAuthConfig azureAuthConfig, String workspaceHost) {
        return new DatabricksConfig()
                .setHost(workspaceHost)
                .setAccountId(databricksAuthConfig.getAccountId())
                .setAzureTenantId(azureAuthConfig.getTenantId())
                .setAzureClientId(azureAuthConfig.getClientId())
                .setAzureClientSecret(azureAuthConfig.getClientSecret());
    }

    protected void setGitCredentials(
            WorkspaceClient workspaceClient, GitCredentialsConfig gitCredentialsConfig, String workspaceName) {

        try {

            Optional.ofNullable(workspaceClient.gitCredentials().list())
                    .map(credentials -> StreamSupport.stream(credentials.spliterator(), false))
                    .orElse(Stream.empty()) // If list of gitCredentials is null, use an empty stream
                    .filter(credentialInfo ->
                            credentialInfo.getGitProvider().equalsIgnoreCase(gitCredentialsConfig.getProvider()))
                    .findFirst()
                    .ifPresentOrElse(
                            credentialInfo -> {
                                // If credentials already exist, update them
                                logger.warn(
                                        "Credentials for {} for the workspace {} already exist. Updating them with the ones provided",
                                        gitCredentialsConfig.getProvider().toUpperCase(),
                                        workspaceName);

                                workspaceClient
                                        .gitCredentials()
                                        .update(new UpdateCredentials()
                                                .setCredentialId(credentialInfo.getCredentialId())
                                                .setGitUsername(gitCredentialsConfig.getUsername())
                                                .setPersonalAccessToken(gitCredentialsConfig.getToken())
                                                .setGitProvider(gitCredentialsConfig
                                                        .getProvider()
                                                        .toUpperCase()));
                            },
                            () -> {
                                // If credentials don't exist or gitCredentials().list() is null, create new ones
                                workspaceClient
                                        .gitCredentials()
                                        .create(new CreateCredentials()
                                                .setPersonalAccessToken(gitCredentialsConfig.getToken())
                                                .setGitUsername(gitCredentialsConfig.getUsername())
                                                .setGitProvider(gitCredentialsConfig.getProvider()));
                            });

        } catch (Exception e) {
            // Catching possible exceptions generated by Databricks
            String errorMessage = String.format(
                    "Error setting Git credentials for %s. Please try again and if the error persists contact the platform team. Details: %s",
                    workspaceName, e.getMessage());
            logger.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }
}
