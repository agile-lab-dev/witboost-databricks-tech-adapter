package it.agilelab.witboost.provisioning.databricks.bean;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import java.util.function.Function;
import lombok.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class WorkspaceClientConfig {

    private static final Logger logger = LoggerFactory.getLogger(WorkspaceClientConfig.class);

    @Bean
    public Function<WorkspaceClientConfigParams, WorkspaceClient> workspaceClientFactory() {
        return this::createWorkspaceClient;
    }

    protected DatabricksConfig buildAzureDatabricksConfig(WorkspaceClientConfigParams workspaceClientConfigParams) {
        return new DatabricksConfig()
                .setHost(workspaceClientConfigParams.getWorkspaceHost())
                .setAccountId(
                        workspaceClientConfigParams.getDatabricksAuthConfig().getAccountId())
                .setAzureTenantId(
                        workspaceClientConfigParams.getAzureAuthConfig().getTenantId())
                .setAzureClientId(
                        workspaceClientConfigParams.getAzureAuthConfig().getClientId())
                .setAzureClientSecret(
                        workspaceClientConfigParams.getAzureAuthConfig().getClientSecret());
    }

    protected DatabricksConfig buildOAuthDatabricksConfig(WorkspaceClientConfigParams workspaceClientConfigParams) {
        return new DatabricksConfig()
                .setHost(workspaceClientConfigParams.getWorkspaceHost())
                .setClientId(workspaceClientConfigParams.getDatabricksClientID())
                .setClientSecret(workspaceClientConfigParams.getDatabricksClientSecret());
    }

    @Bean
    @Scope(value = "prototype")
    public WorkspaceClient createWorkspaceClient(WorkspaceClientConfigParams workspaceClientConfigParams) {
        try {
            DatabricksConfig config;

            if (workspaceClientConfigParams.getAuthType() == null) {
                throw new IllegalArgumentException("Invalid auth type: null");
            }

            return switch (workspaceClientConfigParams.getAuthType()) {
                case AZURE -> {
                    config = buildAzureDatabricksConfig(workspaceClientConfigParams);
                    yield new WorkspaceClient(config);
                }
                case OAUTH -> {
                    config = buildOAuthDatabricksConfig(workspaceClientConfigParams);
                    yield new WorkspaceClient(config);
                }
                default -> throw new IllegalArgumentException(
                        "Invalid auth type: " + workspaceClientConfigParams.getAuthType());
            };

        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error initializing the workspaceClient for %s.Please try again and if the error persists contact the platform team. Details: %s",
                    workspaceClientConfigParams.getWorkspaceName(), e.getMessage());
            logger.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WorkspaceClientConfigParams {

        public enum AuthType {
            AZURE,
            OAUTH
        }

        private AuthType authType;

        // Azure auth
        private DatabricksAuthConfig databricksAuthConfig;
        private AzureAuthConfig azureAuthConfig;

        // OAuth
        private String databricksClientID;
        private String databricksClientSecret;

        // Common
        private String workspaceHost;
        private String workspaceName;

        // Azure-auth
        public WorkspaceClientConfigParams(
                AuthType authType,
                DatabricksAuthConfig databricksAuthConfig,
                AzureAuthConfig azureAuthConfig,
                String workspaceHost,
                String workspaceName) {
            this.authType = authType;
            this.databricksAuthConfig = databricksAuthConfig;
            this.azureAuthConfig = azureAuthConfig;
            this.workspaceHost = workspaceHost;
            this.workspaceName = workspaceName;
        }

        // OAuth
        public WorkspaceClientConfigParams(
                AuthType authType,
                String databricksClientID,
                String databricksClientSecret,
                String workspaceHost,
                String workspaceName) {
            this.authType = authType;
            this.databricksClientID = databricksClientID;
            this.databricksClientSecret = databricksClientSecret;
            this.workspaceHost = workspaceHost;
        }
    }
}
