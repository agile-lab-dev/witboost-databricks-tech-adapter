package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.*;
import com.databricks.sdk.service.compute.ListClustersRequest;
import com.databricks.sdk.service.iam.*;
import com.databricks.sdk.service.oauth2.CreateServicePrincipalSecretRequest;
import com.databricks.sdk.service.oauth2.CreateServicePrincipalSecretResponse;
import com.databricks.sdk.service.oauth2.DeleteServicePrincipalSecretRequest;
import com.databricks.sdk.service.provisioning.Workspace;
import com.databricks.sdk.service.workspace.CreateCredentialsRequest;
import com.databricks.sdk.service.workspace.UpdateCredentialsRequest;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import lombok.extern.slf4j.Slf4j;

/**
 * Manages workspace-level operations such as retrieving cluster and SQL warehouse details,
 * handling Git credentials, and managing service principals for a Databricks workspace.
 */
@Slf4j
public class WorkspaceLevelManager {

    private final WorkspaceClient workspaceClient;
    private final AccountClient accountClient;

    public WorkspaceLevelManager(WorkspaceClient workspaceClient, AccountClient accountClient) {
        this.workspaceClient = workspaceClient;
        this.accountClient = accountClient;
    }

    /**
     * Retrieves the name of the workspace associated with the current configuration.
     *
     * @return the name of the workspace that matches the current configuration
     */
    protected String getWorkspaceName() {
        String workspaceHost = workspaceClient.config().getHost();

        Iterable<Workspace> listWorkspaces = accountClient.workspaces().list();
        Stream<Workspace> stream = StreamSupport.stream(listWorkspaces.spliterator(), false);

        return stream.filter(ws -> String.format("https://%s.azuredatabricks.net", ws.getDeploymentName())
                        .equalsIgnoreCase(workspaceHost))
                .findFirst()
                .get()
                .getWorkspaceName();
    }

    /**
     * Retrieves the SQL Warehouse ID corresponding to the given SQL Warehouse name.
     *
     * This method searches through the list of SQL Warehouses in the current workspace
     * and returns the ID of the SQL Warehouse that matches the provided name. If no
     * matching SQL Warehouse is found, or an error occurs during the search process,
     * a {@code FailedOperation} containing an error message is returned.
     *
     * @param sqlWarehouseName the name of the SQL Warehouse to search for
     * @return an {@code Either} containing the SQL Warehouse ID if found,
     *         or a {@code FailedOperation} detailing the error if no match
     *         is found or an issue occurs during the process
     */
    public Either<FailedOperation, String> getSqlWarehouseIdFromName(String sqlWarehouseName) {

        var sqlWarehouseList = workspaceClient.dataSources().list();

        if (sqlWarehouseList != null) {
            for (var sqlWarehouseInfo : sqlWarehouseList) {
                if (sqlWarehouseInfo.getName().equalsIgnoreCase(sqlWarehouseName)) {
                    String sqlWarehouseId = sqlWarehouseInfo.getWarehouseId();
                    log.info(String.format("SQL Warehouse '%s' found. Id: %s.", sqlWarehouseName, sqlWarehouseId));
                    return right(sqlWarehouseId);
                }
            }
        }

        String errorMessage = String.format(
                "An error occurred while searching for Sql Warehouse '%s' details. Please try again and if the error persists contact the platform team. Details: Sql Warehouse not found.",
                sqlWarehouseName);
        log.error(errorMessage);
        return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
    }

    /**
     * Retrieves the compute cluster ID from the cluster name in the current workspace.
     *
     * This method searches through the list of compute clusters available in the workspace
     * to find the ID of the cluster that matches the provided cluster name. If no matching
     * cluster is found, or an error occurs during the process, a {@code FailedOperation}
     * containing an error message is returned.
     *
     * @param clusterName the name of the compute cluster to search for
     * @return an {@code Either} containing the compute cluster ID if found,
     *         or a {@code FailedOperation} detailing the error if no match is found
     *         or an issue occurs during the process
     */
    public Either<FailedOperation, String> getComputeClusterIdFromName(String clusterName) {

        var clusterList = workspaceClient.clusters().list(new ListClustersRequest());

        if (clusterList != null) {
            for (var clusterInfo : clusterList) {
                if (clusterInfo.getClusterName().equalsIgnoreCase(clusterName)) {
                    String clusterId = clusterInfo.getClusterId();
                    log.info(String.format("Cluster '%s' found. Id: '%s'.", clusterName, clusterId));
                    return right(clusterId);
                }
            }
        }

        String errorMessage = String.format(
                "An error occurred while searching for Cluster '%s' in workspace '%s'. Please try again and if the error persists contact the platform team. Details: Cluster not found.",
                clusterName, getWorkspaceName());
        log.error(errorMessage);
        return left(FailedOperation.singleProblemFailedOperation(errorMessage));
    }

    /**
     * Sets Git credentials for a specified workspace. If credentials for the given provider already exist,
     * this method updates them. If not, it creates new credentials.
     *
     * @param workspaceClient the client used to interact with the workspace
     * @param gitCredentialsConfig the configuration containing details about the Git credentials, such as username, token, and provider
     * @return an {@code Either} with a {@code FailedOperation} in case of an error, or {@code Void} upon successful execution
     */
    public synchronized Either<FailedOperation, Void> setGitCredentials(
            WorkspaceClient workspaceClient, GitCredentialsConfig gitCredentialsConfig) {
        try {

            String workspaceName = getWorkspaceName();
            log.info(
                    "Setting Git credentials for {} in workspace '{}'",
                    gitCredentialsConfig.getUsername(),
                    workspaceName);

            Optional.ofNullable(workspaceClient.gitCredentials().list())
                    .map(credentials -> StreamSupport.stream(credentials.spliterator(), false))
                    .orElse(Stream.empty()) // If the list of gitCredentials is null, use an empty stream
                    .filter(credentialInfo ->
                            credentialInfo.getGitProvider().equalsIgnoreCase(gitCredentialsConfig.getProvider()))
                    .findFirst()
                    .ifPresentOrElse(
                            credentialInfo -> { // If credentials already exist, update them
                                log.warn(
                                        "Credentials for {} already exist in workspace '{}'. Updating them with the ones provided",
                                        gitCredentialsConfig.getProvider().toUpperCase(),
                                        workspaceName);
                                workspaceClient
                                        .gitCredentials()
                                        .update(new UpdateCredentialsRequest()
                                                .setCredentialId(credentialInfo.getCredentialId())
                                                .setGitUsername(gitCredentialsConfig.getUsername())
                                                .setPersonalAccessToken(gitCredentialsConfig.getToken())
                                                .setGitProvider(gitCredentialsConfig
                                                        .getProvider()
                                                        .toUpperCase()));
                            },
                            () -> { // If credentials don't exist or gitCredentials().list() is null, create new ones
                                workspaceClient
                                        .gitCredentials()
                                        .create(new CreateCredentialsRequest()
                                                .setPersonalAccessToken(gitCredentialsConfig.getToken())
                                                .setGitUsername(gitCredentialsConfig.getUsername())
                                                .setGitProvider(gitCredentialsConfig.getProvider()));
                            });

            log.info(
                    "Git credentials successfully updated for '{}' in workspace '{}'",
                    gitCredentialsConfig.getUsername(),
                    workspaceName);
            return right(null);

        } catch (Exception e) {
            // Catching possible exceptions generated by Databricks
            String errorMessage = String.format(
                    "Error setting Git credentials for %s. Please try again and if the error persists contact the platform team. Details: %s",
                    getWorkspaceName(), e.getMessage());
            log.error(errorMessage, e);
            return left(FailedOperation.singleProblemFailedOperation(errorMessage));
        }
    }

    /**
     * Retrieves a service principal by its display name from the current workspace.
     *
     * @param principalName the display name of the service principal to look up
     * @return an {@code Either} containing the {@code ServicePrincipal} if found, or a {@code FailedOperation} detailing the error if not found or an issue occurs during the process
     *
     */
    public Either<FailedOperation, ServicePrincipal> getServicePrincipalFromName(String principalName) {
        try {
            String workspaceName = getWorkspaceName();
            log.info("Looking up principal ID for {}", principalName);

            Iterable<ServicePrincipal> workspaceServicePrincipals =
                    workspaceClient.servicePrincipals().list(new ListServicePrincipalsRequest());

            List<ServicePrincipal> principalList = new ArrayList<>();
            if (workspaceServicePrincipals != null) workspaceServicePrincipals.forEach(principalList::add);

            if (principalList.isEmpty()) {
                String errorMessage = String.format("No service principals found in workspace %s", workspaceName);
                log.error(errorMessage);
                return left(FailedOperation.singleProblemFailedOperation(errorMessage));
            }

            Optional<ServicePrincipal> principal = principalList.stream()
                    .filter(sp -> {
                        return sp.getDisplayName().trim().equalsIgnoreCase(principalName.trim());
                    })
                    .findFirst();
            if (principal.isPresent()) {
                log.info(
                        "Found principal with application ID {} for {}",
                        principal.get().getApplicationId(),
                        principalName);
                return right(principal.get());
            } else {
                String errorMessage =
                        String.format("No principal named %s found in workspace %s", principalName, workspaceName);
                log.error(errorMessage);
                return left(FailedOperation.singleProblemFailedOperation(errorMessage));
            }

        } catch (Exception e) {
            String errorMessage = String.format(
                    "Error getting information about service principal '%s' in workspace '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    principalName, getWorkspaceName(), e.getMessage());
            log.error(errorMessage, e);
            return left(FailedOperation.singleProblemFailedOperation(errorMessage));
        }
    }

    /**
     * Generates a secret for a given service principal with a specified lifetime.
     * This method creates a new secret for the service principal identified by the given ID,
     * associating it with the defined lifetime. If the operation is successful, it returns
     * the secret ID and secret value. In case of failure, it returns a {@code FailedOperation}
     * detailing the issue.
     *
     * @param servicePrincipalId the ID of the service principal for which the secret is being generated
     * @param lifetimeSeconds the duration, in seconds, for which the generated secret remains valid
     * @return an {@code Either} containing:
     *         - a {@code Map.Entry<String, String>} with the secret ID and its value upon successful creation
     *         - a {@code FailedOperation} describing the issue if the operation fails
     */
    public Either<FailedOperation, Map.Entry<String, String>> generateSecretForServicePrincipal(
            long servicePrincipalId, String lifetimeSeconds) {
        try {
            log.info(
                    "Generating secret for service principal {} with lifetime {} seconds",
                    servicePrincipalId,
                    lifetimeSeconds);

            CreateServicePrincipalSecretRequest req = new CreateServicePrincipalSecretRequest()
                    .setServicePrincipalId(servicePrincipalId)
                    .setLifetime(lifetimeSeconds);

            CreateServicePrincipalSecretResponse info =
                    accountClient.servicePrincipalSecrets().create(req);
            log.info("Successfully generated secret for service principal {}", servicePrincipalId);

            return right(new AbstractMap.SimpleEntry<>(info.getId(), info.getSecret()));
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An unexpected error occurred while generating secret for service principal %s. Please try again and if the error persists contact the platform team. Details: %s",
                    servicePrincipalId, e.getMessage());
            log.error(errorMessage, e);
            return left(FailedOperation.singleProblemFailedOperation(errorMessage));
        }
    }

    /**
     * Deletes a secret associated with the specified service principal.
     * This method removes the secret identified by the provided secret ID from the service principal
     * identified by the given service principal ID. If the operation fails, it returns a detailed error message.
     *
     * @param servicePrincipalId the ID of the service principal from which the secret is to be deleted
     * @param secretId the unique identifier of the secret to be deleted
     * @return an {@code Either} containing:
     *         - {@code Void} if the secret is successfully deleted
     *         - a {@code FailedOperation} detailing the error if the deletion fails
     */
    public Either<FailedOperation, Void> deleteServicePrincipalSecret(long servicePrincipalId, String secretId) {
        try {
            log.info("Deleting secret {} for service principal {}", secretId, servicePrincipalId);
            DeleteServicePrincipalSecretRequest req = new DeleteServicePrincipalSecretRequest()
                    .setServicePrincipalId(servicePrincipalId)
                    .setSecretId(secretId);
            accountClient.servicePrincipalSecrets().delete(req);
            log.info("Successfully deleted secret {} for service principal {}", secretId, servicePrincipalId);
            return right(null);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An unexpected error occurred while deleting secret %s for service principal %s. Please try again and if the error persists contact the platform team. Details: %s",
                    secretId, servicePrincipalId, e.getMessage());
            log.error(errorMessage, e);
            return left(FailedOperation.singleProblemFailedOperation(errorMessage));
        }
    }
}
