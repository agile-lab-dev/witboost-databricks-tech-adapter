package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.*;
import com.databricks.sdk.service.compute.ListClustersRequest;
import com.databricks.sdk.service.iam.*;
import com.databricks.sdk.service.provisioning.Workspace;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkspaceLevelManager {

    private final Logger logger = LoggerFactory.getLogger(WorkspaceLevelManager.class);

    private final WorkspaceClient workspaceClient;
    private final AccountClient accountClient;

    public WorkspaceLevelManager(WorkspaceClient workspaceClient, AccountClient accountClient) {
        this.workspaceClient = workspaceClient;
        this.accountClient = accountClient;
    }

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

    public Either<FailedOperation, String> getSqlWarehouseIdFromName(String sqlWarehouseName) {

        var sqlWarehouseList = workspaceClient.dataSources().list();

        if (sqlWarehouseList != null) {
            for (var sqlWarehouseInfo : sqlWarehouseList) {
                if (sqlWarehouseInfo.getName().equalsIgnoreCase(sqlWarehouseName)) {
                    String sqlWarehouseId = sqlWarehouseInfo.getWarehouseId();
                    logger.info(String.format("SQL Warehouse '%s' found. Id: %s.", sqlWarehouseName, sqlWarehouseId));
                    return right(sqlWarehouseId);
                }
            }
        }

        String errorMessage = String.format(
                "An error occurred while searching for Sql Warehouse '%s' details. Please try again and if the error persists contact the platform team. Details: Sql Warehouse not found.",
                sqlWarehouseName);
        logger.error(errorMessage);
        return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
    }

    public Either<FailedOperation, String> getComputeClusterIdFromName(String clusterName) {

        var clusterList = workspaceClient.clusters().list(new ListClustersRequest());

        if (clusterList != null) {
            for (var clusterInfo : clusterList) {
                if (clusterInfo.getClusterName().equalsIgnoreCase(clusterName)) {
                    String clusterId = clusterInfo.getClusterId();
                    logger.info(String.format("Cluster '%s' found. Id: '%s'.", clusterName, clusterId));
                    return right(clusterId);
                }
            }
        }

        String errorMessage = String.format(
                "An error occurred while searching for Cluster '%s' in workspace '%s'. Please try again and if the error persists contact the platform team. Details: Cluster not found.",
                clusterName, getWorkspaceName());
        logger.error(errorMessage);
        return left(FailedOperation.singleProblemFailedOperation(errorMessage));
    }
}
