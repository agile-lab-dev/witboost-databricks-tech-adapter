package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.catalog.*;
import com.databricks.sdk.service.sql.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.params.ApiClientConfigParams;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.GitCredentialsConfig;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.permissions.AzurePermissionsManager;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureMapper;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.databricks.DatabricksMapper;
import java.util.*;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OutputPortHandler {

    private final Logger logger = LoggerFactory.getLogger(OutputPortHandler.class);

    private final AzureAuthConfig azureAuthConfig;
    private final GitCredentialsConfig gitCredentialsConfig;
    private final Function<ApiClientConfigParams, ApiClient> apiClientFactory;
    private final AzurePermissionsManager azurePermissionsManager;
    private final MiscConfig miscConfig;
    private final AzureMapper azureMapper;
    private final DatabricksAuthConfig databricksAuthConfig;

    @Autowired
    public OutputPortHandler(
            AzureAuthConfig azureAuthConfig,
            GitCredentialsConfig gitCredentialsConfig,
            Function<ApiClientConfigParams, ApiClient> apiClientFactory,
            AzurePermissionsManager azurePermissionsManager,
            MiscConfig miscConfig,
            AzureMapper azureMapper,
            DatabricksAuthConfig databricksAuthConfig) {
        this.azureAuthConfig = azureAuthConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
        this.apiClientFactory = apiClientFactory;
        this.azurePermissionsManager = azurePermissionsManager;
        this.miscConfig = miscConfig;
        this.azureMapper = azureMapper;
        this.databricksAuthConfig = databricksAuthConfig;
    }

    /**
     * Provisions a Databricks View as Output Port.
     *
     * @param provisionRequest        the request containing the details for provisioning
     * @param workspaceClient         the Databricks workspace client
     * @param databricksWorkspaceInfo information about the Databricks workspace
     * @return Either a FailedOperation or a String containing the ID of the created table if successful
     */
    public Either<FailedOperation, TableInfo> provisionOutputPort(
            ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {

        try {
            DatabricksOutputPortSpecific databricksOutputPortSpecific =
                    provisionRequest.component().getSpecific();

            var unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

            // Retrieving fields from request
            String catalogNameOP = databricksOutputPortSpecific.getCatalogNameOP();
            String schemaNameOP = databricksOutputPortSpecific.getSchemaNameOP();
            String viewNameOP = databricksOutputPortSpecific.getViewNameOP();
            String viewFullNameOP = catalogNameOP + "." + schemaNameOP + "." + viewNameOP;
            String tableFullName = databricksOutputPortSpecific.getCatalogName() + "."
                    + databricksOutputPortSpecific.getSchemaName() + "." + databricksOutputPortSpecific.getTableName();

            // Catalog
            Either<FailedOperation, Void> eitherCreatedCatalog =
                    unityCatalogManager.createCatalogIfNotExists(catalogNameOP);
            if (eitherCreatedCatalog.isLeft()) {
                return left(eitherCreatedCatalog.getLeft());
            }

            // Schema
            Either<FailedOperation, Void> eitherCreatedSchema =
                    unityCatalogManager.createSchemaIfNotExists(catalogNameOP, schemaNameOP);
            if (eitherCreatedSchema.isLeft()) {
                return left(eitherCreatedSchema.getLeft());
            }

            ApiClientConfigParams apiClientConfigParams = new ApiClientConfigParams(
                    databricksAuthConfig,
                    azureAuthConfig,
                    gitCredentialsConfig,
                    databricksWorkspaceInfo.getDatabricksHost(),
                    databricksWorkspaceInfo.getName());

            ApiClient apiClient = apiClientFactory.apply(apiClientConfigParams);

            // Retrieving Sql Warehouse id from name
            String sqlWarehouseName = databricksOutputPortSpecific.getSqlWarehouseName();
            var sqlWarehouseId = getSqlWarehouseId(apiClient, sqlWarehouseName);

            if (sqlWarehouseId.isLeft()) return left(sqlWarehouseId.getLeft());

            // Create OP
            Either<FailedOperation, String> eitherExecutedStatementCreateOutputPort = executeStatementCreateOutputPort(
                    apiClient, catalogNameOP, schemaNameOP, viewNameOP, tableFullName, sqlWarehouseId.get());

            if (eitherExecutedStatementCreateOutputPort.isLeft()) {
                return left(eitherExecutedStatementCreateOutputPort.getLeft());
            }

            String statementId = eitherExecutedStatementCreateOutputPort.get();

            Either<FailedOperation, Void> eitherFinishedPolling = pollOnStatementExecution(apiClient, statementId);
            if (eitherFinishedPolling.isLeft()) {
                return left(eitherFinishedPolling.getLeft());
            }

            logger.info(
                    String.format("Output Port '%s' is now available. Start setting permissions. ", viewFullNameOP));

            // Permissions
            String dpOwner = provisionRequest.dataProduct().getDataProductOwner();
            String devGroup = provisionRequest.dataProduct().getDevGroup();

            DatabricksMapper databricksMapper = new DatabricksMapper();

            Map<String, Either<Throwable, String>> eitherMap = databricksMapper.map(Set.of(dpOwner, devGroup));

            // Map DP OWNER
            Either<Throwable, String> eitherDpOwnerDatabricksId = eitherMap.get(dpOwner);
            if (eitherDpOwnerDatabricksId.isLeft()) {
                var error = eitherDpOwnerDatabricksId.getLeft();
                return left(new FailedOperation(Collections.singletonList(new Problem(error.getMessage(), error))));
            }
            String dpOwnerDatabricksId = eitherDpOwnerDatabricksId.get();

            // Map DEV GROUP
            Either<Throwable, String> eitherDpDevGroupDatabricksId = eitherMap.get(devGroup);
            if (eitherDpDevGroupDatabricksId.isLeft()) {
                var error = eitherDpDevGroupDatabricksId.getLeft();
                return left(new FailedOperation(Collections.singletonList(new Problem(error.getMessage(), error))));
            }
            String dpDevGroupDatabricksId = eitherDpDevGroupDatabricksId.get();

            // List of principals to set permissions to
            List<String> principalsListPermissions = List.of(dpOwnerDatabricksId, dpDevGroupDatabricksId);

            // Retrieve environment
            String environment = provisionRequest.dataProduct().getEnvironment();

            if (environment.equalsIgnoreCase(miscConfig.developmentEnvironmentName())) {
                Either<FailedOperation, Void> eitherAssignedPermissionsCatalogOP =
                        assignDatabricksPermissionsCatalog(workspaceClient, principalsListPermissions, catalogNameOP);
                if (eitherAssignedPermissionsCatalogOP.isLeft()) {
                    return left(eitherAssignedPermissionsCatalogOP.getLeft());
                }

                Either<FailedOperation, Void> eitherAssignedPermissionsSchemaOP = assignDatabricksPermissionsSchema(
                        workspaceClient, principalsListPermissions, catalogNameOP, schemaNameOP);
                if (eitherAssignedPermissionsSchemaOP.isLeft()) {
                    return left(eitherAssignedPermissionsSchemaOP.getLeft());
                }

                Either<FailedOperation, Void> eitherAssignedPermissionsViewOP =
                        assignDatabricksPermissionsView(workspaceClient, principalsListPermissions, viewFullNameOP);
                if (eitherAssignedPermissionsViewOP.isLeft()) {
                    return left(eitherAssignedPermissionsViewOP.getLeft());
                }
            }

            // Retrieving tableInfo
            TableInfo tableInfo = unityCatalogManager
                    .getTableInfo(catalogNameOP, schemaNameOP, viewNameOP)
                    .get();

            return right(tableInfo);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while provisioning component %s. Please try again and if the error persists contact the platform team. Details: %s",
                    provisionRequest.component().getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private Either<FailedOperation, Void> assignDatabricksPermissionsView(
            WorkspaceClient workspaceClient, List<String> principalsListPermissions, String viewFullName) {
        try {

            for (String principal : principalsListPermissions) {

                logger.info(
                        String.format("Updating permissions on view '%s' for principal %s", viewFullName, principal));

                PermissionsChange permissionChanges = new PermissionsChange()
                        .setAdd(List.of(Privilege.SELECT))
                        .setPrincipal(principal);

                UpdatePermissions updatePermission = new UpdatePermissions();
                updatePermission
                        .setChanges(List.of(permissionChanges))
                        .setSecurableType(SecurableType.TABLE)
                        .setFullName(viewFullName);

                workspaceClient.grants().update(updatePermission);
                logger.info(
                        String.format("Permission on view '%s' for principal %s updated.", viewFullName, principal));
            }
            return right(null);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while updating permissions for view '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    viewFullName, e.getMessage());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
    }

    private Either<FailedOperation, Void> assignDatabricksPermissionsCatalog(
            WorkspaceClient workspaceClient, List<String> principalsListPermissions, String catalogNameOP) {
        try {

            for (String principal : principalsListPermissions) {

                logger.info(String.format(
                        "Updating permissions on CATALOG '%s' for principal %s", catalogNameOP, principal));

                PermissionsChange permissionChanges = new PermissionsChange()
                        .setAdd(List.of(Privilege.USE_CATALOG))
                        .setPrincipal(principal);

                UpdatePermissions updatePermission = new UpdatePermissions();
                updatePermission
                        .setChanges(List.of(permissionChanges))
                        .setSecurableType(SecurableType.CATALOG)
                        .setFullName(catalogNameOP);

                workspaceClient.grants().update(updatePermission);
                logger.info(String.format(
                        "Permission on CATALOG '%s' for principal %s updated.", catalogNameOP, principal));
            }
            return right(null);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while updating permissions for CATALOG '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    catalogNameOP, e.getMessage());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
    }

    private Either<FailedOperation, Void> assignDatabricksPermissionsSchema(
            WorkspaceClient workspaceClient,
            List<String> principalsListPermissions,
            String catalogNameOP,
            String schemaNameOP) {
        try {

            for (String principal : principalsListPermissions) {

                logger.info(
                        String.format("Updating permissions on SCHEMA '%s' for principal %s", schemaNameOP, principal));

                PermissionsChange permissionChanges = new PermissionsChange()
                        .setAdd(List.of(Privilege.USE_SCHEMA))
                        .setPrincipal(principal);

                UpdatePermissions updatePermission = new UpdatePermissions();
                updatePermission
                        .setChanges(List.of(permissionChanges))
                        .setSecurableType(SecurableType.SCHEMA)
                        .setFullName(catalogNameOP + "." + schemaNameOP);

                workspaceClient.grants().update(updatePermission);
                logger.info(
                        String.format("Permission on SCHEMA '%s' for principal %s updated.", schemaNameOP, principal));
            }
            return right(null);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while updating permissions for SCHEMA '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    schemaNameOP, e.getMessage());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
    }

    private Either<FailedOperation, String> executeStatementCreateOutputPort(
            ApiClient apiClient,
            String catalogNameOP,
            String schemaNameOP,
            String viewNameOP,
            String tableFullName,
            String sqlWarehouseId) {

        try {
            String queryToCreateOP =
                    String.format("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s;", viewNameOP, tableFullName);

            logger.info("Query: {}", queryToCreateOP);

            ExecuteStatementRequest request = new ExecuteStatementRequest()
                    .setCatalog(catalogNameOP)
                    .setSchema(schemaNameOP)
                    .setStatement(queryToCreateOP)
                    .setWarehouseId(sqlWarehouseId);

            StatementExecutionAPI statementExecutionAPI = new StatementExecutionAPI(apiClient);

            String statementId = statementExecutionAPI.executeStatement(request).getStatementId();

            return right(statementId);
        } catch (Exception e) {

            String viewFullName = catalogNameOP + "." + schemaNameOP + "." + viewNameOP;

            String errorMessage = String.format(
                    "An error occurred while creating view '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    viewFullName, e.getMessage());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
    }

    private Either<FailedOperation, Void> pollOnStatementExecution(ApiClient apiClient, String statementId) {

        StatementExecutionAPI statementExecutionAPI = new StatementExecutionAPI(apiClient);

        var isStatementRunning = true;

        while (isStatementRunning) {

            GetStatementResponse getStatementResponse = statementExecutionAPI.getStatement(statementId);

            StatementState statementState = getStatementResponse.getStatus().getState();

            String logMessage = String.format("Status of statement (id: %s): %s. ", statementId, statementState);

            switch (statementState) {
                case PENDING, RUNNING:
                    logger.info(logMessage + "Still polling.");
                    break;
                case FAILED, CANCELED, CLOSED:
                    String errorMessage = String.format(logMessage);
                    logger.error(errorMessage);
                    return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
                case SUCCEEDED:
                    isStatementRunning = false;
                    logger.info(String.format(logMessage));
                    break;
            }
        }
        return right(null);
    }

    private Either<FailedOperation, String> getSqlWarehouseId(ApiClient apiClient, String sqlWarehouseName) {

        DataSourcesAPI dataSourceAPI = new DataSourcesAPI(apiClient);
        var sqlWarehouseList = dataSourceAPI.list();

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

    public Either<FailedOperation, Void> unprovisionOutputPort(
            ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {

        DatabricksOutputPortSpecific databricksOutputPortSpecific =
                provisionRequest.component().getSpecific();

        String catalogNameOP = databricksOutputPortSpecific.getCatalogNameOP();
        String schemaNameOP = databricksOutputPortSpecific.getSchemaNameOP();
        String viewNameOP = databricksOutputPortSpecific.getViewNameOP();
        String viewFullNameOP = catalogNameOP + "." + schemaNameOP + "." + viewNameOP;

        try {
            var unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

            Either<FailedOperation, Boolean> eitherExistingCatalog =
                    unityCatalogManager.checkCatalogExistence(catalogNameOP);
            if (eitherExistingCatalog.isLeft()) {
                return left(eitherExistingCatalog.getLeft());
            }

            if (!eitherExistingCatalog.get()) {
                logger.info(String.format(
                        "Unprovision of '%s' skipped. Catalog '%s' not found.", viewFullNameOP, catalogNameOP));
                return right(null);
            }

            Either<FailedOperation, Boolean> eitherExistingSchema =
                    unityCatalogManager.checkSchemaExistence(catalogNameOP, schemaNameOP);
            if (eitherExistingSchema.isLeft()) {
                return left(eitherExistingSchema.getLeft());
            }

            if (!eitherExistingSchema.get()) {
                logger.info(String.format(
                        "Unprovision of '%s' skipped. Schema '%s' in catalog '%s' not found.",
                        viewFullNameOP, schemaNameOP, catalogNameOP));
                return right(null);
            }

            Either<FailedOperation, Void> eitherDeletedView =
                    unityCatalogManager.dropTableIfExists(catalogNameOP, schemaNameOP, viewNameOP);
            if (eitherDeletedView.isLeft()) {
                return left(eitherDeletedView.getLeft());
            }
            logger.info(String.format("Unprovision of '%s' terminated correctly.", viewFullNameOP));

            return right(null);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while unprovisioning '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    viewFullNameOP, e.getMessage());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
    }
}
