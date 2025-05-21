package it.agilelab.witboost.provisioning.databricks.service.provision.handler;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.AccountClient;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.*;
import com.databricks.sdk.service.sql.*;
import com.witboost.provisioning.model.Column;
import com.witboost.provisioning.model.DataContract;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.*;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.object.View;
import it.agilelab.witboost.provisioning.databricks.model.databricks.outputport.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.openapi.model.UpdateAclRequest;
import it.agilelab.witboost.provisioning.databricks.permissions.AzurePermissionsManager;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.azure.AzureMapper;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.databricks.DatabricksMapper;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OutputPortHandler {

    private final Logger logger = LoggerFactory.getLogger(OutputPortHandler.class);

    private final AzureAuthConfig azureAuthConfig;
    private final GitCredentialsConfig gitCredentialsConfig;
    private final AzurePermissionsManager azurePermissionsManager;
    private final MiscConfig miscConfig;
    private final AzureMapper azureMapper;
    private final DatabricksAuthConfig databricksAuthConfig;
    private final DatabricksPermissionsConfig databricksPermissionsConfig;
    private final AccountClient accountClient;

    @Autowired
    public OutputPortHandler(
            AzureAuthConfig azureAuthConfig,
            GitCredentialsConfig gitCredentialsConfig,
            AzurePermissionsManager azurePermissionsManager,
            MiscConfig miscConfig,
            AzureMapper azureMapper,
            DatabricksAuthConfig databricksAuthConfig,
            DatabricksPermissionsConfig databricksPermissionsConfig,
            AccountClient accountClient) {
        this.azureAuthConfig = azureAuthConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
        this.azurePermissionsManager = azurePermissionsManager;
        this.miscConfig = miscConfig;
        this.azureMapper = azureMapper;
        this.databricksAuthConfig = databricksAuthConfig;
        this.databricksPermissionsConfig = databricksPermissionsConfig;
        this.accountClient = accountClient;
    }

    /**
     * Provisions a Databricks View as an Output Port.
     * <p>
     * This method performs several operations to create a Databricks view, configure its permissions, and handle
     * the metadata and descriptions for columns. It supports mapping data product users and groups for permission
     * assignment and ensures the correct operations in a Databricks workspace.
     * </p>
     * <p>
     * The process includes:
     * <ul>
     *     <li>Attaching the metastore if required.</li>
     *     <li>Creating the catalog and schema if they do not already exist.</li>
     *     <li>Creating or replacing the view based on the provided table and columns.</li>
     *     <li>Setting owner and developer permissions for the view.</li>
     *     <li>Adding column-level descriptions (if available).</li>
     *     <li>Adding a description to the view itself.</li>
     * </ul>
     * </p>
     *
     * @param provisionRequest        The request object containing the details required to provision the Output Port.
     *                                This includes the data contract, metadata, catalog, schema, and table details.
     * @param workspaceClient         The client used to interact with the Databricks workspace.
     * @param databricksWorkspaceInfo An object containing metadata about the Databricks workspace where provisioning
     *                                is performed, such as workspace name, host, and other information.
     * @return Either a {@code FailedOperation} object in case of an error, or a {@code TableInfo} object containing
     *         information about the created view if the operation is successful.
     */
    public Either<FailedOperation, TableInfo> provisionOutputPort(
            ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest,
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {

        try {

            OutputPort<DatabricksOutputPortSpecific> outputPort =
                    (OutputPort<DatabricksOutputPortSpecific>) provisionRequest.component();
            DatabricksOutputPortSpecific databricksOutputPortSpecific = outputPort.getSpecific();

            var unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

            // Retrieving fields from request
            String catalogNameOP = databricksOutputPortSpecific.getCatalogNameOP();
            String schemaNameOP = databricksOutputPortSpecific.getSchemaNameOP();
            String viewNameOP = databricksOutputPortSpecific.getViewNameOP();
            String viewFullNameOP = String.format("`%s`.`%s`.`%s`", catalogNameOP, schemaNameOP, viewNameOP);
            String tableFullName = String.format(
                    "`%s`.`%s`.`%s`",
                    databricksOutputPortSpecific.getCatalogName(),
                    databricksOutputPortSpecific.getSchemaName(),
                    databricksOutputPortSpecific.getTableName());

            // If the workspace is set to not be managed by the tech adapter, we don't attach the metastore ourselves
            if (databricksWorkspaceInfo.isManaged()) {
                Either<FailedOperation, Void> eitherAttachedMetastore =
                        unityCatalogManager.attachMetastore(databricksOutputPortSpecific.getMetastore());

                if (eitherAttachedMetastore.isLeft()) return left(eitherAttachedMetastore.getLeft());
            } else {
                logger.info("Skipping metastore attachment as workspace is not managed by Tech Adapter");
            }

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

            String sqlWarehouseName = databricksOutputPortSpecific.getSqlWarehouseName();
            var sqlWarehouseId = getSqlWarehouseIdFromName(workspaceClient, sqlWarehouseName);

            if (sqlWarehouseId.isLeft()) return left(sqlWarehouseId.getLeft());

            String columnsListString = createColumnsListForSelectStatement(provisionRequest);

            // Create OP
            Either<FailedOperation, String> eitherExecutedStatementCreateOutputPort =
                    executeStatementCreateOrReplaceView(
                            catalogNameOP,
                            schemaNameOP,
                            viewNameOP,
                            tableFullName,
                            columnsListString,
                            sqlWarehouseId.get(),
                            workspaceClient);

            if (eitherExecutedStatementCreateOutputPort.isLeft()) {
                return left(eitherExecutedStatementCreateOutputPort.getLeft());
            }

            String statementId = eitherExecutedStatementCreateOutputPort.get();

            Either<FailedOperation, Void> eitherFinishedPolling =
                    pollOnStatementExecution(workspaceClient, statementId);
            if (eitherFinishedPolling.isLeft()) {
                return left(eitherFinishedPolling.getLeft());
            }

            logger.info(
                    String.format("Output Port '%s' is now available. Start setting permissions. ", viewFullNameOP));

            // Re-assign (eventual) permissions
            String dpOwner = provisionRequest.dataProduct().getDataProductOwner();
            String devGroup = provisionRequest.dataProduct().getDevGroup();

            // TODO: This is a temporary solution. Remove or update this logic in the future.
            if (!devGroup.startsWith("group:")) devGroup = "group:" + devGroup;

            DatabricksMapper databricksMapper = new DatabricksMapper(accountClient);

            Map<String, Either<Throwable, String>> eitherMap = databricksMapper.map(Set.of(dpOwner, devGroup));

            Either<Throwable, String> eitherDpOwnerMapped = eitherMap.get(dpOwner);
            if (eitherDpOwnerMapped.isLeft()) {
                var error = eitherDpOwnerMapped.getLeft();
                return left(new FailedOperation(Collections.singletonList(new Problem(error.getMessage(), error))));
            }
            String dpOwnerMapped = eitherDpOwnerMapped.get();

            Either<Throwable, String> eitherDpDevGroupMapped = eitherMap.get(devGroup);
            if (eitherDpDevGroupMapped.isLeft()) {
                var error = eitherDpDevGroupMapped.getLeft();
                return left(new FailedOperation(Collections.singletonList(new Problem(error.getMessage(), error))));
            }
            String dpDevGroupMapped = eitherDpDevGroupMapped.get();

            String environment = provisionRequest.dataProduct().getEnvironment();

            if (environment.equalsIgnoreCase(miscConfig.developmentEnvironmentName())) {

                String ownerPermissionLevelConfig =
                        databricksPermissionsConfig.getOutputPort().getOwner();
                String developerPermissionLevelConfig =
                        databricksPermissionsConfig.getOutputPort().getDeveloper();

                Either<FailedOperation, Void> eitherAssignedPermissionsViewOPToOwner =
                        unityCatalogManager.assignDatabricksPermissionToTableOrView(
                                dpOwnerMapped,
                                Privilege.valueOf(ownerPermissionLevelConfig),
                                new View(catalogNameOP, schemaNameOP, viewNameOP));
                if (eitherAssignedPermissionsViewOPToOwner.isLeft()) {
                    return left(eitherAssignedPermissionsViewOPToOwner.getLeft());
                }

                Either<FailedOperation, Void> eitherAssignedPermissionsViewOPToDevGroup =
                        unityCatalogManager.assignDatabricksPermissionToTableOrView(
                                dpDevGroupMapped,
                                Privilege.valueOf(developerPermissionLevelConfig),
                                new View(catalogNameOP, schemaNameOP, viewNameOP));
                if (eitherAssignedPermissionsViewOPToDevGroup.isLeft()) {
                    return left(eitherAssignedPermissionsViewOPToDevGroup.getLeft());
                }
            }

            // Add description on columns (if descriptions are not empty)
            DataContract dataContract = outputPort.getDataContract();
            for (Column col : dataContract.getSchema()) {

                Either<FailedOperation, Optional<String>> eitherOptionalStatementIdAddColumn =
                        executeStatementCommentOnColumn(
                                catalogNameOP, schemaNameOP, viewNameOP, col, sqlWarehouseId.get(), workspaceClient);
                if (eitherOptionalStatementIdAddColumn.isLeft())
                    return left(eitherOptionalStatementIdAddColumn.getLeft());

                Optional<String> optionalStatementIdAddColumn = eitherOptionalStatementIdAddColumn.get();

                if (optionalStatementIdAddColumn.isPresent()) {

                    String statementIdAddColumn = optionalStatementIdAddColumn.get();

                    Either<FailedOperation, Void> eitherStatementExecutionAddColumnFinished =
                            pollOnStatementExecution(workspaceClient, statementIdAddColumn);
                    if (eitherStatementExecutionAddColumnFinished.isLeft())
                        return left(eitherStatementExecutionAddColumnFinished.getLeft());
                }
            }

            // Add VIEW description
            Either<FailedOperation, Optional<String>> eitherOptionalStatementIdAlterViewSetDescription =
                    executeStatementAlterViewSetDescription(
                            catalogNameOP,
                            schemaNameOP,
                            viewNameOP,
                            outputPort.getDescription(),
                            sqlWarehouseId.get(),
                            workspaceClient);
            if (eitherOptionalStatementIdAlterViewSetDescription.isLeft())
                return left(eitherOptionalStatementIdAlterViewSetDescription.getLeft());

            Optional<String> optionalStatementIdAlterViewSetDescription =
                    eitherOptionalStatementIdAlterViewSetDescription.get();

            if (optionalStatementIdAlterViewSetDescription.isPresent()) {
                // Poll on status
                String statementIdAlterViewSetDescription = optionalStatementIdAlterViewSetDescription.get();

                Either<FailedOperation, Void> eitherStatementExecutionAlterViewSetDescriptionFinished =
                        pollOnStatementExecution(workspaceClient, statementIdAlterViewSetDescription);
                if (eitherStatementExecutionAlterViewSetDescriptionFinished.isLeft())
                    return left(eitherStatementExecutionAlterViewSetDescriptionFinished.getLeft());
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

    /**
     * Creates a comma-separated list of column names for a SELECT statement.
     * <p>
     * If the schema contains no columns, the SELECT statement will default to all columns using "*".
     * Otherwise, the column names from the schema are concatenated into a comma-separated list.
     * </p>
     *
     * @param provisionRequest The request containing the schema definition used to build the SELECT statement.
     * @return A string representing the comma-separated list of column names for the SELECT statement.
     */
    private String createColumnsListForSelectStatement(
            ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest) {
        OutputPort<DatabricksOutputPortSpecific> component =
                (OutputPort<DatabricksOutputPortSpecific>) provisionRequest.component();

        List<Column> viewSchemaNode = component.getDataContract().getSchema();

        if (viewSchemaNode.isEmpty()) {
            return "*";
        } else {
            List<String> columnsList = new ArrayList<>();

            for (Column viewColumn : viewSchemaNode) {
                String viewColumnName = viewColumn.getName();
                columnsList.add(viewColumnName);
            }

            return String.join(",", columnsList);
        }
    }

    /**
     * Creates an SQL statement to create or replace a Databricks view.
     * <p>
     * This method generates an SQL query using the provided view name, table name,
     * and the list of columns to include in the SELECT clause.
     * </p>
     *
     * @param viewNameOP    The name of the view to be created or replaced.
     * @param tableFullName The fully qualified name of the underlying table.
     * @param columnsList   A comma-separated list of column names for the SELECT statement.
     * @return A string representing the SQL query for creating or replacing the view.
     */
    private String createStatementCreateOrReplaceView(String viewNameOP, String tableFullName, String columnsList) {
        return String.format(
                "CREATE OR REPLACE VIEW `%s` AS SELECT %s FROM %s;", viewNameOP, columnsList, tableFullName);
    }

    /**
     * Executes a SQL statement to create or replace a Databricks view.
     * <p>
     * Generates the SQL statement based on the provided parameters, executes it, and polls for its status
     * until the operation completes. If it fails, the method returns an appropriate error message.
     * </p>
     *
     * @param catalogNameOP   The catalog name containing the view.
     * @param schemaNameOP    The schema name containing the view.
     * @param viewNameOP      The name of the view being created or replaced.
     * @param tableFullName   The fully qualified name of the underlying table.
     * @param columnsList     A comma-separated list of columns for the SELECT statement.
     * @param sqlWarehouseId  The SQL warehouse ID used to execute the query.
     * @param workspaceClient The Databricks workspace client.
     * @return Either a {@code FailedOperation} if an error occurs, or the statement ID if successful.
     */
    private Either<FailedOperation, String> executeStatementCreateOrReplaceView(
            String catalogNameOP,
            String schemaNameOP,
            String viewNameOP,
            String tableFullName,
            String columnsList,
            String sqlWarehouseId,
            WorkspaceClient workspaceClient) {

        try {

            String queryToCreateOP = createStatementCreateOrReplaceView(viewNameOP, tableFullName, columnsList);

            Either<FailedOperation, String> eitherStatementId =
                    executeQuery(queryToCreateOP, catalogNameOP, schemaNameOP, sqlWarehouseId, workspaceClient);
            if (eitherStatementId.isLeft()) return left(eitherStatementId.getLeft());

            return right(eitherStatementId.get());

        } catch (Exception e) {

            String viewFullName = catalogNameOP + "." + schemaNameOP + "." + viewNameOP;

            String errorMessage = String.format(
                    "An error occurred while creating view '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    viewFullName, e.getMessage());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
    }
    /**
     * Creates a SQL statement to add a comment to a specific column within a view.
     *
     * @param viewNameOP The fully qualified name of the view.
     * @param column     The column for which the comment is being added.
     * @return An {@code Optional} containing the SQL statement if the column has a description,
     *         or {@code Optional.empty()} if the description is null or empty.
     */
    private Optional<String> createStatementCommentColumn(String viewNameOP, Column column) {
        // COMMENT ON COLUMN test_view.id IS 'This is my id column'

        if (column.getDescription() != null
                && !(column.getDescription().isEmpty())
                && !(column.getDescription().isBlank())) {
            return Optional.of(String.format(
                    "COMMENT ON COLUMN %s.%s IS \"%s\"", viewNameOP, column.getName(), column.getDescription()));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Executes a SQL statement to add a comment to a specific column within a view.
     * <p>
     * If the column has a non-empty description, the generated query is executed,
     * and the statement ID is returned. Otherwise, the method simply returns an empty result.
     * </p>
     *
     * @param catalogNameOP   The name of the catalog to which the view belongs.
     * @param schemaNameOP    The name of the schema to which the view belongs.
     * @param viewNameOP      The name of the view containing the column.
     * @param column          The column for which the comment is being added.
     * @param sqlWarehouseId  The ID of the SQL warehouse used for executing the statement.
     * @param workspaceClient The Databricks workspace client used to execute the query.
     * @return Either a {@code FailedOperation} containing error details, or an {@code Optional<String>}
     *         containing the statement ID if the operation succeeded.
     */
    public Either<FailedOperation, Optional<String>> executeStatementCommentOnColumn(
            String catalogNameOP,
            String schemaNameOP,
            String viewNameOP,
            Column column,
            String sqlWarehouseId,
            WorkspaceClient workspaceClient) {

        try {
            Optional<String> optionalQuery = createStatementCommentColumn(viewNameOP, column);

            if (optionalQuery.isPresent()) {
                String query = optionalQuery.get();
                logger.info(String.format("Query to add description on columns for VIEW '%s': %s", viewNameOP, query));
                Either<FailedOperation, String> eitherStatementId =
                        executeQuery(query, catalogNameOP, schemaNameOP, sqlWarehouseId, workspaceClient);
                if (eitherStatementId.isLeft()) return left(eitherStatementId.getLeft());

                return right(Optional.of(eitherStatementId.get()));
            } else {
                return right(Optional.empty());
            }

        } catch (Exception e) {

            String viewFullName = catalogNameOP + "." + schemaNameOP + "." + viewNameOP;

            String errorMessage = String.format(
                    "An error occurred while adding column comments to view '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    viewFullName, e.getMessage());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
    }

    /**
     * Creates a SQL statement to set a description for a Databricks view.
     * <p>
     * The description is added as a `TBLPROPERTIES` property of the view.
     * If the description is null or blank, the method returns {@code Optional.empty()}.
     * </p>
     *
     * @param viewNameOP      The fully qualified name of the view.
     * @param viewDescription The description to set for the view.
     * @return An {@code Optional<String>} containing the SQL statement if the description is provided.
     */
    private Optional<String> createStatementAlterViewSetDescription(String viewNameOP, String viewDescription) {
        // ALTER VIEW `catalog`.`ic-schema-v01`.`test_view` SET TBLPROPERTIES ('comment' = 'desc test')

        if (viewDescription != null && !(viewDescription.isBlank())) {
            return Optional.of(
                    String.format("ALTER VIEW %s SET TBLPROPERTIES ('comment' = \"%s\")", viewNameOP, viewDescription));

        } else {
            return Optional.empty();
        }
    }

    /**
     * Executes a SQL statement to set a description for a Databricks view.
     * <p>
     * This method generates the SQL query to set the view description and executes it using
     * the Databricks SQL warehouse. If successful, it returns the statement ID.
     * </p>
     *
     * @param catalogNameOP      The name of the catalog to which the view belongs.
     * @param schemaNameOP       The name of the schema to which the view belongs.
     * @param viewNameOP         The name of the view being updated.
     * @param viewDescription    The description to set for the view.
     * @param sqlWarehouseId     The ID of the SQL warehouse used for executing the statement.
     * @param workspaceClient    The Databricks workspace client used to execute the query.
     * @return Either a {@code FailedOperation} containing error details, or an {@code Optional<String>}
     *         containing the statement ID if the operation succeeds.
     */
    public Either<FailedOperation, Optional<String>> executeStatementAlterViewSetDescription(
            String catalogNameOP,
            String schemaNameOP,
            String viewNameOP,
            String viewDescription,
            String sqlWarehouseId,
            WorkspaceClient workspaceClient) {

        try {
            Optional<String> optionalQuery = createStatementAlterViewSetDescription(viewNameOP, viewDescription);

            if (optionalQuery.isPresent()) {
                String query = optionalQuery.get();
                logger.info(String.format("Query to set VIEW description '%s': %s", viewNameOP, query));
                Either<FailedOperation, String> eitherStatementId =
                        executeQuery(query, catalogNameOP, schemaNameOP, sqlWarehouseId, workspaceClient);
                if (eitherStatementId.isLeft()) return left(eitherStatementId.getLeft());

                return right(Optional.of(eitherStatementId.get()));
            } else {
                return right(Optional.empty());
            }

        } catch (Exception e) {

            String viewFullName = catalogNameOP + "." + schemaNameOP + "." + viewNameOP;

            String errorMessage = String.format(
                    "An error occurred while adding description to view '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    viewFullName, e.getMessage());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
    }

    /**
     * Executes a custom query in a Databricks workspace.
     * <p>
     * The query is executed in the context of a specified catalog, schema,
     * and SQL warehouse. If successful, the statement ID is returned.
     * </p>
     *
     * @param query           The SQL query to execute.
     * @param catalogNameOP   The name of the catalog used for the query.
     * @param schemaNameOP    The name of the schema used for the query.
     * @param sqlWarehouseId  The ID of the SQL warehouse used for executing the query.
     * @param workspaceClient The Databricks workspace client used to run the query.
     * @return Either a {@code FailedOperation} containing error details, or a string containing
     *         the statement ID if the operation succeeds.
     */
    public Either<FailedOperation, String> executeQuery(
            String query,
            String catalogNameOP,
            String schemaNameOP,
            String sqlWarehouseId,
            WorkspaceClient workspaceClient) {

        try {
            logger.debug("Query: {}", query);

            ExecuteStatementRequest request = new ExecuteStatementRequest()
                    .setCatalog(catalogNameOP)
                    .setSchema(schemaNameOP)
                    .setStatement(query)
                    .setWarehouseId(sqlWarehouseId);

            String statementId = workspaceClient
                    .statementExecution()
                    .executeStatement(request)
                    .getStatementId();

            return right(statementId);
        } catch (Exception e) {

            String errorMessage = String.format(
                    "An error occurred while running query '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    query, e.getMessage());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
    }

    /**
     * Polls the status of a statement execution in the Databricks workspace.
     * <p>
     * The method repeatedly checks the status of the SQL statement execution
     * until it completes or fails. If the statement succeeds, it returns successfully;
     * otherwise, an error is returned.
     * </p>
     *
     * @param workspaceClient The Databricks workspace client used to run the query.
     * @param statementId  The ID of the statement being monitored.
     * @return Either a {@code FailedOperation} if the statement fails, or {@code Void} if it succeeds.
     */
    private Either<FailedOperation, Void> pollOnStatementExecution(
            WorkspaceClient workspaceClient, String statementId) {

        var isStatementRunning = true;

        while (isStatementRunning) {

            StatementResponse getStatementResponse =
                    workspaceClient.statementExecution().getStatement(statementId);

            StatementState statementState = getStatementResponse.getStatus().getState();

            String logMessage = String.format("Status of statement (id: %s): %s. ", statementId, statementState);

            switch (statementState) {
                case PENDING, RUNNING:
                    logger.info(logMessage + "Still polling.");
                    break;
                case FAILED, CANCELED, CLOSED:
                    String errorMessage = String.format(
                            "%s. Details: %s",
                            logMessage,
                            getStatementResponse.getStatus().getError().getMessage());
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

    /**
     * Retrieves the ID of a Databricks SQL warehouse using its name.
     * <p>
     * This method searches the available SQL warehouses in the Databricks workspace
     * to find a matching warehouse name. If found, it returns the warehouse ID.
     * If no match is found, an error is returned.
     * </p>
     *
     * @param workspaceClient The Databricks workspace client used to retrieve the list of warehouses.
     * @param sqlWarehouseName The name of the SQL warehouse to search for.
     * @return Either a {@code FailedOperation} with the error details, or a {@code String} containing
     *         the SQL warehouse ID if it is found.
     */
    public Either<FailedOperation, String> getSqlWarehouseIdFromName(
            WorkspaceClient workspaceClient, String sqlWarehouseName) {

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

    /**
     * Unprovisions a Databricks Output Port.
     * <p>
     * This method removes an existing Databricks Output Port view and
     * associated metadata from a specific catalog and schema in the Databricks workspace.
     * </p>
     *
     * @param provisionRequest       The request object containing the details required for unprovisioning
     *                               the Output Port.
     * @param workspaceClient        The client used to interact with the Databricks workspace.
     * @param databricksWorkspaceInfo An object containing metadata about the Databricks workspace where
     *                                the unprovisioning is performed, such as the workspace name and host information.
     * @return Either a {@code FailedOperation} object containing error details if the operation fails,
     *         or a {@code Void} result if the operation is successful.
     */
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

    /**
     * Updates the Access Control List (ACL) for a Databricks Output Port.
     * <p>
     * This method maps the provided list of references to Databricks user or group IDs, removes any
     * permissions for entities no longer in the ACL, and assigns SELECT privileges to the new set of references.
     * </p>
     * <p>
     * The process includes:
     * <ul>
     *     <li>Mapping the provided references to Databricks IDs.</li>
     *     <li>Removing SELECT grants for principals no longer in the reference list.</li>
     *     <li>Adding SELECT grants for all provided references.</li>
     * </ul>
     * </p>
     *
     * @param provisionRequest       The request object containing the details about the Output Port
     *                               and the environment where the ACL update is being performed.
     * @param updateAclRequest       The request object containing the list of references (users or groups)
     *                               affected by the ACL update.
     * @param workspaceClient        The Databricks workspace client used to interact with the Databricks instance.
     * @param unityCatalogManager    The manager used to handle Unity Catalog interactions for permissions and metadata.
     * @return Either a {@code FailedOperation} object providing error details if the operation fails,
     *         or a {@code ProvisioningStatus} object if the ACL update completes successfully.
     */
    public Either<FailedOperation, ProvisioningStatus> updateAcl(
            ProvisionRequest<DatabricksOutputPortSpecific> provisionRequest,
            UpdateAclRequest updateAclRequest, // only for the refs
            WorkspaceClient workspaceClient,
            UnityCatalogManager unityCatalogManager) {

        logger.info("Start retrieving info needed to update Access Control List");

        DatabricksOutputPortSpecific databricksOutputPortSpecific =
                provisionRequest.component().getSpecific();
        String catalogNameOP = databricksOutputPortSpecific.getCatalogNameOP();
        String schemaNameOP = databricksOutputPortSpecific.getSchemaNameOP();
        String viewNameOP = databricksOutputPortSpecific.getViewNameOP();

        String environment = provisionRequest.dataProduct().getEnvironment();

        logger.info(String.format(
                "Start updating Access Control List for %s.%s.%s", catalogNameOP, schemaNameOP, viewNameOP));

        // Mapping of refs entities
        DatabricksMapper databricksMapper = new DatabricksMapper(accountClient);

        List<String> refs = updateAclRequest.getRefs();
        Set<String> refsSet = new HashSet<>(refs);

        Map<String, Either<Throwable, String>> eitherMapRefs = databricksMapper.map(refsSet);

        // Retrieve refs mapped
        List<String> mappedRefs = new ArrayList<>();

        List<Problem> problemsMapping = new ArrayList<>();

        refs.forEach(ref -> {
            Either<Throwable, String> eitherDatabricksId = eitherMapRefs.get(ref);
            if (eitherDatabricksId.isLeft()) {
                problemsMapping.add(new Problem(eitherDatabricksId.getLeft().toString()));
            } else {
                mappedRefs.add(eitherDatabricksId.get());
            }
        });

        if (!problemsMapping.isEmpty()) {
            logger.error(
                    "An error occurred while mapping Databricks entities. Please try again and if the error persists contact the platform team. ");
            for (Problem problem : problemsMapping) {
                logger.error("Problem: {}", problem.description());
            }
            return left(new FailedOperation(problemsMapping));
        }

        // Creating Databricks object View
        View viewOP = new View(catalogNameOP, schemaNameOP, viewNameOP);

        // Step 1: remove grants for entities that are no longer in refs
        logger.info("Retrieving current permissions on output port");

        Either<FailedOperation, Collection<PrivilegeAssignment>> eitherCurrentPermissions =
                unityCatalogManager.retrieveDatabricksPermissions(SecurableType.TABLE, viewOP);

        if (eitherCurrentPermissions.isLeft()) {
            return left(eitherCurrentPermissions.getLeft());
        }

        List<Problem> problemsRemovingPermissions = new ArrayList<>();

        Collection<PrivilegeAssignment> currentPermissions = eitherCurrentPermissions.get();

        // Permissions
        String dpOwner = provisionRequest.dataProduct().getDataProductOwner();
        String devGroup = provisionRequest.dataProduct().getDevGroup();

        // TODO: This is a temporary solution. Remove or update this logic in the future.
        if (!devGroup.startsWith("group:")) devGroup = "group:" + devGroup;

        Map<String, Either<Throwable, String>> eitherMap = databricksMapper.map(Set.of(dpOwner, devGroup));

        // Map DP OWNER
        Either<Throwable, String> eitherDpOwnerMapped = eitherMap.get(dpOwner);
        if (eitherDpOwnerMapped.isLeft()) {
            var error = eitherDpOwnerMapped.getLeft();
            return left(new FailedOperation(Collections.singletonList(new Problem(error.getMessage(), error))));
        }
        String dpOwnerMapped = eitherDpOwnerMapped.get();

        // Map DEV GROUP
        Either<Throwable, String> eitherDpDevGroupMapped = eitherMap.get(devGroup);
        if (eitherDpDevGroupMapped.isLeft()) {
            var error = eitherDpDevGroupMapped.getLeft();
            return left(new FailedOperation(Collections.singletonList(new Problem(error.getMessage(), error))));
        }
        String dpDevGroupMapped = eitherDpDevGroupMapped.get();

        currentPermissions.forEach(privilegeAssignment -> {
            String principal = privilegeAssignment.getPrincipal();

            if (environment.equalsIgnoreCase(miscConfig.developmentEnvironmentName())
                    & (Objects.equals(principal, dpOwnerMapped) | Objects.equals(principal, dpDevGroupMapped))) {
                logger.info(String.format(
                        "Environment is %s and so, privileges of %s (Data Product Owner or Development Group) are not removed",
                        environment, principal));
            } else {
                if (!mappedRefs.contains(principal)) {

                    logger.info(String.format(
                            "Principal %s does not have SELECT permission any longer on table %s. Removing grant.",
                            principal, viewOP.fullyQualifiedName()));

                    Either<FailedOperation, Void> eitherUpdatedPermissions =
                            unityCatalogManager.updateDatabricksPermissions(
                                    principal, Privilege.SELECT, Boolean.FALSE, viewOP);

                    if (eitherUpdatedPermissions.isLeft()) {
                        problemsRemovingPermissions.add(
                                new Problem(eitherUpdatedPermissions.getLeft().toString()));
                    } else {
                        logger.info(String.format(
                                "SELECT permission removed from %s for principal %s successfully!",
                                viewOP.fullyQualifiedName(), principal));
                    }
                }
            }
        });

        if (!problemsRemovingPermissions.isEmpty()) {
            logger.error(
                    "An error occurred while removing permissions on Databricks entities. Please try again and if the error persists contact the platform team. ");
            for (Problem problem : problemsRemovingPermissions) {
                logger.error("Problem: {}", problem.description());
            }
            return left(new FailedOperation(problemsRemovingPermissions));
        }

        // Step 2: assign grants for all entities in refs
        List<Problem> problemsAddingPermissions = new ArrayList<>();

        mappedRefs.forEach(databricksId -> {
            logger.info(String.format("Assigning permissions to Databricks entity: %s", databricksId));

            Either<FailedOperation, Void> eitherAssignedPermissions =
                    unityCatalogManager.assignDatabricksPermissionSelectToTableOrView(databricksId, viewOP);

            if (eitherAssignedPermissions.isLeft()) {
                problemsAddingPermissions.addAll(
                        eitherAssignedPermissions.getLeft().problems());
            } else {
                logger.info(String.format(
                        "SELECT permission on '%s' added for Databricks Id %s successfully!",
                        viewOP.fullyQualifiedName(), databricksId));
            }
        });

        if (!problemsAddingPermissions.isEmpty()) {
            logger.error(
                    "An error occurred while adding SELECT grant to Databricks entities. Please try again and if the error persists contact the platform team. ");
            for (Problem problem : problemsAddingPermissions) {
                logger.error("Problem: {}", problem.description());
            }
            return left(new FailedOperation(problemsAddingPermissions));
        }

        return right(new ProvisioningStatus(ProvisioningStatus.StatusEnum.COMPLETED, "Update of Acl completed!"));
    }
}
