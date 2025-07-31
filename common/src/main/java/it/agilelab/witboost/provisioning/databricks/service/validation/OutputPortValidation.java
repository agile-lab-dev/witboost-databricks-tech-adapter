package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.error.platform.NotFound;
import com.witboost.provisioning.model.Column;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.outputport.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OutputPortValidation {

    private static final Logger logger = LoggerFactory.getLogger(OutputPortValidation.class);
    private final MiscConfig miscConfig;
    private final WorkspaceHandler workspaceHandler;

    @Autowired
    public OutputPortValidation(MiscConfig miscConfig, WorkspaceHandler workspaceHandler) {
        this.miscConfig = miscConfig;
        this.workspaceHandler = workspaceHandler;
    }

    public Either<FailedOperation, Void> validate(
            OutputPort<DatabricksOutputPortSpecific> component, String environment) {
        logger.info("Started validation of Output Port {} (id: {}).", component.getName(), component.getId());

        String workspace = component.getSpecific().getWorkspace();
        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherWorkspaceExists =
                workspaceHandler.getWorkspaceInfo(workspace);
        if (eitherWorkspaceExists.isLeft()) {
            return (left(eitherWorkspaceExists.getLeft()));
        }

        Optional<DatabricksWorkspaceInfo> databricksWorkspaceInfoOptional = eitherWorkspaceExists.get();
        if (databricksWorkspaceInfoOptional.isEmpty()) {
            logger.info(
                    "Validation of Output Port {} (id: {}) completed successfully. ATTENTION: couldn't check for source table compliance as workspace doesn't exist yet",
                    component.getName(),
                    component.getId());
            return right(null);
        }

        DatabricksWorkspaceInfo databricksWorkspaceInfo = databricksWorkspaceInfoOptional.get();
        Either<FailedOperation, WorkspaceClient> eitherWorkspaceClient =
                workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
        if (eitherWorkspaceClient.isLeft()) {
            return (left(eitherWorkspaceClient.getLeft()));
        }

        WorkspaceClient workspaceClient = eitherWorkspaceClient.get();

        try {
            workspaceClient.metastores().current();

            Either<FailedOperation, Void> validationResult =
                    validateTableExistenceAndSchema(workspaceClient, component, environment, databricksWorkspaceInfo);

            if (validationResult.isRight())
                logger.info(
                        "Validation of Output Port {} (id: {}) completed successfully.",
                        component.getName(),
                        component.getId());

            return validationResult;
        } catch (NotFound notFoundException) {
            if (notFoundException.getErrorCode().equalsIgnoreCase("METASTORE_DOES_NOT_EXIST")) {

                if (databricksWorkspaceInfo.isManaged()) {
                    logger.info(
                            "Validation of Output Port {} (id: {}) completed successfully. ATTENTION: couldn't check for source table compliance because no metastore assigned for the current workspace",
                            component.getName(),
                            component.getId());
                    return right(null);
                }

                String errorMessage = String.format(
                        "Validation of Output Port %s (id: %s) failed. No metastore assigned for the current workspace",
                        component.getName(), component.getId());
                logger.error(errorMessage);

                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }
            String errorMessage = String.format(
                    "An error occurred while retrieving current metastore for workspace %s. Please try again and if the error persists contact the platform team. Details: %s",
                    workspace, notFoundException.getMessage());
            logger.error(errorMessage, notFoundException);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, notFoundException))));
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while retrieving current metastore for workspace %s. Please try again and if the error persists contact the platform team. Details: %s",
                    workspace, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    /**
     * Validates if the table exists and if the view's schema is a subset of the table schema.
     */
    public Either<FailedOperation, Void> validateTableExistenceAndSchema(
            WorkspaceClient workspaceClient,
            OutputPort<DatabricksOutputPortSpecific> component,
            String environment,
            DatabricksWorkspaceInfo databricksWorkspaceInfo) {

        var unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

        String catalogName = component.getSpecific().getCatalogName();
        String schemaName = component.getSpecific().getSchemaName();
        String tableName = component.getSpecific().getTableName();
        String tableFullName = catalogName + "." + schemaName + "." + tableName;

        logger.info(
                "Checking if the table {} provided in Output Port {} already exists...",
                tableFullName,
                component.getName());

        Either<FailedOperation, Boolean> eitherTableExists =
                unityCatalogManager.checkTableExistence(catalogName, schemaName, tableName);
        if (eitherTableExists.isLeft()) {
            return (left(eitherTableExists.getLeft()));
        }

        Boolean tableExists = eitherTableExists.get();

        if (!tableExists) {
            String errorMessage = String.format(
                            "The table '%s', provided in Output Port %s, does not exist. ",
                            tableFullName, component.getName())
                    + (environment.equalsIgnoreCase(miscConfig.developmentEnvironmentName())
                            ? "Be sure that the table exists by either running the DLT Workload that creates it or creating the table manually."
                            : "Be sure that the DLT Workload that creates it is being deployed correctly and that the table name is correct.");
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }

        logger.info("The table '{}', provided in Output Port {}, exists. ", tableFullName, component.getName());
        logger.info(
                "Checking if the schema provided in the Output Port {} is a subset or, at least, equal to the schema of table '{}'.",
                component.getName(),
                tableFullName);

        Either<FailedOperation, List<String>> eitherOriginalTableColumnNames =
                unityCatalogManager.retrieveTableColumnsNames(catalogName, schemaName, tableName);
        if (eitherOriginalTableColumnNames.isLeft()) {
            return (left(eitherOriginalTableColumnNames.getLeft()));
        }

        List<String> originalTableColumnNames = eitherOriginalTableColumnNames.get();

        var schemaValidation = checkViewSchema(component, originalTableColumnNames, tableFullName);

        if (schemaValidation.isLeft()) return left(schemaValidation.getLeft());

        return right(null);
    }

    private Either<FailedOperation, Void> checkViewSchema(
            OutputPort<DatabricksOutputPortSpecific> component,
            List<String> originalTableColumnNames,
            String tableFullName) {

        List<Column> viewSchemaNode = component.getDataContract().getSchema();

        for (Column viewColumn : viewSchemaNode) {
            String viewColumnName = viewColumn.getName();
            if (!originalTableColumnNames.contains(viewColumnName)) {
                String errorMessage = String.format(
                        "Check for Output Port %s: the column '%s' cannot be found in the table '%s'.",
                        component.getName(), viewColumnName, tableFullName);
                logger.error(errorMessage);
                return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
            }
        }
        return right(null);
    }
}
