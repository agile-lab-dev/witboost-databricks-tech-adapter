package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.witboost.provisioning.model.Column;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.params.ApiClientConfigParams;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.outputport.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import java.util.*;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OutputPortValidation {

    private static final Logger logger = LoggerFactory.getLogger(OutputPortValidation.class);
    private final MiscConfig miscConfig;
    private final WorkspaceHandler workspaceHandler;
    private final Function<ApiClientConfigParams, ApiClient> apiClientFactory;

    @Autowired
    public OutputPortValidation(
            MiscConfig miscConfig,
            WorkspaceHandler workspaceHandler,
            Function<ApiClientConfigParams, ApiClient> apiClientFactory) {
        this.miscConfig = miscConfig;
        this.apiClientFactory = apiClientFactory;
        this.workspaceHandler = workspaceHandler;
    }

    public Either<FailedOperation, Void> validate(
            OutputPort<DatabricksOutputPortSpecific> component, String environment) {

        logger.info(
                String.format("Checking if the table provided in Output Port %s already exists", component.getName()));
        String catalogName = component.getSpecific().getCatalogName();
        String schemaName = component.getSpecific().getSchemaName();
        String tableName = component.getSpecific().getTableName();
        logger.info("catalog: " + catalogName);
        logger.info("schema: " + schemaName);
        logger.info("table: " + tableName);

        String tableFullName = catalogName + "." + schemaName + "." + tableName;

        String workspaceName = component.getSpecific().getWorkspace();

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherWorkspaceExists =
                workspaceHandler.getWorkspaceInfo(workspaceName);
        if (eitherWorkspaceExists.isLeft()) {
            return (left(eitherWorkspaceExists.getLeft()));
        }

        Optional<DatabricksWorkspaceInfo> databricksWorkspaceInfoOptional = eitherWorkspaceExists.get();
        if (databricksWorkspaceInfoOptional.isEmpty()) {
            String errorMessage = String.format("Validation failed. Workspace '%s' not found.", workspaceName);
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }

        DatabricksWorkspaceInfo databricksWorkspaceInfo = databricksWorkspaceInfoOptional.get();

        Either<FailedOperation, WorkspaceClient> eitherWorkspaceClient =
                workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
        if (eitherWorkspaceClient.isLeft()) {
            return (left(eitherWorkspaceClient.getLeft()));
        }

        WorkspaceClient workspaceClient = eitherWorkspaceClient.get();

        var unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

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
                            : "Be sure that the DLT Workload is being deployed correctly and that the table name is correct.");
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        }
        logger.info(String.format(
                "The table '%s', provided in Output Port %s, exists. ", tableFullName, component.getName()));

        logger.info(String.format(
                "Checking if the schema provided in the Output Port %s is a subset or, at least, equal to the schema of table '%s'.",
                component.getName(), tableFullName));

        Either<FailedOperation, List<String>> eitherOriginalTableColumnNames =
                unityCatalogManager.retrieveTableColumnsNames(catalogName, schemaName, tableName);
        if (eitherOriginalTableColumnNames.isLeft()) {
            return (left(eitherOriginalTableColumnNames.getLeft()));
        }

        List<String> originalTableColumnNames = eitherOriginalTableColumnNames.get();

        var schemaValidation = checkViewSchema(component, originalTableColumnNames, tableFullName);

        if (schemaValidation.isLeft()) return left(schemaValidation.getLeft());

        logger.info(String.format(
                "Validation of Output Port %s (id: %s) completed successfully",
                component.getName(), component.getId()));

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
