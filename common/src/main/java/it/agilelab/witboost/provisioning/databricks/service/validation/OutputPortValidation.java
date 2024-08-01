package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.TableExistsResponse;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TablesAPI;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksOutputPortSpecific;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OutputPortValidation {

    private static final Logger logger = LoggerFactory.getLogger(OutputPortValidation.class);
    private final MiscConfig miscConfig;

    private final TablesAPI tablesAPI;

    @Autowired
    public OutputPortValidation(MiscConfig miscConfig, TablesAPI tablesAPI) {
        this.miscConfig = miscConfig;
        this.tablesAPI = tablesAPI;
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

        boolean tableExists = checkIfTableExists(tableFullName);

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
        List<String> originalTableColumnNames = retrieveTableColumnsNames(tableFullName);

        var schemaValidation = checkViewSchema(component, originalTableColumnNames, tableFullName);

        if (schemaValidation.isLeft()) return left(schemaValidation.getLeft());

        logger.info(String.format(
                "Validation of Output Port %s (id: %s) completed successfully",
                component.getName(), component.getId()));

        return right(null);
    }

    private Boolean checkIfTableExists(String tableFullName) {

        TableExistsResponse tableExistsResponse = tablesAPI.exists(tableFullName);

        return tableExistsResponse.getTableExists();
    }

    private List<String> retrieveTableColumnsNames(String tableFullName) {

        List<String> colNames = new ArrayList<>();

        TableInfo tableInfo = tablesAPI.get(tableFullName);

        for (ColumnInfo column : tableInfo.getColumns()) {
            colNames.add(column.getName());
        }

        return colNames;
    }

    private Either<FailedOperation, Void> checkViewSchema(
            OutputPort<DatabricksOutputPortSpecific> component,
            List<String> originalTableColumnNames,
            String tableFullName) {

        JsonNode viewSchemaNode = component.getDataContract().get("schema");

        for (JsonNode viewColumn : viewSchemaNode) {
            String viewColumnName = viewColumn.get("name").asText();
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
