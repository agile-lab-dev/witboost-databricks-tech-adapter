package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.*;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnityCatalogManager {
    private final WorkspaceClient workspaceClient;
    private final DatabricksWorkspaceInfo databricksWorkspaceInfo;

    public UnityCatalogManager(WorkspaceClient workspaceClient, DatabricksWorkspaceInfo databricksWorkspaceInfo) {
        this.workspaceClient = workspaceClient;
        this.databricksWorkspaceInfo = databricksWorkspaceInfo;
    }

    private final Logger logger = LoggerFactory.getLogger(UnityCatalogManager.class);

    public Either<FailedOperation, Void> attachMetastore(String metastoreName) {

        try {
            logger.info(String.format(
                    "Attaching the workspace %s to the metastore %s",
                    databricksWorkspaceInfo.getName(), metastoreName));
            var metastoreId = getMetastoreId(metastoreName);
            if (metastoreId.isLeft()) return left(metastoreId.getLeft());

            workspaceClient
                    .metastores()
                    .assign(new CreateMetastoreAssignment()
                            .setWorkspaceId(Long.valueOf(databricksWorkspaceInfo.getId()))
                            .setMetastoreId(metastoreId.get()));

            return right(null);

        } catch (Exception e) {
            String error = String.format(
                    "Error linking the workspace %s to the metastore %s",
                    databricksWorkspaceInfo.getName(), metastoreName);
            logger.error(error, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(error, e))));
        }
    }

    public Either<FailedOperation, Void> createCatalogIfNotExists(String catalogName) {
        try {
            Either<FailedOperation, Boolean> eitherCatalogExists = checkCatalogExistence(catalogName);
            if (eitherCatalogExists.isLeft()) return left(eitherCatalogExists.getLeft());

            boolean catalogExists = eitherCatalogExists.get();

            if (!catalogExists) {
                var createCatalog = createCatalog(catalogName);
                if (createCatalog.isLeft()) return left(createCatalog.getLeft());
            }

            return right(null);
        } catch (Exception e) {

            String errorMessage = String.format(
                    "An error occurred while creating unity catalog '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    catalogName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private Either<FailedOperation, Void> createCatalog(String catalogName) {
        try {
            logger.info(
                    String.format("Creating unityCatalog %s in %s", catalogName, databricksWorkspaceInfo.getName()));
            workspaceClient.catalogs().create(catalogName);
            return right(null);

        } catch (Exception e) {

            String errorMessage = String.format(
                    "An error occurred while creating unity catalog '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    catalogName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private Either<FailedOperation, String> getMetastoreId(String metastoreName) {
        var metastoreList = workspaceClient.metastores().list();
        if (metastoreList != null) {
            for (var metastoreInfo : metastoreList) {
                if (metastoreInfo.getName().equalsIgnoreCase(metastoreName)) {
                    return right(metastoreInfo.getMetastoreId());
                }
            }
        }

        String errorMessage = String.format(
                "An error occurred while searching metastore '%s' details. Please try again and if the error persists contact the platform team. Details: Metastore not found",
                metastoreName);
        logger.error(errorMessage);
        return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
    }

    public Either<FailedOperation, Boolean> checkCatalogExistence(String catalogName) {
        try {
            var catalogsList = workspaceClient.catalogs().list(new ListCatalogsRequest());
            if (catalogsList != null) {
                for (CatalogInfo catalogInfo : catalogsList) {
                    if (catalogInfo.getName().equalsIgnoreCase(catalogName)) {
                        return right(true);
                    }
                }
            }

            return right(false);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred trying to search the catalog %s. Please try again and if the error persists contact the platform team. Details: %s",
                    catalogName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    public Either<FailedOperation, Void> createSchemaIfNotExists(String catalogName, String schemaName) {
        try {
            Either<FailedOperation, Boolean> eitherSchemaExists = checkSchemaExistence(catalogName, schemaName);
            if (eitherSchemaExists.isLeft()) return left(eitherSchemaExists.getLeft());

            boolean schemaExists = eitherSchemaExists.get();

            if (!schemaExists) {
                var createSchema = createSchema(catalogName, schemaName);
                if (createSchema.isLeft()) return left(createSchema.getLeft());
            }

            return right(null);
        } catch (Exception e) {

            String errorMessage = String.format(
                    "An error occurred while creating schema '%s' in catalog '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    schemaName, catalogName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private Either<FailedOperation, Void> createSchema(String catalogName, String schemaName) {
        try {
            logger.info(String.format(
                    "Creating schema '%s' in catalog '%s', in workspace %s",
                    schemaName, catalogName, databricksWorkspaceInfo.getName()));
            workspaceClient.schemas().create(schemaName, catalogName);
            return right(null);

        } catch (Exception e) {

            String errorMessage = String.format(
                    "An error occurred while creating schema '%s' in catalog '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    schemaName, catalogName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    public Either<FailedOperation, Boolean> checkSchemaExistence(String catalogName, String schemaName) {
        try {
            var catalogExists = checkCatalogExistence(catalogName);

            if (catalogExists.isLeft()) {
                // checkCatalogExistence returns Left, so it fails for various reason
                return left(catalogExists.getLeft());
            }

            if (!catalogExists.get()) {
                // checkCatalogExistence returns Right(false), so the catalog does not exist
                return left(new FailedOperation(Collections.singletonList(new Problem(String.format(
                        "An error occurred trying to search the schema '%s' in catalog '%s': catalog '%s' does not exist!",
                        schemaName, catalogName, catalogName)))));
            } else {
                // checkCatalogExistence returns Right(true), so the catalog exists, so I can proceed with schemas check
                var schemasList = workspaceClient.schemas().list(catalogName);
                if (schemasList != null) {
                    for (SchemaInfo schemaInfo : schemasList) {
                        if (schemaInfo.getName().equalsIgnoreCase(schemaName)) {
                            return right(true);
                        }
                    }
                }
            }
            return right(false);

        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred trying to search the schema '%s' in catalog '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    schemaName, catalogName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    public Either<FailedOperation, TableInfo> getTableInfo(String catalogName, String schemaName, String tableName) {
        String tableFullName = retrieveTableFullName(catalogName, schemaName, tableName);
        return right(workspaceClient.tables().get(tableFullName));
    }

    public Either<FailedOperation, Boolean> checkTableExistence(
            String catalogName, String schemaName, String tableName) {

        String tableFullName = retrieveTableFullName(catalogName, schemaName, tableName);

        try {

            TableExistsResponse tableExistsResponse = workspaceClient.tables().exists(tableFullName);
            return right(tableExistsResponse.getTableExists());
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while searching table %s. Please try again and if the error persists contact the platform team. Details: %s",
                    tableFullName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    public Either<FailedOperation, Void> dropTableIfExists(String catalogName, String schemaName, String tableName) {

        String tableFullName = retrieveTableFullName(catalogName, schemaName, tableName);

        try {
            Either<FailedOperation, Boolean> eitherTableExists =
                    checkTableExistence(catalogName, schemaName, tableName);
            if (eitherTableExists.isLeft()) return left(eitherTableExists.getLeft());

            boolean tableExists = eitherTableExists.get();

            if (!tableExists) {
                logger.info(String.format("Drop table skipped. Table '%s' does not exist.", tableFullName));
            } else {
                logger.info(String.format("Dropping table '%s'.", tableFullName));
                workspaceClient.tables().delete(tableFullName);
                logger.info(String.format("Table '%s' correctly dropped.", tableFullName));
            }
            return (right(null));

        } catch (Exception e) {

            String errorMessage = String.format(
                    "An error occurred while dropping table '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    tableFullName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    public Either<FailedOperation, List<String>> retrieveTableColumnsNames(
            String catalogName, String schemaName, String tableName) {

        String tableFullName = retrieveTableFullName(catalogName, schemaName, tableName);

        try {
            logger.info(String.format(
                    "Retrieving columns for table '%s' in workspace %s",
                    tableFullName, databricksWorkspaceInfo.getName()));
            List<String> colNames = new ArrayList<>();

            TableInfo tableInfo = workspaceClient.tables().get(tableFullName);

            Collection<ColumnInfo> columns = tableInfo.getColumns();

            for (ColumnInfo column : columns) {
                colNames.add(column.getName());
            }

            return right(colNames);

        } catch (Exception e) {

            String errorMessage = String.format(
                    "An error occurred while retrieving columns for table '%s' in workspace %s. Please try again and if the error persists contact the platform team. Details: %s",
                    tableFullName, databricksWorkspaceInfo.getName(), e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private String retrieveTableFullName(String catalogName, String schemaName, String tableName) {
        return catalogName + "." + schemaName + "." + tableName;
    }
}
