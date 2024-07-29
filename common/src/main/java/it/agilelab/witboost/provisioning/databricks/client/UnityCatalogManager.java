package it.agilelab.witboost.provisioning.databricks.client;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.CatalogInfo;
import com.databricks.sdk.service.catalog.CreateMetastoreAssignment;
import com.databricks.sdk.service.catalog.ListCatalogsRequest;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import java.util.Collections;
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

    public Either<FailedOperation, Void> attachMetastore(String metastoreName, String catalogName) {

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

            Either<FailedOperation, Boolean> eitherCatalogExists = checkCatalogExistence(catalogName);
            if (eitherCatalogExists.isLeft()) return left(eitherCatalogExists.getLeft());
            boolean catalogExists = eitherCatalogExists.get();

            if (!catalogExists) {
                var createCatalog = createCatalog(catalogName);
                if (createCatalog.isLeft()) return left(createCatalog.getLeft());
            }
            return right(null);

        } catch (Exception e) {
            String error = String.format(
                    "Error linking the workspace %s to the metastore %s",
                    databricksWorkspaceInfo.getName(), metastoreName);
            logger.error(error, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(error, e))));
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
}
