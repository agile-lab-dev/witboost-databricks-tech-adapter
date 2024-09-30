package it.agilelab.witboost.provisioning.databricks.service.provision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.catalog.*;
import com.databricks.sdk.service.sql.*;
import com.fasterxml.jackson.databind.JsonNode;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.bean.params.ApiClientConfigParams;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.*;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.ProvisionRequest;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.object.View;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.openapi.model.UpdateAclRequest;
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
    private final DatabricksPermissionsConfig databricksPermissionsConfig;

    @Autowired
    public OutputPortHandler(
            AzureAuthConfig azureAuthConfig,
            GitCredentialsConfig gitCredentialsConfig,
            Function<ApiClientConfigParams, ApiClient> apiClientFactory,
            AzurePermissionsManager azurePermissionsManager,
            MiscConfig miscConfig,
            AzureMapper azureMapper,
            DatabricksAuthConfig databricksAuthConfig,
            DatabricksPermissionsConfig databricksPermissionsConfig) {
        this.azureAuthConfig = azureAuthConfig;
        this.gitCredentialsConfig = gitCredentialsConfig;
        this.apiClientFactory = apiClientFactory;
        this.azurePermissionsManager = azurePermissionsManager;
        this.miscConfig = miscConfig;
        this.azureMapper = azureMapper;
        this.databricksAuthConfig = databricksAuthConfig;
        this.databricksPermissionsConfig = databricksPermissionsConfig;
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
            String viewFullNameOP = String.format("`%s`.`%s`.`%s`", catalogNameOP, schemaNameOP, viewNameOP);
            String tableFullName = String.format(
                    "`%s`.`%s`.`%s`",
                    databricksOutputPortSpecific.getCatalogName(),
                    databricksOutputPortSpecific.getSchemaName(),
                    databricksOutputPortSpecific.getTableName());

            Either<FailedOperation, Void> eitherAttachedMetastore =
                    unityCatalogManager.attachMetastore(databricksOutputPortSpecific.getMetastore());

            if (eitherAttachedMetastore.isLeft()) return left(eitherAttachedMetastore.getLeft());

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

            String columnsListString = createColumnsListForSelectStatement(provisionRequest);

            // Create OP
            Either<FailedOperation, String> eitherExecutedStatementCreateOutputPort = executeStatementCreateOutputPort(
                    apiClient,
                    catalogNameOP,
                    schemaNameOP,
                    viewNameOP,
                    tableFullName,
                    columnsListString,
                    sqlWarehouseId.get());

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

            // TODO: This is a temporary solution. Remove or update this logic in the future.
            if (!devGroup.startsWith("group:")) devGroup = "group:" + devGroup;

            DatabricksMapper databricksMapper = new DatabricksMapper();

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

            // Retrieve environment
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

    private String createColumnsListForSelectStatement(ProvisionRequest provisionRequest) {
        OutputPort component = (OutputPort) provisionRequest.component();

        JsonNode viewSchemaNode = component.getDataContract().get("schema");

        if (viewSchemaNode.isEmpty()) {
            return "*";
        } else {
            List<String> columnsList = new ArrayList<>();

            for (JsonNode viewColumn : viewSchemaNode) {
                String viewColumnName = viewColumn.get("name").asText();
                columnsList.add(viewColumnName);
            }

            return String.join(",", columnsList);
        }
    }

    private Either<FailedOperation, String> executeStatementCreateOutputPort(
            ApiClient apiClient,
            String catalogNameOP,
            String schemaNameOP,
            String viewNameOP,
            String tableFullName,
            String columnsList,
            String sqlWarehouseId) {

        try {
            String queryToCreateOP = String.format(
                    "CREATE OR REPLACE VIEW `%s` AS SELECT %s FROM %s;", viewNameOP, columnsList, tableFullName);

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
        DatabricksMapper databricksMapper = new DatabricksMapper();

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
