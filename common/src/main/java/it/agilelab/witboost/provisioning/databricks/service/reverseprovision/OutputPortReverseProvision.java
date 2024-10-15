package it.agilelab.witboost.provisioning.databricks.service.reverseprovision;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static it.agilelab.witboost.provisioning.databricks.service.reverseprovision.ReverseProvisionStatusHandler.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.service.catalog.ColumnTypeName;
import com.databricks.sdk.service.catalog.TableInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.witboost.provisioning.model.Column;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.client.UnityCatalogManager;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest.*;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningRequest;
import it.agilelab.witboost.provisioning.databricks.openapi.model.ReverseProvisioningStatus;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import it.agilelab.witboost.provisioning.databricks.service.validation.OutputPortValidation;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class OutputPortReverseProvision {

    private static final Logger logger = LoggerFactory.getLogger(OutputPortValidation.class);
    private final WorkspaceHandler workspaceHandler;
    private final String SCHEMA_AND_DETAILS = "SCHEMA_AND_DETAILS";
    private final String VIEW = "VIEW";

    public OutputPortReverseProvision(WorkspaceHandler workspaceHandler) {
        this.workspaceHandler = workspaceHandler;
    }

    public ReverseProvisioningStatus reverseProvision(ReverseProvisioningRequest reverseProvisioningRequest) {

        ObjectMapper mapper = new ObjectMapper().registerModule(new Jdk8Module());

        // Parsing params
        Object paramsObj = reverseProvisioningRequest.getParams();
        Params params = mapper.convertValue(paramsObj, Params.class);

        String catalogName = params.getCatalogName();
        String schemaName = params.getSchemaName();
        String tableName = params.getTableName();
        String reverseProvisioningOption = params.getReverseProvisioningOption();

        logger.info(String.format("Start Output Port reverse provisioning with following parameters: %s", params));

        String tableFullName = catalogName + "." + schemaName + "." + tableName;

        String workspaceName =
                params.getEnvironmentSpecificConfig().getSpecific().getWorkspace();

        logger.info("workspace for reverse provisioning: " + workspaceName);

        Either<FailedOperation, Optional<DatabricksWorkspaceInfo>> eitherWorkspaceExists =
                workspaceHandler.getWorkspaceInfo(workspaceName);
        if (eitherWorkspaceExists.isLeft()) {
            return handleReverseProvisioningStatusFailed(
                    String.format("Error while retrieving workspace info of %s", workspaceName));
        }

        Optional<DatabricksWorkspaceInfo> databricksWorkspaceInfoOptional = eitherWorkspaceExists.get();
        if (databricksWorkspaceInfoOptional.isEmpty()) {
            return handleReverseProvisioningStatusFailed(
                    String.format("Validation failed. Workspace '%s' not found.", workspaceName));
        }

        DatabricksWorkspaceInfo databricksWorkspaceInfo = databricksWorkspaceInfoOptional.get();

        Either<FailedOperation, WorkspaceClient> eitherWorkspaceClient =
                workspaceHandler.getWorkspaceClient(databricksWorkspaceInfo);
        if (eitherWorkspaceClient.isLeft()) {
            return handleReverseProvisioningStatusFailed(
                    String.format("Error while retrieving workspace client for workspace %s.", workspaceName));
        }

        WorkspaceClient workspaceClient = eitherWorkspaceClient.get();

        // Start validation of Reverse Provisioning request
        Either<ReverseProvisioningStatus, Void> eitherValidRequest = validateProvisionRequest(
                workspaceClient,
                databricksWorkspaceInfo,
                catalogName,
                schemaName,
                tableName,
                reverseProvisioningOption);

        if (eitherValidRequest.isLeft()) {
            return eitherValidRequest.getLeft();
        }

        // Validation ended. Now let's start the reverse provisioning

        logger.info("Start retrieving schema.");
        Either<FailedOperation, ArrayList<Column>> eitherColumnsList =
                retrieveColumnsList(workspaceClient, tableFullName);

        ArrayList<Column> columnsList;

        if (eitherColumnsList.isRight()) {
            columnsList = eitherColumnsList.get();
        } else {

            String result = eitherColumnsList.getLeft().problems().stream()
                    .map(Problem::description)
                    .collect(Collectors.joining(", "));

            return handleReverseProvisioningStatusFailed(result);
        }

        HashMap<Object, Object> updates = new HashMap<>();
        updates.put("spec.mesh.dataContract.schema", columnsList);
        updates.put("witboost.parameters.schemaDefinition", columnsList);

        if (reverseProvisioningOption.equalsIgnoreCase(SCHEMA_AND_DETAILS)) {
            updates.put("spec.mesh.specific.catalogName", catalogName);
            updates.put("spec.mesh.specific.schemaName", schemaName);
            updates.put("spec.mesh.specific.tableName", tableName);
            updates.put("witboost.parameters.catalogName", catalogName);
            updates.put("witboost.parameters.schemaName", schemaName);
            updates.put("witboost.parameters.tableName", tableName);
        }

        return handleReverseProvisioningStatusCompleted("Reverse provisioning successfully completed.", updates);
    }

    private Either<FailedOperation, ArrayList<Column>> retrieveColumnsList(
            WorkspaceClient workspaceClient, String tableFullName) {

        try {

            TableInfo tableInfo = workspaceClient.tables().get(tableFullName);

            Integer maxLengthString = 65535;

            ArrayList<Column> columnsList = new ArrayList<>();

            List<Problem> problems = new ArrayList<>();

            List<String> primaryKeyCols = Optional.ofNullable(tableInfo.getTableConstraints()).stream()
                    .flatMap(Collection::stream)
                    .filter(c -> c.getPrimaryKeyConstraint() != null)
                    .flatMap(c -> c.getPrimaryKeyConstraint().getChildColumns().stream())
                    .toList();

            String pkInfoMessage = !(primaryKeyCols.isEmpty())
                    ? String.format("Primary key cols of the table: '%s'", String.join(",", primaryKeyCols))
                    : "No primary key cols in table";
            logger.info(pkInfoMessage);

            tableInfo.getColumns().forEach(col -> {
                ColumnTypeName typeName = col.getTypeName();

                // Mapping Databricks data type to Open Metadata Type
                Either<FailedOperation, String> eitherOpenMetaDataType = mapDatabricksToOpenMetadata(
                        getDataTypesMap(), col.getTypeName().name());

                if (eitherOpenMetaDataType.isLeft()) {
                    problems.addAll(eitherOpenMetaDataType.getLeft().problems());
                } else {
                    String openMetaDataType = eitherOpenMetaDataType.get();

                    // Note: currently getter for typeScale and typePrecision always return 0.
                    Column column = new Column();
                    column.setName(col.getName());
                    column.setDataType(openMetaDataType);
                    column.setDescription(col.getTypeText());
                    switch (typeName) {
                        case DECIMAL -> {
                            column.setPrecision(
                                    Optional.of(col.getTypePrecision().intValue()));
                            column.setScale(Optional.of(col.getTypeScale().intValue()));
                        }
                        case STRING -> column.setDataLength(Optional.of(maxLengthString));
                    }
                    ;

                    // In case of PRIMARY_KEY constraint an eventual NOT_NULL constraint will be ignored as it is
                    // implicit in PRIMARY_KEY constraint.
                    if (primaryKeyCols.contains(column.getName())) {
                        column.setConstraint(Optional.of("PRIMARY_KEY"));
                    } else {
                        if (!(col.getNullable())) {
                            column.setConstraint(Optional.of("NOT_NULL"));
                        }
                    }
                    columnsList.add(column);
                }
            });

            if (!problems.isEmpty()) {
                return Either.left(new FailedOperation(problems));
            }

            logger.info("Column list correctly retrieved");

            return right(columnsList);
        } catch (Exception e) {
            String errorMessage = String.format(
                    "An error occurred while retrieving column list from Databricks table '%s'. Please try again and if the error persists contact the platform team. Details: %s",
                    tableFullName, e.getMessage());
            logger.error(errorMessage, e);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private Map<Pattern, String> getDataTypesMap() {
        Map<Pattern, String> typeMap = new LinkedHashMap<>();

        typeMap.put(Pattern.compile("^DECIMAL.*"), "DECIMAL");
        typeMap.put(Pattern.compile("^ARRAY.*"), "ARRAY");
        typeMap.put(Pattern.compile("^MAP.*"), "MAP");
        typeMap.put(Pattern.compile("^STRUCT.*"), "STRUCT");
        typeMap.put(Pattern.compile("^VARCHAR.*"), "VARCHAR");
        typeMap.put(Pattern.compile("^CHAR.*"), "CHAR");

        typeMap.put(Pattern.compile("^TIMESTAMP_NTZ$"), "TIMESTAMPZ");

        typeMap.put(Pattern.compile("^BIGINT$"), "BIGINT");
        typeMap.put(Pattern.compile("^BINARY$"), "BINARY");
        typeMap.put(Pattern.compile("^BOOLEAN$"), "BOOLEAN");
        typeMap.put(Pattern.compile("^DATE$"), "DATE");
        typeMap.put(Pattern.compile("^DOUBLE$"), "DOUBLE");
        typeMap.put(Pattern.compile("^FLOAT$"), "FLOAT");
        typeMap.put(Pattern.compile("^INT$"), "INT");
        typeMap.put(Pattern.compile("^INTERVAL$"), "INTERVAL");
        typeMap.put(Pattern.compile("^SMALLINT$"), "SMALLINT");
        typeMap.put(Pattern.compile("^STRING$"), "STRING");
        typeMap.put(Pattern.compile("^TIMESTAMP$"), "TIMESTAMP");
        typeMap.put(Pattern.compile("^TINYINT$"), "TINYINT");
        typeMap.put(Pattern.compile("^VARIANT$"), "VARIANT");

        return typeMap;
    }

    private Either<FailedOperation, String> mapDatabricksToOpenMetadata(
            Map<Pattern, String> dataTypesMap, String databricksType) {
        try {
            Set<Map.Entry<Pattern, String>> entrySet = dataTypesMap.entrySet();
            for (Map.Entry<Pattern, String> entry : entrySet) {
                Pattern pattern = entry.getKey();
                if (pattern.matcher(databricksType).matches()) {
                    String openMetadataType = entry.getValue();
                    logger.info(String.format(
                            "Mapped '%s' Databricks data type into '%s' Open Metadata data type",
                            databricksType, openMetadataType));
                    return right(openMetadataType);
                }
            }
            String errorMessage = String.format("Not able to convert data type '%s' in Open Metadata", databricksType);
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        } catch (Exception e) {
            String errorMessage = String.format(
                    "Not able to convert data type '%s' in Open Metadata. Details: %s", databricksType, e.getMessage());
            logger.error(errorMessage);
            return left(new FailedOperation(Collections.singletonList(new Problem(errorMessage, e))));
        }
    }

    private Either<ReverseProvisioningStatus, Void> validateProvisionRequest(
            WorkspaceClient workspaceClient,
            DatabricksWorkspaceInfo databricksWorkspaceInfo,
            String catalogName,
            String schemaName,
            String tableName,
            String reverseProvisioningOption) {
        var unityCatalogManager = new UnityCatalogManager(workspaceClient, databricksWorkspaceInfo);

        Either<FailedOperation, Boolean> eitherTableExists =
                unityCatalogManager.checkTableExistence(catalogName, schemaName, tableName);

        if (eitherTableExists.isLeft()) {
            return Either.left(handleReverseProvisioningStatusFailed("Error while checking table existence."));
        }

        Boolean tableExists = eitherTableExists.get();

        if (!tableExists) {
            return Either.left(handleReverseProvisioningStatusFailed(String.format(
                    "The table '%s.%s.%s', provided in the Reverse Provisioning, request does not exist. ",
                    catalogName, schemaName, tableName)));
        }
        logger.info(String.format(
                "The table '%s.%s.%s', provided in the Reverse Provisioning request, exists. ",
                catalogName, schemaName, tableName));

        TableInfo tableInfo = workspaceClient.tables().get(catalogName + "." + schemaName + "." + tableName);

        // Check, in case of reverseProvisioningOption = SCHEMA_AND_DETAILS, that the source table is not a VIEW
        if (reverseProvisioningOption.equalsIgnoreCase(SCHEMA_AND_DETAILS)) {
            if (tableInfo.getTableType().toString().equalsIgnoreCase(VIEW)) {
                return Either.left(handleReverseProvisioningStatusFailed(
                        "It's not possible to inherit table details from a VIEW. "));
            }
        }

        return right(null);
    }
}
