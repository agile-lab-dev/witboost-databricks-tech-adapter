package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.TableExistsResponse;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TablesAPI;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vavr.control.Either;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.bean.params.ApiClientConfigParams;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
@ExtendWith(MockitoExtension.class)
public class OutputPortValidationTest {

    @Autowired
    private MiscConfig miscConfig;

    @Mock
    private WorkspaceHandler workspaceHandler;

    @Mock
    private Function<ApiClientConfigParams, ApiClient> apiClientFactory;

    private OutputPortValidation outputPortValidation;

    private OutputPort<DatabricksOutputPortSpecific> outputPort;

    @BeforeEach
    public void setUp() throws Exception {

        outputPort = new OutputPort<>();
    }

    @Test
    public void testValidate_success() {
        DatabricksOutputPortSpecific databricksOutputPortSpecific = prepareDatabricksOPSpecific();
        outputPort.setSpecific(databricksOutputPortSpecific);

        JsonNode dataContract = prepareDataContract();
        outputPort.setDataContract(dataContract);

        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);

        WorkspaceHandler workspaceHandlerMock = mock(WorkspaceHandler.class);

        when(workspaceHandlerMock.getWorkspaceInfo(any(String.class)))
                .thenReturn(right(Optional.of(mock(DatabricksWorkspaceInfo.class))));
        when(workspaceHandlerMock.getWorkspaceClient(any())).thenReturn(right(workspaceClientMock));

        when(workspaceClientMock.tables()).thenReturn(mock(TablesAPI.class));

        TableExistsResponse tableExistsResponseMock = mock(TableExistsResponse.class);
        when(tableExistsResponseMock.getTableExists()).thenReturn(true);

        when(workspaceClientMock.tables().exists("catalog.schema.table_1")).thenReturn(tableExistsResponseMock);

        TableInfo tableInfoMock = mock(TableInfo.class);

        when(workspaceClientMock.tables().get("catalog.schema.table_1")).thenReturn(tableInfoMock);

        Collection<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(new ColumnInfo().setName("col_1"));
        columnInfos.add(new ColumnInfo().setName("col_2"));

        when(tableInfoMock.getColumns()).thenReturn(columnInfos);

        outputPortValidation = new OutputPortValidation(miscConfig, workspaceHandlerMock, apiClientFactory);

        var responseActual = outputPortValidation.validate(outputPort, "development");

        Either<FailedOperation, Void> responseExpected = right(null);
        assertEquals(responseExpected, responseActual);
    }

    @Test
    public void testValidateFailForTableDoesNotExistDevelopEnv() {
        DatabricksOutputPortSpecific databricksOutputPortSpecific = prepareDatabricksOPSpecific();
        outputPort.setSpecific(databricksOutputPortSpecific);

        JsonNode dataContract = prepareDataContract();
        outputPort.setDataContract(dataContract);
        outputPort.setName("op_name");

        WorkspaceHandler workspaceHandlerMock = mock(WorkspaceHandler.class);
        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);

        when(workspaceHandlerMock.getWorkspaceInfo(any(String.class)))
                .thenReturn(right(Optional.of(mock(DatabricksWorkspaceInfo.class))));
        when(workspaceHandlerMock.getWorkspaceClient(any())).thenReturn(right(workspaceClientMock));

        TableExistsResponse tableExistsResponse = mock(TableExistsResponse.class);
        when(tableExistsResponse.getTableExists()).thenReturn(false);

        when(workspaceClientMock.tables()).thenReturn(mock(TablesAPI.class));

        when(workspaceClientMock.tables().exists("catalog.schema.table_1")).thenReturn(tableExistsResponse);

        outputPortValidation = new OutputPortValidation(miscConfig, workspaceHandlerMock, apiClientFactory);
        var responseActual = outputPortValidation.validate(outputPort, "development");

        String errorMessage =
                "The table 'catalog.schema.table_1', provided in Output Port op_name, does not exist. Be sure that the table exists by either running the DLT Workload that creates it or creating the table manually.";
        Either<FailedOperation, Object> responseExpected =
                left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        assertEquals(responseExpected, responseActual);
    }

    @Test
    public void testValidateFailForTableDoesNotExistProdEnv() {
        DatabricksOutputPortSpecific databricksOutputPortSpecific = prepareDatabricksOPSpecific();
        outputPort.setSpecific(databricksOutputPortSpecific);

        JsonNode dataContract = prepareDataContract();
        outputPort.setDataContract(dataContract);

        outputPort.setName("op_name");

        WorkspaceHandler workspaceHandlerMock = mock(WorkspaceHandler.class);
        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);

        when(workspaceHandlerMock.getWorkspaceInfo(any(String.class)))
                .thenReturn(right(Optional.of(mock(DatabricksWorkspaceInfo.class))));
        when(workspaceHandlerMock.getWorkspaceClient(any())).thenReturn(right(workspaceClientMock));

        TableExistsResponse tableExistsResponse = mock(TableExistsResponse.class);
        when(tableExistsResponse.getTableExists()).thenReturn(false);

        when(workspaceClientMock.tables()).thenReturn(mock(TablesAPI.class));
        when(workspaceClientMock.tables().exists("catalog.schema.table_1")).thenReturn(tableExistsResponse);

        outputPortValidation = new OutputPortValidation(miscConfig, workspaceHandlerMock, apiClientFactory);
        var responseActual = outputPortValidation.validate(outputPort, "prod");

        String errorMessage =
                "The table 'catalog.schema.table_1', provided in Output Port op_name, does not exist. Be sure that the DLT Workload is being deployed correctly and that the table name is correct.";
        Either<FailedOperation, Object> responseExpected =
                left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        assertEquals(responseExpected, responseActual);
    }

    @Test
    public void testValidateFailForDataContractFailure() {

        DatabricksOutputPortSpecific databricksOutputPortSpecific = prepareDatabricksOPSpecific();
        outputPort.setSpecific(databricksOutputPortSpecific);

        JsonNode dataContract = prepareDataContract();
        outputPort.setDataContract(dataContract);

        outputPort.setName("op_name");

        WorkspaceHandler workspaceHandlerMock = mock(WorkspaceHandler.class);
        WorkspaceClient workspaceClientMock = mock(WorkspaceClient.class);

        when(workspaceHandlerMock.getWorkspaceInfo(any(String.class)))
                .thenReturn(right(Optional.of(mock(DatabricksWorkspaceInfo.class))));
        when(workspaceHandlerMock.getWorkspaceClient(any())).thenReturn(right(workspaceClientMock));

        TableExistsResponse tableExistsResponse = mock(TableExistsResponse.class);
        when(tableExistsResponse.getTableExists()).thenReturn(true);

        when(workspaceClientMock.tables()).thenReturn(mock(TablesAPI.class));

        when(workspaceClientMock.tables().exists("catalog.schema.table_1")).thenReturn(tableExistsResponse);

        TableInfo tableInfo = mock(TableInfo.class);

        when(workspaceClientMock.tables().get("catalog.schema.table_1")).thenReturn(tableInfo);

        Collection<ColumnInfo> columnInfos = new ArrayList<>();
        columnInfos.add(new ColumnInfo().setName("col_1")); // Just col_1 in the original table. Test must fail

        when(tableInfo.getColumns()).thenReturn(columnInfos);

        outputPortValidation = new OutputPortValidation(miscConfig, workspaceHandlerMock, apiClientFactory);
        var responseActual = outputPortValidation.validate(outputPort, "development");

        String errorMessage =
                "Check for Output Port op_name: the column 'col_2' cannot be found in the table 'catalog.schema.table_1'.";
        Either<FailedOperation, Object> responseExpected =
                left(new FailedOperation(Collections.singletonList(new Problem(errorMessage))));
        assertEquals(responseExpected, responseActual);
    }

    private DatabricksOutputPortSpecific prepareDatabricksOPSpecific() {

        DatabricksOutputPortSpecific databricksOutputPortSpecific = new DatabricksOutputPortSpecific();
        databricksOutputPortSpecific.setWorkspace("ws");
        databricksOutputPortSpecific.setTableName("table_1");
        databricksOutputPortSpecific.setCatalogName("catalog");
        databricksOutputPortSpecific.setSchemaName("schema");

        return databricksOutputPortSpecific;
    }

    private JsonNode prepareDataContract() {
        ObjectMapper objectMapper = new ObjectMapper();
        ObjectNode schemaNode = objectMapper.createObjectNode();

        ObjectNode col1Node = objectMapper.createObjectNode();
        col1Node.put("name", "col_1");
        col1Node.put("dataType", "TEXT");

        ObjectNode col2Node = objectMapper.createObjectNode();
        col2Node.put("name", "col_2");
        col2Node.put("dataType", "TEXT");

        ArrayNode colsNode = objectMapper.createArrayNode();
        colsNode.add(col1Node);
        colsNode.add(col2Node);

        return schemaNode.set("schema", colsNode);
    }
}
