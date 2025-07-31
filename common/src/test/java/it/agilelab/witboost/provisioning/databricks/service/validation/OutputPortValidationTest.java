package it.agilelab.witboost.provisioning.databricks.service.validation;

import static io.vavr.control.Either.left;
import static io.vavr.control.Either.right;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.error.platform.NotFound;
import com.databricks.sdk.service.catalog.*;
import com.witboost.provisioning.model.Column;
import com.witboost.provisioning.model.DataContract;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.bean.ApiClientConfig;
import it.agilelab.witboost.provisioning.databricks.common.FailedOperation;
import it.agilelab.witboost.provisioning.databricks.common.Problem;
import it.agilelab.witboost.provisioning.databricks.config.MiscConfig;
import it.agilelab.witboost.provisioning.databricks.model.OutputPort;
import it.agilelab.witboost.provisioning.databricks.model.databricks.DatabricksWorkspaceInfo;
import it.agilelab.witboost.provisioning.databricks.model.databricks.outputport.DatabricksOutputPortSpecific;
import it.agilelab.witboost.provisioning.databricks.service.WorkspaceHandler;
import java.util.*;
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
    private WorkspaceClient workspaceClient;

    @Mock
    private Function<ApiClientConfig.ApiClientConfigParams, ApiClient> apiClientFactory;

    private OutputPortValidation outputPortValidation;
    private OutputPort<DatabricksOutputPortSpecific> outputPort;
    private static final String TABLE_FULL_NAME = "catalog.schema.table_1";

    @BeforeEach
    public void setUp() {
        setupCommonMocks();
        setupOutputPort();
        outputPortValidation = new OutputPortValidation(miscConfig, workspaceHandler);
    }

    private void setupCommonMocks() {
        when(workspaceClient.metastores()).thenReturn(mock(MetastoresAPI.class));
        lenient()
                .when(workspaceClient.metastores().current())
                .thenReturn(new MetastoreAssignment().setMetastoreId("metastoreID"));
        lenient().when(workspaceHandler.getWorkspaceClient(any())).thenReturn(right(workspaceClient));
        lenient().when(workspaceClient.tables()).thenReturn(mock(TablesAPI.class));
        lenient()
                .when(workspaceHandler.getWorkspaceInfo(any(String.class)))
                .thenReturn(right(Optional.of(mock(DatabricksWorkspaceInfo.class))));
    }

    private void setupOutputPort() {
        outputPort = new OutputPort<>();
        outputPort.setSpecific(prepareDatabricksOPSpecific());
        outputPort.setDataContract(prepareDataContract());
        outputPort.setName("op_name");
    }

    private void mockTableExists(boolean exists) {
        TableExistsResponse tableExistsResponse = mock(TableExistsResponse.class);
        when(tableExistsResponse.getTableExists()).thenReturn(exists);
        when(workspaceClient.tables().exists(TABLE_FULL_NAME)).thenReturn(tableExistsResponse);
    }

    private void mockTableColumns(Collection<ColumnInfo> columns) {
        TableInfo tableInfo = mock(TableInfo.class);
        when(workspaceClient.tables().get(TABLE_FULL_NAME)).thenReturn(tableInfo);
        when(tableInfo.getColumns()).thenReturn(columns);
    }

    @Test
    public void testValidate_success() {
        mockTableExists(true);

        Collection<ColumnInfo> columnInfos =
                Arrays.asList(new ColumnInfo().setName("col_1"), new ColumnInfo().setName("col_2"));
        mockTableColumns(columnInfos);

        var responseActual = outputPortValidation.validate(outputPort, "development");
        assertEquals(right(null), responseActual);
    }

    @Test
    public void testValidateFailForTableDoesNotExistDevelopEnv() {
        mockTableExists(false);

        var responseActual = outputPortValidation.validate(outputPort, "development");

        String errorMessage = "The table '" + TABLE_FULL_NAME + "', provided in Output Port op_name, "
                + "does not exist. Be sure that the table exists by either running the DLT Workload "
                + "that creates it or creating the table manually.";
        assertEquals(left(new FailedOperation(List.of(new Problem(errorMessage)))), responseActual);
    }

    @Test
    public void testValidateFailForTableDoesNotExistProdEnv() {
        mockTableExists(false);

        var responseActual = outputPortValidation.validate(outputPort, "prod");

        String errorMessage = "The table '" + TABLE_FULL_NAME + "', provided in Output Port op_name, "
                + "does not exist. Be sure that the DLT Workload that creates it is being deployed "
                + "correctly and that the table name is correct.";
        assertEquals(left(new FailedOperation(List.of(new Problem(errorMessage)))), responseActual);
    }

    @Test
    public void testValidateFailForDataContractFailure() {
        mockTableExists(true);
        mockTableColumns(List.of(new ColumnInfo().setName("col_1")));

        var responseActual = outputPortValidation.validate(outputPort, "development");

        String errorMessage = "Check for Output Port op_name: the column 'col_2' cannot be found in the table '"
                + TABLE_FULL_NAME + "'.";
        assertEquals(left(new FailedOperation(List.of(new Problem(errorMessage)))), responseActual);
    }

    @Test
    public void testValidateWorkspaceClientError() {
        when(workspaceHandler.getWorkspaceClient(any()))
                .thenReturn(left(new FailedOperation(List.of(new Problem("This is an error")))));

        var responseActual = outputPortValidation.validate(outputPort, "development");

        assert (responseActual.isLeft());
        assert (responseActual.getLeft().problems().get(0).getMessage().contains("This is an error"));
    }

    @Test
    public void testValidateMetastoreError() {
        when(workspaceClient.metastores())
                .thenThrow(new NotFound("NOT_FOUND", "Metastore error", Collections.emptyList()));

        var responseActual = outputPortValidation.validate(outputPort, "development");

        String errorMessage =
                "An error occurred while retrieving current metastore for workspace ws. Please try again and if the error persists contact the platform team. Details: Metastore error";

        assert (responseActual.isLeft());
        assert (responseActual.getLeft().problems().get(0).getMessage().contains(errorMessage));
    }

    @Test
    public void testValidateMetastoreDoesNotExistManagedWorkspace() {
        DatabricksWorkspaceInfo workspaceInfo = mock(DatabricksWorkspaceInfo.class);
        when(workspaceInfo.isManaged()).thenReturn(true);
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceClient.metastores())
                .thenThrow(new NotFound("METASTORE_DOES_NOT_EXIST", "Metastore error", Collections.emptyList()));

        var responseActual = outputPortValidation.validate(outputPort, "development");

        assertEquals(right(null), responseActual);
    }

    @Test
    public void testValidateMetastoreDoesNotExistUnmanagedWorkspace() {
        DatabricksWorkspaceInfo workspaceInfo = mock(DatabricksWorkspaceInfo.class);
        when(workspaceInfo.isManaged()).thenReturn(false);
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(right(Optional.of(workspaceInfo)));

        when(workspaceClient.metastores())
                .thenThrow(new NotFound("METASTORE_DOES_NOT_EXIST", "Metastore error", Collections.emptyList()));

        var responseActual = outputPortValidation.validate(outputPort, "development");

        String errorMessage =
                "Validation of Output Port op_name (id: null) failed. No metastore assigned for the current workspace";

        assert (responseActual.isLeft());
        assert (responseActual.getLeft().problems().get(0).getMessage().contains(errorMessage));
    }

    @Test
    public void testValidateWorkspaceDoesNotExist() {
        when(workspaceHandler.getWorkspaceInfo(anyString())).thenReturn(right(Optional.empty()));

        var responseActual = outputPortValidation.validate(outputPort, "development");

        assertEquals(right(null), responseActual);
    }

    @Test
    public void testValidateWorkspaceInfoError() {
        when(workspaceHandler.getWorkspaceInfo(anyString()))
                .thenReturn(left(new FailedOperation(List.of(new Problem("This is an error")))));

        var responseActual = outputPortValidation.validate(outputPort, "development");

        assertEquals(left(new FailedOperation(List.of(new Problem("This is an error")))), responseActual);
    }

    private DatabricksOutputPortSpecific prepareDatabricksOPSpecific() {
        DatabricksOutputPortSpecific databricksOutputPortSpecific = new DatabricksOutputPortSpecific();
        databricksOutputPortSpecific.setWorkspace("ws");
        databricksOutputPortSpecific.setTableName("table_1");
        databricksOutputPortSpecific.setCatalogName("catalog");
        databricksOutputPortSpecific.setSchemaName("schema");
        return databricksOutputPortSpecific;
    }

    private DataContract prepareDataContract() {
        Column dataColumn1 = new Column();
        dataColumn1.setName("col_1");
        dataColumn1.setDataType("TEXT");
        Column dataColumn2 = new Column();
        dataColumn2.setName("col_2");
        dataColumn2.setDataType("TEXT");

        DataContract dataContract = new DataContract();
        dataContract.setSchema(List.of(dataColumn1, dataColumn2));
        return dataContract;
    }
}
