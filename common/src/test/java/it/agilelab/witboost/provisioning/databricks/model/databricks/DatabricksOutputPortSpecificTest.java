package it.agilelab.witboost.provisioning.databricks.model.databricks;

import static org.junit.jupiter.api.Assertions.*;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatabricksOutputPortSpecificTest {

    private Validator validator;
    private DatabricksOutputPortSpecific outputPortSpecific;

    @BeforeEach
    public void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();

        outputPortSpecific = new DatabricksOutputPortSpecific();
    }

    @Test
    public void testTableNameNotNull() {
        outputPortSpecific.setTableName(null);
        outputPortSpecific.setSchemaName("fake_schema");
        outputPortSpecific.setCatalogName("fake_catalog");
        outputPortSpecific.setWorkspaceHost("fake_ws");
        Set<ConstraintViolation<DatabricksOutputPortSpecific>> violations = validator.validate(outputPortSpecific);
        assertEquals(1, violations.size());

        assertEquals("tableName", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testTableNameValid() {
        outputPortSpecific.setTableName("sales");
        outputPortSpecific.setSchemaName("fake_schema");
        outputPortSpecific.setCatalogName("fake_catalog");
        outputPortSpecific.setWorkspaceHost("fake_ws");
        Set<ConstraintViolation<DatabricksOutputPortSpecific>> violations = validator.validate(outputPortSpecific);
        assertEquals(0, violations.size());
    }

    @Test
    public void testSettersAndGetters() {

        outputPortSpecific = new DatabricksOutputPortSpecific();
        outputPortSpecific.setWorkspaceHost("ws");
        outputPortSpecific.setCatalogName("c");
        outputPortSpecific.setSchemaName("s");
        outputPortSpecific.setTableName("t");

        assertEquals("ws", outputPortSpecific.getWorkspaceHost());
        assertEquals("c", outputPortSpecific.getCatalogName());
        assertEquals("s", outputPortSpecific.getSchemaName());
        assertEquals("t", outputPortSpecific.getTableName());
    }
}
