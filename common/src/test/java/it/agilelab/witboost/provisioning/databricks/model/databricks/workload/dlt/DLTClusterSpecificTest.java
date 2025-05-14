package it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.sdk.service.pipelines.PipelineClusterAutoscaleMode;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.Locale;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.context.i18n.LocaleContextHolder;

@SpringBootTest
@Import(TestConfig.class)
public class DLTClusterSpecificTest {

    private static Validator validator;

    @BeforeAll
    public static void setUp() {
        LocaleContextHolder.setLocale(Locale.ENGLISH);
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    private DLTClusterSpecific createValidCluster() {
        DLTClusterSpecific cluster = new DLTClusterSpecific();
        cluster.setMode(PipelineClusterAutoscaleMode.LEGACY);
        cluster.setMinWorkers(1L);
        cluster.setMaxWorkers(10L);
        cluster.setNumWorkers(null);
        cluster.setWorkerType("Standard_DS3_v2");
        cluster.setDriverType("Standard_DS3_v2");
        cluster.setPolicyId("policyId");
        return cluster;
    }

    @Test
    public void testValidCluster() {
        DLTClusterSpecific cluster = createValidCluster();
        Set<ConstraintViolation<DLTClusterSpecific>> violations = validator.validate(cluster);
        assertTrue(violations.isEmpty());
    }

    @Test
    public void testInvalidCluster() {
        DLTClusterSpecific cluster = createValidCluster();
        cluster.setMode(null); // Invalid mode
        Set<ConstraintViolation<DLTClusterSpecific>> violations = validator.validate(cluster);
        assertEquals(1, violations.size());
        assertEquals(
                "Invalid configuration for DLTClusterSpecific. If the cluster mode is 'ENHANCED' or 'LEGACY', minWorkers and maxWorkers must be specified, otherwise only numWorkers must be specified",
                violations.iterator().next().getMessage());
    }

    @Test
    public void testInvalidClusterAttributes() {
        DLTClusterSpecific cluster = createValidCluster();
        cluster.setMode(null);
        cluster.setMinWorkers(1L);
        cluster.setMaxWorkers(10L);
        Set<ConstraintViolation<DLTClusterSpecific>> violations = validator.validate(cluster);
        assertEquals(1, violations.size());
        assertEquals(
                "Invalid configuration for DLTClusterSpecific. If the cluster mode is 'ENHANCED' or 'LEGACY', minWorkers and maxWorkers must be specified, otherwise only numWorkers must be specified",
                violations.iterator().next().getMessage());
    }

    @Test
    public void testInvalidClusterAttributes2() {
        DLTClusterSpecific cluster = createValidCluster();
        cluster.setMode(PipelineClusterAutoscaleMode.LEGACY); // Legacy mode should not have numWorkers
        cluster.setNumWorkers(5L);
        Set<ConstraintViolation<DLTClusterSpecific>> violations = validator.validate(cluster);
        assertEquals(1, violations.size());
        assertEquals(
                "Invalid configuration for DLTClusterSpecific. If the cluster mode is 'ENHANCED' or 'LEGACY', minWorkers and maxWorkers must be specified, otherwise only numWorkers must be specified",
                violations.iterator().next().getMessage());
    }
}
