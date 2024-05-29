package it.agilelab.witboost.provisioning.databricks.model.databricks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.sdk.service.compute.AzureAvailability;
import com.databricks.sdk.service.compute.RuntimeEngine;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.HashMap;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class ClusterSpecificTest {

    private Validator validator;
    private ClusterSpecific clusterSpecific;

    @BeforeEach
    public void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
        clusterSpecific = new ClusterSpecific();
        // Set default values for required fields
        clusterSpecific.setClusterSparkVersion("someVersion");
        clusterSpecific.setNodeTypeId("someNodeTypeId");
        clusterSpecific.setNumWorkers(1L);
    }

    @Test
    public void testClusterSparkVersionNotBlank() {
        clusterSpecific.setClusterSparkVersion("");
        Set<ConstraintViolation<ClusterSpecific>> violations = validator.validate(clusterSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "clusterSparkVersion",
                violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testNodeTypeIdNotBlank() {
        clusterSpecific.setNodeTypeId("");
        Set<ConstraintViolation<ClusterSpecific>> violations = validator.validate(clusterSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "nodeTypeId", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testNumWorkersMin() {
        clusterSpecific.setNumWorkers(0L);
        Set<ConstraintViolation<ClusterSpecific>> violations = validator.validate(clusterSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "numWorkers", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testDefaultValues() {
        assertThat(clusterSpecific.getClusterSparkVersion()).isNotBlank();
        assertThat(clusterSpecific.getNodeTypeId()).isNotBlank();
        assertThat(clusterSpecific.getNumWorkers()).isGreaterThan(0);

        assertThat(clusterSpecific.getSpotBidMaxPrice()).isNull();
        assertThat(clusterSpecific.getFirstOnDemand()).isNull();
        assertThat(clusterSpecific.getSpotInstances()).isNull();
        assertThat(clusterSpecific.getAvailability()).isNull();
        assertThat(clusterSpecific.getDriverNodeTypeId()).isNull();
        assertThat(clusterSpecific.getSparkConf()).isNull();
        assertThat(clusterSpecific.getSparkEnvVars()).isNull();
        assertThat(clusterSpecific.getRuntimeEngine()).isNull();
    }

    @Test
    public void testSettersAndGetters() {
        clusterSpecific.setSpotBidMaxPrice(10D);
        clusterSpecific.setFirstOnDemand(5L);
        clusterSpecific.setSpotInstances(true);
        clusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        clusterSpecific.setDriverNodeTypeId("driverNodeTypeId");
        clusterSpecific.setSparkConf(new HashMap<>());
        clusterSpecific.setSparkEnvVars(new HashMap<>());
        clusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);

        assertEquals(10, clusterSpecific.getSpotBidMaxPrice());
        assertEquals(5, clusterSpecific.getFirstOnDemand());
        assertEquals(true, clusterSpecific.getSpotInstances());
        assertEquals(AzureAvailability.ON_DEMAND_AZURE, clusterSpecific.getAvailability());
        assertEquals("driverNodeTypeId", clusterSpecific.getDriverNodeTypeId());
        assertEquals(new HashMap<>(), clusterSpecific.getSparkConf());
        assertEquals(new HashMap<>(), clusterSpecific.getSparkEnvVars());
        assertEquals(RuntimeEngine.PHOTON, clusterSpecific.getRuntimeEngine());
    }
}
