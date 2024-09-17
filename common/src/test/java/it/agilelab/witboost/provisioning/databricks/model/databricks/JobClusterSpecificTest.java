package it.agilelab.witboost.provisioning.databricks.model.databricks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.sdk.service.compute.AzureAvailability;
import com.databricks.sdk.service.compute.RuntimeEngine;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.JobClusterSpecific;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class JobClusterSpecificTest {

    private Validator validator;
    private JobClusterSpecific jobClusterSpecific;

    @BeforeEach
    public void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
        jobClusterSpecific = new JobClusterSpecific();
        // Set default values for required fields
        jobClusterSpecific.setClusterSparkVersion("someVersion");
        jobClusterSpecific.setNodeTypeId("someNodeTypeId");
        jobClusterSpecific.setNumWorkers(1L);
    }

    @Test
    public void testClusterSparkVersionNotBlank() {
        jobClusterSpecific.setClusterSparkVersion("");
        Set<ConstraintViolation<JobClusterSpecific>> violations = validator.validate(jobClusterSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "clusterSparkVersion",
                violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testNodeTypeIdNotBlank() {
        jobClusterSpecific.setNodeTypeId("");
        Set<ConstraintViolation<JobClusterSpecific>> violations = validator.validate(jobClusterSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "nodeTypeId", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testNumWorkersMin() {
        jobClusterSpecific.setNumWorkers(0L);
        Set<ConstraintViolation<JobClusterSpecific>> violations = validator.validate(jobClusterSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "numWorkers", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testDefaultValues() {
        assertThat(jobClusterSpecific.getClusterSparkVersion()).isNotBlank();
        assertThat(jobClusterSpecific.getNodeTypeId()).isNotBlank();
        assertThat(jobClusterSpecific.getNumWorkers()).isGreaterThan(0);

        assertThat(jobClusterSpecific.getSpotBidMaxPrice()).isNull();
        assertThat(jobClusterSpecific.getFirstOnDemand()).isNull();
        assertThat(jobClusterSpecific.getSpotInstances()).isNull();
        assertThat(jobClusterSpecific.getAvailability()).isNull();
        assertThat(jobClusterSpecific.getDriverNodeTypeId()).isNull();
        assertThat(jobClusterSpecific.getSparkConf()).isNull();
        assertThat(jobClusterSpecific.getSparkEnvVars()).isNull();
        assertThat(jobClusterSpecific.getRuntimeEngine()).isNull();
    }

    @Test
    public void testSettersAndGetters() {
        jobClusterSpecific.setSpotBidMaxPrice(10D);
        jobClusterSpecific.setFirstOnDemand(5L);
        jobClusterSpecific.setSpotInstances(true);
        jobClusterSpecific.setAvailability(AzureAvailability.ON_DEMAND_AZURE);
        jobClusterSpecific.setDriverNodeTypeId("driverNodeTypeId");
        SparkConf sparkConf = new SparkConf();
        sparkConf.setName("spark.conf");
        sparkConf.setValue("value");
        jobClusterSpecific.setSparkConf(List.of(sparkConf));
        SparkEnvVar sparkEnvVar = new SparkEnvVar();
        sparkEnvVar.setName("spark.env.var");
        sparkEnvVar.setValue("value");
        jobClusterSpecific.setSparkEnvVars(List.of(sparkEnvVar));
        jobClusterSpecific.setRuntimeEngine(RuntimeEngine.PHOTON);

        assertEquals(10, jobClusterSpecific.getSpotBidMaxPrice());
        assertEquals(5, jobClusterSpecific.getFirstOnDemand());
        assertEquals(true, jobClusterSpecific.getSpotInstances());
        assertEquals(AzureAvailability.ON_DEMAND_AZURE, jobClusterSpecific.getAvailability());
        assertEquals("driverNodeTypeId", jobClusterSpecific.getDriverNodeTypeId());
        assertEquals(
                sparkConf.getName(), jobClusterSpecific.getSparkConf().get(0).getName());
        assertEquals(
                sparkConf.getValue(), jobClusterSpecific.getSparkConf().get(0).getValue());
        assertEquals(
                sparkEnvVar.getName(),
                jobClusterSpecific.getSparkEnvVars().get(0).getName());
        assertEquals(
                sparkEnvVar.getValue(),
                jobClusterSpecific.getSparkEnvVars().get(0).getValue());
        assertEquals(RuntimeEngine.PHOTON, jobClusterSpecific.getRuntimeEngine());
    }
}
