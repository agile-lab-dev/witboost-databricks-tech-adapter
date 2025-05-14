package it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.sdk.service.pipelines.PipelineClusterAutoscaleMode;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workflow.DatabricksWorkflowWorkloadSpecific;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class DatabricksDLTWorkloadSpecificTest {

    private static Validator validator;

    @BeforeAll
    public static void setUp() {
        Locale.setDefault(Locale.ENGLISH);
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    private DatabricksDLTWorkloadSpecific createValidWorkload() {
        DLTClusterSpecific cluster = new DLTClusterSpecific();
        cluster.setMode(PipelineClusterAutoscaleMode.LEGACY);
        cluster.setWorkerType("Standard_DS3_v2");
        cluster.setDriverType("Standard_DS3_v2");
        cluster.setPolicyId("policyId");

        DatabricksDLTWorkloadSpecific specific = new DatabricksDLTWorkloadSpecific();
        specific.setWorkspace("workspace");
        specific.setRepoPath("dataproduct/component");
        specific.setPipelineName("pipelineName");
        specific.setProductEdition(DatabricksDLTWorkloadSpecific.ProductEdition.CORE);
        specific.setContinuous(true);
        specific.setNotebooks(List.of("notebook1", "notebook2"));
        specific.setFiles(List.of("file1", "file2"));
        specific.setMetastore("metastore");
        specific.setCatalog("catalog");
        specific.setTarget("target");
        specific.setPhoton(true);
        List notifications = new ArrayList();
        notifications.add(new DatabricksDLTWorkloadSpecific.PipelineNotification(
                "email@email.com", Collections.singletonList("on-update-test")));
        specific.setNotifications(notifications);
        specific.setChannel(DatabricksDLTWorkloadSpecific.PipelineChannel.CURRENT);
        specific.setCluster(cluster);

        DatabricksWorkflowWorkloadSpecific.GitSpecific dltGitSpecific =
                new DatabricksWorkflowWorkloadSpecific.GitSpecific();
        dltGitSpecific.setGitRepoUrl("https://github.com/repo.git");
        specific.setGit(dltGitSpecific);

        return specific;
    }

    @Test
    public void testValidWorkload() {
        DatabricksDLTWorkloadSpecific workload = createValidWorkload();
        Set<ConstraintViolation<DatabricksDLTWorkloadSpecific>> violations = validator.validate(workload);
        assertTrue(violations.isEmpty());
    }

    @Test
    public void testInvalidWorkspace() {
        DatabricksDLTWorkloadSpecific workload = createValidWorkload();
        workload.setWorkspace("");
        Set<ConstraintViolation<DatabricksDLTWorkloadSpecific>> violations = validator.validate(workload);
        assertEquals(1, violations.size());
        assertEquals("must not be blank", violations.iterator().next().getMessage());
    }

    @Test
    public void testInvalidPipelineName() {
        DatabricksDLTWorkloadSpecific workload = createValidWorkload();
        workload.setPipelineName("");
        Set<ConstraintViolation<DatabricksDLTWorkloadSpecific>> violations = validator.validate(workload);
        assertEquals(1, violations.size());
        assertEquals("must not be blank", violations.iterator().next().getMessage());
    }

    @Test
    public void testInvalidProductEdition() {
        DatabricksDLTWorkloadSpecific workload = createValidWorkload();
        workload.setProductEdition(null);
        Set<ConstraintViolation<DatabricksDLTWorkloadSpecific>> violations = validator.validate(workload);
        assertEquals(1, violations.size());
        assertEquals("must not be null", violations.iterator().next().getMessage());
    }

    @Test
    public void testInvalidContinuous() {
        DatabricksDLTWorkloadSpecific workload = createValidWorkload();
        workload.setContinuous(null);
        Set<ConstraintViolation<DatabricksDLTWorkloadSpecific>> violations = validator.validate(workload);
        assertEquals(1, violations.size());
        assertEquals("must not be null", violations.iterator().next().getMessage());
    }

    @Test
    public void testInvalidNotebooks() {
        DatabricksDLTWorkloadSpecific workload = createValidWorkload();
        workload.setNotebooks(List.of("", "notebook2"));
        Set<ConstraintViolation<DatabricksDLTWorkloadSpecific>> violations = validator.validate(workload);
        assertEquals(1, violations.size());
        assertEquals("must not be blank", violations.iterator().next().getMessage());
    }

    @Test
    public void testInvalidCatalog() {
        DatabricksDLTWorkloadSpecific workload = createValidWorkload();
        workload.setCatalog("");
        Set<ConstraintViolation<DatabricksDLTWorkloadSpecific>> violations = validator.validate(workload);
        assertEquals(1, violations.size());
        assertEquals("must not be blank", violations.iterator().next().getMessage());
    }

    @Test
    public void testInvalidPhoton() {
        DatabricksDLTWorkloadSpecific workload = createValidWorkload();
        workload.setPhoton(null);
        Set<ConstraintViolation<DatabricksDLTWorkloadSpecific>> violations = validator.validate(workload);
        assertEquals(1, violations.size());
        assertEquals("must not be null", violations.iterator().next().getMessage());
    }

    @Test
    public void testInvalidCluster() {
        DatabricksDLTWorkloadSpecific workload = createValidWorkload();
        workload.setCluster(null);
        Set<ConstraintViolation<DatabricksDLTWorkloadSpecific>> violations = validator.validate(workload);
        assertEquals(1, violations.size());
        assertEquals("must not be null", violations.iterator().next().getMessage());
    }
}
