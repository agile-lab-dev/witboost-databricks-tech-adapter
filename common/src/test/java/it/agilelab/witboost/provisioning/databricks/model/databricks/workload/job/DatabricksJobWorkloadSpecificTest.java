package it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class DatabricksJobWorkloadSpecificTest {

    private Validator validator;
    private DatabricksJobWorkloadSpecific workloadSpecific1;
    private DatabricksJobWorkloadSpecific workloadSpecific2;
    private DatabricksJobWorkloadSpecific workloadSpecific3;

    @BeforeEach
    public void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();

        workloadSpecific1 = createWorkloadSpecific();
        workloadSpecific2 = createWorkloadSpecific();
        workloadSpecific3 = new DatabricksJobWorkloadSpecific();

        workloadSpecific3.setWorkspace("testWorkspace3");
        workloadSpecific3.setJobName("testJob3");

        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific3 =
                new DatabricksJobWorkloadSpecific.JobGitSpecific();
        jobGitSpecific3.setGitReference("main");
        jobGitSpecific3.setGitReferenceType(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);
        jobGitSpecific3.setGitPath("/src");
        jobGitSpecific3.setGitRepoUrl("https://github.com/repo3.git");
        workloadSpecific3.setGit(jobGitSpecific3);

        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific3 =
                new DatabricksJobWorkloadSpecific.SchedulingSpecific();
        schedulingSpecific3.setCronExpression("0 0 12 * * ?");
        schedulingSpecific3.setJavaTimezoneId("Europe/Rome");
        workloadSpecific3.setScheduling(schedulingSpecific3);

        JobClusterSpecific jobClusterSpecific3 = new JobClusterSpecific();
        jobClusterSpecific3.setClusterSparkVersion("7.3.x-ml-scala2.12");
        jobClusterSpecific3.setNodeTypeId("Standard_D3_v2");
        jobClusterSpecific3.setNumWorkers(2L);
        workloadSpecific3.setCluster(jobClusterSpecific3);
    }

    private DatabricksJobWorkloadSpecific createWorkloadSpecific() {
        DatabricksJobWorkloadSpecific workloadSpecific = new DatabricksJobWorkloadSpecific();
        workloadSpecific.setWorkspace("testWorkspace");
        workloadSpecific.setJobName("testJob");
        workloadSpecific.setRepoPath("dataproduct/component");

        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific =
                new DatabricksJobWorkloadSpecific.JobGitSpecific();
        jobGitSpecific.setGitReference("main");
        jobGitSpecific.setGitReferenceType(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);
        jobGitSpecific.setGitPath("/src");
        jobGitSpecific.setGitRepoUrl("https://github.com/repo.git");
        workloadSpecific.setGit(jobGitSpecific);

        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific =
                new DatabricksJobWorkloadSpecific.SchedulingSpecific();
        schedulingSpecific.setCronExpression("0 0 12 * * ?");
        schedulingSpecific.setJavaTimezoneId("Europe/Rome");
        workloadSpecific.setScheduling(schedulingSpecific);

        JobClusterSpecific jobClusterSpecific = new JobClusterSpecific();
        jobClusterSpecific.setClusterSparkVersion("7.3.x-ml-scala2.12");
        jobClusterSpecific.setNodeTypeId("Standard_D3_v2");
        jobClusterSpecific.setNumWorkers(2L);
        workloadSpecific.setCluster(jobClusterSpecific);

        return workloadSpecific;
    }

    @Test
    public void testWorkspaceNotBlank() {
        workloadSpecific1.setWorkspace("");
        Set<ConstraintViolation<DatabricksJobWorkloadSpecific>> violations = validator.validate(workloadSpecific1);
        assertEquals(1, violations.size());
        assertEquals("workspace", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testWorkspaceNotNull() {
        workloadSpecific1.setWorkspace(null);
        Set<ConstraintViolation<DatabricksJobWorkloadSpecific>> violations = validator.validate(workloadSpecific1);
        assertEquals(1, violations.size());
        assertEquals("workspace", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testJobNameNotBlank() {
        workloadSpecific1.setJobName("");
        Set<ConstraintViolation<DatabricksJobWorkloadSpecific>> violations = validator.validate(workloadSpecific1);
        assertEquals(1, violations.size());
        assertEquals("jobName", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testGitNotNull() {
        workloadSpecific1.setGit(null);
        Set<ConstraintViolation<DatabricksJobWorkloadSpecific>> violations = validator.validate(workloadSpecific1);
        assertEquals(1, violations.size());
        assertEquals("git", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testClusterNotNull() {
        workloadSpecific1.setCluster(null);
        Set<ConstraintViolation<DatabricksJobWorkloadSpecific>> violations = validator.validate(workloadSpecific1);
        assertEquals(1, violations.size());
        assertEquals("cluster", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testDefaultValues() {
        assertNotNull(workloadSpecific1);
        assertThat(workloadSpecific1.getWorkspace()).isNotBlank();
        assertThat(workloadSpecific1.getJobName()).isNotBlank();
        assertNull(workloadSpecific1.getDescription());
        assertNotNull(workloadSpecific1.getGit());
        assertNotNull(workloadSpecific1.getScheduling());
        assertNotNull(workloadSpecific1.getCluster());
    }

    @Test
    public void testSettersAndGetters() {
        assertEquals("testWorkspace", workloadSpecific1.getWorkspace());
        assertEquals("testJob", workloadSpecific1.getJobName());
        workloadSpecific1.setDescription("Test description");
        assertEquals("Test description", workloadSpecific1.getDescription());

        DatabricksJobWorkloadSpecific.JobGitSpecific jobGitSpecific = workloadSpecific1.getGit();
        assertEquals("main", jobGitSpecific.getGitReference());
        assertEquals(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH, jobGitSpecific.getGitReferenceType());
        assertEquals("/src", jobGitSpecific.getGitPath());
        assertEquals("https://github.com/repo.git", jobGitSpecific.getGitRepoUrl());

        DatabricksJobWorkloadSpecific.SchedulingSpecific schedulingSpecific = workloadSpecific1.getScheduling();
        assertEquals("0 0 12 * * ?", schedulingSpecific.getCronExpression());
        assertEquals("Europe/Rome", schedulingSpecific.getJavaTimezoneId());

        JobClusterSpecific jobClusterSpecific = workloadSpecific1.getCluster();
        assertEquals("7.3.x-ml-scala2.12", jobClusterSpecific.getClusterSparkVersion());
        assertEquals("Standard_D3_v2", jobClusterSpecific.getNodeTypeId());
        assertEquals(2, jobClusterSpecific.getNumWorkers());
    }

    @AfterEach
    public void tearDown() {
        workloadSpecific1 = null;
        workloadSpecific2 = null;
        workloadSpecific3 = null;
    }
}
