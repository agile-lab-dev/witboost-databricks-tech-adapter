package it.agilelab.witboost.provisioning.databricks.model.databricks;

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
public class DatabricksWorkloadSpecificTest {

    private Validator validator;
    private DatabricksWorkloadSpecific workloadSpecific1;
    private DatabricksWorkloadSpecific workloadSpecific2;
    private DatabricksWorkloadSpecific workloadSpecific3;

    @BeforeEach
    public void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();

        workloadSpecific1 = createWorkloadSpecific();
        workloadSpecific2 = createWorkloadSpecific();
        workloadSpecific3 = new DatabricksWorkloadSpecific();

        workloadSpecific3.setWorkspace("testWorkspace3");
        workloadSpecific3.setJobName("testJob3");

        GitSpecific gitSpecific3 = new GitSpecific();
        gitSpecific3.setGitReference("main");
        gitSpecific3.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific3.setGitPath("/src");
        gitSpecific3.setGitRepoUrl("https://github.com/repo3.git");
        workloadSpecific3.setGit(gitSpecific3);

        SchedulingSpecific schedulingSpecific3 = new SchedulingSpecific();
        schedulingSpecific3.setEnableScheduling(true);
        schedulingSpecific3.setCronExpression("0 0 12 * * ?");
        schedulingSpecific3.setJavaTimezoneId("Europe/Rome");
        workloadSpecific3.setScheduling(schedulingSpecific3);

        ClusterSpecific clusterSpecific3 = new ClusterSpecific();
        clusterSpecific3.setClusterSparkVersion("7.3.x-ml-scala2.12");
        clusterSpecific3.setNodeTypeId("Standard_D3_v2");
        clusterSpecific3.setNumWorkers(2L);
        workloadSpecific3.setCluster(clusterSpecific3);
    }

    private DatabricksWorkloadSpecific createWorkloadSpecific() {
        DatabricksWorkloadSpecific workloadSpecific = new DatabricksWorkloadSpecific();
        workloadSpecific.setWorkspace("testWorkspace");
        workloadSpecific.setJobName("testJob");

        GitSpecific gitSpecific = new GitSpecific();
        gitSpecific.setGitReference("main");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific.setGitPath("/src");
        gitSpecific.setGitRepoUrl("https://github.com/repo.git");
        workloadSpecific.setGit(gitSpecific);

        SchedulingSpecific schedulingSpecific = new SchedulingSpecific();
        schedulingSpecific.setEnableScheduling(true);
        schedulingSpecific.setCronExpression("0 0 12 * * ?");
        schedulingSpecific.setJavaTimezoneId("Europe/Rome");
        workloadSpecific.setScheduling(schedulingSpecific);

        ClusterSpecific clusterSpecific = new ClusterSpecific();
        clusterSpecific.setClusterSparkVersion("7.3.x-ml-scala2.12");
        clusterSpecific.setNodeTypeId("Standard_D3_v2");
        clusterSpecific.setNumWorkers(2L);
        workloadSpecific.setCluster(clusterSpecific);

        return workloadSpecific;
    }

    @Test
    public void testWorkspaceNotBlank() {
        workloadSpecific1.setWorkspace("");
        Set<ConstraintViolation<DatabricksWorkloadSpecific>> violations = validator.validate(workloadSpecific1);
        assertEquals(1, violations.size());
        assertEquals("workspace", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testJobNameNotBlank() {
        workloadSpecific1.setJobName("");
        Set<ConstraintViolation<DatabricksWorkloadSpecific>> violations = validator.validate(workloadSpecific1);
        assertEquals(1, violations.size());
        assertEquals("jobName", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testGitNotNull() {
        workloadSpecific1.setGit(null);
        Set<ConstraintViolation<DatabricksWorkloadSpecific>> violations = validator.validate(workloadSpecific1);
        assertEquals(1, violations.size());
        assertEquals("git", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testSchedulingNotNull() {
        workloadSpecific1.setScheduling(null);
        Set<ConstraintViolation<DatabricksWorkloadSpecific>> violations = validator.validate(workloadSpecific1);
        assertEquals(1, violations.size());
        assertEquals(
                "scheduling", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testClusterNotNull() {
        workloadSpecific1.setCluster(null);
        Set<ConstraintViolation<DatabricksWorkloadSpecific>> violations = validator.validate(workloadSpecific1);
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

        GitSpecific gitSpecific = workloadSpecific1.getGit();
        assertEquals("main", gitSpecific.getGitReference());
        assertEquals(GitReferenceType.BRANCH, gitSpecific.getGitReferenceType());
        assertEquals("/src", gitSpecific.getGitPath());
        assertEquals("https://github.com/repo.git", gitSpecific.getGitRepoUrl());

        SchedulingSpecific schedulingSpecific = workloadSpecific1.getScheduling();
        assertEquals(true, schedulingSpecific.getEnableScheduling());
        assertEquals("0 0 12 * * ?", schedulingSpecific.getCronExpression());
        assertEquals("Europe/Rome", schedulingSpecific.getJavaTimezoneId());

        ClusterSpecific clusterSpecific = workloadSpecific1.getCluster();
        assertEquals("7.3.x-ml-scala2.12", clusterSpecific.getClusterSparkVersion());
        assertEquals("Standard_D3_v2", clusterSpecific.getNodeTypeId());
        assertEquals(2, clusterSpecific.getNumWorkers());
    }

    @AfterEach
    public void tearDown() {
        workloadSpecific1 = null;
        workloadSpecific2 = null;
        workloadSpecific3 = null;
    }
}
