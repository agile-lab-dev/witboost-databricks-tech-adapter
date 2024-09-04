package it.agilelab.witboost.provisioning.databricks.model.databricks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.GitReferenceType;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.JobGitSpecific;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class JobGitSpecificTest {

    private Validator validator;
    private JobGitSpecific jobGitSpecific;

    @BeforeEach
    public void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
        jobGitSpecific = new JobGitSpecific();
        jobGitSpecific.setGitReference("main");
        jobGitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        jobGitSpecific.setGitPath("/src");
        jobGitSpecific.setGitRepoUrl("https://github.com/repo.git");
    }

    @Test
    public void testGitReferenceNotBlank() {
        jobGitSpecific.setGitReference("");
        Set<ConstraintViolation<JobGitSpecific>> violations = validator.validate(jobGitSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "gitReference", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testGitReferenceTypeNotNull() {
        jobGitSpecific.setGitReferenceType(null);
        Set<ConstraintViolation<JobGitSpecific>> violations = validator.validate(jobGitSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "gitReferenceType",
                violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testGitPathNotBlank() {
        jobGitSpecific.setGitPath("");
        Set<ConstraintViolation<JobGitSpecific>> violations = validator.validate(jobGitSpecific);
        assertEquals(1, violations.size());
        assertEquals("gitPath", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testGitRepoNotBlank() {
        jobGitSpecific.setGitRepoUrl("");
        Set<ConstraintViolation<JobGitSpecific>> violations = validator.validate(jobGitSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "gitRepoUrl", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testDefaultValues() {
        assertNotNull(jobGitSpecific);
        assertThat(jobGitSpecific.getGitReference()).isNotBlank();
        assertThat(jobGitSpecific.getGitReferenceType()).isNotNull();
        assertThat(jobGitSpecific.getGitPath()).isNotBlank();
        assertThat(jobGitSpecific.getGitRepoUrl()).isNotBlank();
    }

    @Test
    public void testSettersAndGetters() {
        assertEquals("main", jobGitSpecific.getGitReference());
        assertEquals(GitReferenceType.BRANCH, jobGitSpecific.getGitReferenceType());
        assertEquals("/src", jobGitSpecific.getGitPath());
        assertEquals("https://github.com/repo.git", jobGitSpecific.getGitRepoUrl());
    }
}
