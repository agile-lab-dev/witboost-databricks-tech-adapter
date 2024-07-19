package it.agilelab.witboost.provisioning.databricks.model.databricks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.GitReferenceType;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.GitSpecific;
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
public class GitSpecificTest {

    private Validator validator;
    private GitSpecific gitSpecific;

    @BeforeEach
    public void setUp() {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
        gitSpecific = new GitSpecific();
        gitSpecific.setGitReference("main");
        gitSpecific.setGitReferenceType(GitReferenceType.BRANCH);
        gitSpecific.setGitPath("/src");
        gitSpecific.setGitRepoUrl("https://github.com/repo.git");
    }

    @Test
    public void testGitReferenceNotBlank() {
        gitSpecific.setGitReference("");
        Set<ConstraintViolation<GitSpecific>> violations = validator.validate(gitSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "gitReference", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testGitReferenceTypeNotNull() {
        gitSpecific.setGitReferenceType(null);
        Set<ConstraintViolation<GitSpecific>> violations = validator.validate(gitSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "gitReferenceType",
                violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testGitPathNotBlank() {
        gitSpecific.setGitPath("");
        Set<ConstraintViolation<GitSpecific>> violations = validator.validate(gitSpecific);
        assertEquals(1, violations.size());
        assertEquals("gitPath", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testGitRepoNotBlank() {
        gitSpecific.setGitRepoUrl("");
        Set<ConstraintViolation<GitSpecific>> violations = validator.validate(gitSpecific);
        assertEquals(1, violations.size());
        assertEquals(
                "gitRepoUrl", violations.iterator().next().getPropertyPath().toString());
    }

    @Test
    public void testDefaultValues() {
        assertNotNull(gitSpecific);
        assertThat(gitSpecific.getGitReference()).isNotBlank();
        assertThat(gitSpecific.getGitReferenceType()).isNotNull();
        assertThat(gitSpecific.getGitPath()).isNotBlank();
        assertThat(gitSpecific.getGitRepoUrl()).isNotBlank();
    }

    @Test
    public void testSettersAndGetters() {
        assertEquals("main", gitSpecific.getGitReference());
        assertEquals(GitReferenceType.BRANCH, gitSpecific.getGitReferenceType());
        assertEquals("/src", gitSpecific.getGitPath());
        assertEquals("https://github.com/repo.git", gitSpecific.getGitRepoUrl());
    }
}
