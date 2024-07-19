package it.agilelab.witboost.provisioning.databricks.model.databricks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.model.databricks.job.GitReferenceType;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class GitReferenceTypeTest {

    @Test
    public void testFromStringValid() {
        assertThat(GitReferenceType.fromString("BRANCH")).isEqualTo(GitReferenceType.BRANCH);
        assertThat(GitReferenceType.fromString("branch")).isEqualTo(GitReferenceType.BRANCH);
        assertThat(GitReferenceType.fromString("Branch")).isEqualTo(GitReferenceType.BRANCH);

        assertThat(GitReferenceType.fromString("TAG")).isEqualTo(GitReferenceType.TAG);
        assertThat(GitReferenceType.fromString("tag")).isEqualTo(GitReferenceType.TAG);
        assertThat(GitReferenceType.fromString("Tag")).isEqualTo(GitReferenceType.TAG);
    }

    @Test
    public void testFromStringInvalid() {
        assertThatThrownBy(() -> GitReferenceType.fromString("INVALID"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown reference type: INVALID");

        assertThatThrownBy(() -> GitReferenceType.fromString(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown reference type: null");

        assertThatThrownBy(() -> GitReferenceType.fromString(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown reference type: ");
    }

    @Test
    public void testToString() {
        assertThat(GitReferenceType.BRANCH.toString()).isEqualTo("branch");
        assertThat(GitReferenceType.TAG.toString()).isEqualTo("tag");
    }
}
