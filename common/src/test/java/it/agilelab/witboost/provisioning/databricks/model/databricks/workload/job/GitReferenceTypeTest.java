package it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class GitReferenceTypeTest {

    @Test
    public void testFromStringValid() {
        assertThat(DatabricksJobWorkloadSpecific.GitReferenceType.fromString("BRANCH"))
                .isEqualTo(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);
        assertThat(DatabricksJobWorkloadSpecific.GitReferenceType.fromString("branch"))
                .isEqualTo(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);
        assertThat(DatabricksJobWorkloadSpecific.GitReferenceType.fromString("Branch"))
                .isEqualTo(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH);

        assertThat(DatabricksJobWorkloadSpecific.GitReferenceType.fromString("TAG"))
                .isEqualTo(DatabricksJobWorkloadSpecific.GitReferenceType.TAG);
        assertThat(DatabricksJobWorkloadSpecific.GitReferenceType.fromString("tag"))
                .isEqualTo(DatabricksJobWorkloadSpecific.GitReferenceType.TAG);
        assertThat(DatabricksJobWorkloadSpecific.GitReferenceType.fromString("Tag"))
                .isEqualTo(DatabricksJobWorkloadSpecific.GitReferenceType.TAG);
    }

    @Test
    public void testFromStringInvalid() {
        assertThatThrownBy(() -> DatabricksJobWorkloadSpecific.GitReferenceType.fromString("INVALID"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown reference type: INVALID");

        assertThatThrownBy(() -> DatabricksJobWorkloadSpecific.GitReferenceType.fromString(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown reference type: null");

        assertThatThrownBy(() -> DatabricksJobWorkloadSpecific.GitReferenceType.fromString(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown reference type: ");
    }

    @Test
    public void testToString() {
        assertThat(DatabricksJobWorkloadSpecific.GitReferenceType.BRANCH.toString())
                .isEqualTo("branch");
        assertThat(DatabricksJobWorkloadSpecific.GitReferenceType.TAG.toString())
                .isEqualTo("tag");
    }
}
