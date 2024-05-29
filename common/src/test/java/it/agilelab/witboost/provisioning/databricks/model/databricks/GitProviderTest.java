package it.agilelab.witboost.provisioning.databricks.model.databricks;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class GitProviderTest {

    @Test
    public void testFromStringValid() {
        assertThat(GitProvider.fromString("GITLAB")).isEqualTo(GitProvider.GITLAB);
        assertThat(GitProvider.fromString("gitlab")).isEqualTo(GitProvider.GITLAB);
        assertThat(GitProvider.fromString("GiTLaB")).isEqualTo(GitProvider.GITLAB);
    }

    @Test
    public void testFromStringInvalid() {
        assertThatThrownBy(() -> GitProvider.fromString("INVALID"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown gitProvider: INVALID");

        assertThatThrownBy(() -> GitProvider.fromString(null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown gitProvider: null");

        assertThatThrownBy(() -> GitProvider.fromString(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown gitProvider: ");
    }

    @Test
    public void testToString() {
        assertThat(GitProvider.GITLAB.toString()).isEqualTo("gitlab");
    }
}
