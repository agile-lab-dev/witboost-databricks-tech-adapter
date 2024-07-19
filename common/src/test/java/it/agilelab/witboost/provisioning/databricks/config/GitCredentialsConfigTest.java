package it.agilelab.witboost.provisioning.databricks.config;

import static org.junit.jupiter.api.Assertions.*;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@SpringBootTest(classes = {TestConfig.class})
@EnableConfigurationProperties(GitCredentialsConfig.class)
public class GitCredentialsConfigTest {

    @Autowired
    private GitCredentialsConfig gitCredentialsConfig;

    @TestConfiguration
    static class TestConfig {

        @Primary
        @Bean
        public GitCredentialsConfig primaryGitCredentialsConfig() {
            GitCredentialsConfig config = new GitCredentialsConfig();
            config.setUsername("testUsername");
            config.setToken("testToken");
            config.setProvider("GITLAB");
            return config;
        }
    }

    @Test
    public void testConfigurationProperties() {
        assertEquals("testUsername", gitCredentialsConfig.getUsername());
        assertEquals("testToken", gitCredentialsConfig.getToken());
    }
}
