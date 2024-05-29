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
            return config;
        }
    }

    @Test
    public void testConfigurationProperties() {
        assertEquals("testUsername", gitCredentialsConfig.getUsername());
        assertEquals("testToken", gitCredentialsConfig.getToken());
    }

    @Test
    public void testToString() {
        GitCredentialsConfig config = new GitCredentialsConfig();
        config.setUsername("username");
        config.setToken("token");

        String expectedToString = "GitCredentialsConfig(username=username, token=token)";
        assertEquals(expectedToString, config.toString());
    }

    @Test
    public void testCanEqual() {
        GitCredentialsConfig config1 = new GitCredentialsConfig();
        GitCredentialsConfig config2 = new GitCredentialsConfig();
        Object otherObject = new Object();

        assertTrue(config1.canEqual(config2));
        assertFalse(config1.canEqual(otherObject));
    }

    @Test
    public void testEquals() {
        GitCredentialsConfig config1 = new GitCredentialsConfig();
        config1.setUsername("username1");
        config1.setToken("token1");

        GitCredentialsConfig config2 = new GitCredentialsConfig();
        config2.setUsername("username1");
        config2.setToken("token1");

        GitCredentialsConfig config3 = new GitCredentialsConfig();
        config3.setUsername("username2");
        config3.setToken("token2");

        assertTrue(config1.equals(config2));
        assertFalse(config1.equals(config3));
        assertFalse(config2.equals(config3));
        assertFalse(config1.equals(null));
        assertFalse(config1.equals(new Object()));
    }

    @Test
    public void testHashCode() {
        GitCredentialsConfig config1 = new GitCredentialsConfig();
        config1.setUsername("username1");
        config1.setToken("token1");

        GitCredentialsConfig config2 = new GitCredentialsConfig();
        config2.setUsername("username1");
        config2.setToken("token1");

        GitCredentialsConfig config3 = new GitCredentialsConfig();
        config3.setUsername("username2");
        config3.setToken("token2");

        assertEquals(config1.hashCode(), config2.hashCode());
        assertNotEquals(config1.hashCode(), config3.hashCode());
        assertNotEquals(config2.hashCode(), config3.hashCode());
    }
}
