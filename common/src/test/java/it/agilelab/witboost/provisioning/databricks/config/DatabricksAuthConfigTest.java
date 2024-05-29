package it.agilelab.witboost.provisioning.databricks.config;

import static org.junit.jupiter.api.Assertions.*;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

@SpringBootTest
@EnableConfigurationProperties(DatabricksAuthConfig.class)
@Import(TestConfig.class)
public class DatabricksAuthConfigTest {

    @Autowired
    private DatabricksAuthConfig databricksAuthConfig;

    @TestConfiguration
    static class TestConfig {

        @Primary
        @Bean
        public DatabricksAuthConfig primaryDatabricksAuthConfig() {
            DatabricksAuthConfig config = new DatabricksAuthConfig();
            config.setAccountId("testAccountId");
            return config;
        }
    }

    @Test
    public void testConfigurationProperties() {
        assertEquals("testAccountId", databricksAuthConfig.getAccountId());
    }

    @Test
    public void testToString() {
        DatabricksAuthConfig config = new DatabricksAuthConfig();
        config.setAccountId("accountId");

        String expectedToString = "DatabricksAuthConfig(accountId=accountId)";
        assertEquals(expectedToString, config.toString());
    }

    @Test
    public void testCanEqual() {
        DatabricksAuthConfig config1 = new DatabricksAuthConfig();
        DatabricksAuthConfig config2 = new DatabricksAuthConfig();
        Object otherObject = new Object();

        assertTrue(config1.canEqual(config2));
        assertFalse(config1.canEqual(otherObject));
    }

    @Test
    public void testEquals() {
        DatabricksAuthConfig config1 = new DatabricksAuthConfig();
        config1.setAccountId("accountId1");

        DatabricksAuthConfig config2 = new DatabricksAuthConfig();
        config2.setAccountId("accountId1");

        DatabricksAuthConfig config3 = new DatabricksAuthConfig();
        config3.setAccountId("accountId2");

        assertTrue(config1.equals(config2));
        assertFalse(config1.equals(config3));
        assertFalse(config2.equals(config3));
        assertFalse(config1.equals(null));
        assertFalse(config1.equals(new Object()));
    }

    @Test
    public void testHashCode() {
        DatabricksAuthConfig config1 = new DatabricksAuthConfig();
        config1.setAccountId("accountId1");

        DatabricksAuthConfig config2 = new DatabricksAuthConfig();
        config2.setAccountId("accountId1");

        DatabricksAuthConfig config3 = new DatabricksAuthConfig();
        config3.setAccountId("accountId2");

        assertEquals(config1.hashCode(), config2.hashCode());
        assertNotEquals(config1.hashCode(), config3.hashCode());
        assertNotEquals(config2.hashCode(), config3.hashCode());
    }
}
