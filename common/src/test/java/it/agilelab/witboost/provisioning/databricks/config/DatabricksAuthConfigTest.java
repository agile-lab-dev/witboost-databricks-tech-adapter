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
}
