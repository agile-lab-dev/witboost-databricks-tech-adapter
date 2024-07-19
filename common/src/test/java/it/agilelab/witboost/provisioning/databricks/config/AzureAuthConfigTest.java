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
@EnableConfigurationProperties(AzureAuthConfig.class)
@Import(TestConfig.class)
public class AzureAuthConfigTest {

    @Autowired
    private AzureAuthConfig azureAuthConfig;

    @TestConfiguration
    static class AzureAuthTestConfig {

        @Primary
        @Bean
        public AzureAuthConfig primaryAzureAuthConfig() {
            AzureAuthConfig config = new AzureAuthConfig();
            config.setClientId("testClientId");
            config.setTenantId("testTenantId");
            config.setClientSecret("testClientSecret");
            return config;
        }
    }

    @Test
    public void testConfigurationProperties() {
        assertEquals("testClientId", azureAuthConfig.getClientId());
        assertEquals("testTenantId", azureAuthConfig.getTenantId());
        assertEquals("testClientSecret", azureAuthConfig.getClientSecret());
    }
}
