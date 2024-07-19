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
@EnableConfigurationProperties(AzurePermissionsConfig.class)
@Import(TestConfig.class)
public class AzurePermissionsConfigTest {

    @Autowired
    private AzurePermissionsConfig azurePermissionsConfig;

    @TestConfiguration
    static class TestConfig {

        @Primary
        @Bean
        public AzurePermissionsConfig primaryAzurePermissionsConfig() {
            AzurePermissionsConfig config = new AzurePermissionsConfig();
            config.setRoleDefinitionId("testRoleDefinitionId");
            config.setSubscriptionId("testSubscriptionId");
            config.setResourceGroup("testResourceGroup");
            config.setAuth_clientId("testClientId");
            config.setAuth_tenantId("testTenantId");
            config.setAuth_clientSecret("testClientSecret");
            return config;
        }
    }

    @Test
    public void testConfigurationProperties() {
        assertEquals("testRoleDefinitionId", azurePermissionsConfig.getRoleDefinitionId());
        assertEquals("testSubscriptionId", azurePermissionsConfig.getSubscriptionId());
        assertEquals("testResourceGroup", azurePermissionsConfig.getResourceGroup());
        assertEquals("testClientId", azurePermissionsConfig.getAuth_clientId());
        assertEquals("testTenantId", azurePermissionsConfig.getAuth_tenantId());
        assertEquals("testClientSecret", azurePermissionsConfig.getAuth_clientSecret());
    }
}
