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

    @Test
    public void testToString() {
        AzurePermissionsConfig config = new AzurePermissionsConfig();
        config.setRoleDefinitionId("roleDefinitionId");
        config.setSubscriptionId("subscriptionId");
        config.setResourceGroup("resourceGroup");
        config.setAuth_clientId("clientId");
        config.setAuth_tenantId("tenantId");
        config.setAuth_clientSecret("clientSecret");

        String expectedToString =
                "AzurePermissionsConfig(auth_clientId=clientId, auth_tenantId=tenantId, auth_clientSecret=clientSecret, roleDefinitionId=roleDefinitionId, subscriptionId=subscriptionId, resourceGroup=resourceGroup)";
        assertEquals(expectedToString, config.toString());
    }

    @Test
    public void testCanEqual() {
        AzurePermissionsConfig config1 = new AzurePermissionsConfig();
        AzurePermissionsConfig config2 = new AzurePermissionsConfig();
        Object otherObject = new Object();

        assertTrue(config1.canEqual(config2));
        assertFalse(config1.canEqual(otherObject));
    }

    @Test
    public void testEquals() {
        AzurePermissionsConfig config1 = new AzurePermissionsConfig();
        config1.setRoleDefinitionId("roleDefinitionId1");
        config1.setSubscriptionId("subscriptionId1");
        config1.setResourceGroup("resourceGroup1");
        config1.setAuth_clientId("clientId1");
        config1.setAuth_tenantId("tenantId1");
        config1.setAuth_clientSecret("clientSecret1");

        AzurePermissionsConfig config2 = new AzurePermissionsConfig();
        config2.setRoleDefinitionId("roleDefinitionId1");
        config2.setSubscriptionId("subscriptionId1");
        config2.setResourceGroup("resourceGroup1");
        config2.setAuth_clientId("clientId1");
        config2.setAuth_tenantId("tenantId1");
        config2.setAuth_clientSecret("clientSecret1");

        AzurePermissionsConfig config3 = new AzurePermissionsConfig();
        config3.setRoleDefinitionId("roleDefinitionId2");
        config3.setSubscriptionId("subscriptionId2");
        config3.setResourceGroup("resourceGroup2");
        config3.setAuth_clientId("clientId2");
        config3.setAuth_tenantId("tenantId2");
        config3.setAuth_clientSecret("clientSecret2");

        assertTrue(config1.equals(config2));
        assertFalse(config1.equals(config3));
        assertFalse(config2.equals(config3));
        assertFalse(config1.equals(null));
        assertFalse(config1.equals(new Object()));
    }

    @Test
    public void testHashCode() {
        AzurePermissionsConfig config1 = new AzurePermissionsConfig();
        config1.setRoleDefinitionId("roleDefinitionId1");
        config1.setSubscriptionId("subscriptionId1");
        config1.setResourceGroup("resourceGroup1");
        config1.setAuth_clientId("clientId1");
        config1.setAuth_tenantId("tenantId1");
        config1.setAuth_clientSecret("clientSecret1");

        AzurePermissionsConfig config2 = new AzurePermissionsConfig();
        config2.setRoleDefinitionId("roleDefinitionId1");
        config2.setSubscriptionId("subscriptionId1");
        config2.setResourceGroup("resourceGroup1");
        config2.setAuth_clientId("clientId1");
        config2.setAuth_tenantId("tenantId1");
        config2.setAuth_clientSecret("clientSecret1");

        AzurePermissionsConfig config3 = new AzurePermissionsConfig();
        config3.setRoleDefinitionId("roleDefinitionId2");
        config3.setSubscriptionId("subscriptionId2");
        config3.setResourceGroup("resourceGroup2");
        config3.setAuth_clientId("clientId2");
        config3.setAuth_tenantId("tenantId2");
        config3.setAuth_clientSecret("clientSecret2");

        assertEquals(config1.hashCode(), config2.hashCode());
        assertNotEquals(config1.hashCode(), config3.hashCode());
        assertNotEquals(config2.hashCode(), config3.hashCode());
    }
}
