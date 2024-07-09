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

    @Test
    public void testToString() {
        AzureAuthConfig config = new AzureAuthConfig();
        config.setClientId("clientId");
        config.setTenantId("tenantId");
        config.setClientSecret("clientSecret");
        config.setSkuType("trial");

        String expectedToString =
                "AzureAuthConfig(clientId=clientId, tenantId=tenantId, clientSecret=clientSecret, skuType=trial)";
        assertEquals(expectedToString, config.toString());
    }

    @Test
    public void testCanEqual() {
        AzureAuthConfig config1 = new AzureAuthConfig();
        AzureAuthConfig config2 = new AzureAuthConfig();
        Object otherObject = new Object();

        assertTrue(config1.canEqual(config2));
        assertFalse(config1.canEqual(otherObject));
    }

    @Test
    public void testEquals() {
        AzureAuthConfig config1 = new AzureAuthConfig();
        config1.setClientId("clientId1");
        config1.setTenantId("tenantId1");
        config1.setClientSecret("clientSecret1");

        AzureAuthConfig config2 = new AzureAuthConfig();
        config2.setClientId("clientId1");
        config2.setTenantId("tenantId1");
        config2.setClientSecret("clientSecret1");

        AzureAuthConfig config3 = new AzureAuthConfig();
        config3.setClientId("clientId2");
        config3.setTenantId("tenantId2");
        config3.setClientSecret("clientSecret2");

        assertTrue(config1.equals(config2));
        assertFalse(config1.equals(config3));
        assertFalse(config2.equals(config3));
        assertFalse(config1.equals(null));
        assertFalse(config1.equals(new Object()));
    }

    @Test
    public void testHashCode() {
        AzureAuthConfig config1 = new AzureAuthConfig();
        config1.setClientId("clientId1");
        config1.setTenantId("tenantId1");
        config1.setClientSecret("clientSecret1");

        AzureAuthConfig config2 = new AzureAuthConfig();
        config2.setClientId("clientId1");
        config2.setTenantId("tenantId1");
        config2.setClientSecret("clientSecret1");

        AzureAuthConfig config3 = new AzureAuthConfig();
        config3.setClientId("clientId2");
        config3.setTenantId("tenantId2");
        config3.setClientSecret("clientSecret2");

        assertEquals(config1.hashCode(), config2.hashCode());
        assertNotEquals(config1.hashCode(), config3.hashCode());
        assertNotEquals(config2.hashCode(), config3.hashCode());
    }
}
