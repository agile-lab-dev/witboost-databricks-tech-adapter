package it.agilelab.witboost.provisioning.databricks.bean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;

import com.databricks.sdk.core.ApiClient;
import it.agilelab.witboost.provisioning.databricks.TestConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(TestConfig.class)
public class DatabricksApiClientBeanTest {

    private DatabricksApiClientBean apiClientBean;

    String workspaceHost = "https://example.databricks.com";

    @BeforeEach
    public void setUp() {
        // Mocking dependencies
        DatabricksAuthConfig databricksAuthConfig = mock(DatabricksAuthConfig.class);
        AzureAuthConfig azureAuthConfig = mock(AzureAuthConfig.class);

        apiClientBean = new DatabricksApiClientBean(workspaceHost, databricksAuthConfig, azureAuthConfig);
        apiClientBean.setWorkspaceHost(workspaceHost);
    }

    @Test
    public void testGetObject() {
        assertInstanceOf(ApiClient.class, apiClientBean.getObject(workspaceHost));
    }

    @Test
    public void testGetObjectType() {
        assertEquals(ApiClient.class, apiClientBean.getObjectType());
    }
}
