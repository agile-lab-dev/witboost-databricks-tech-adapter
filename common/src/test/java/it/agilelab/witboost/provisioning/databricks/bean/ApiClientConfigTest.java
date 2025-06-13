package it.agilelab.witboost.provisioning.databricks.bean;

import static org.hibernate.validator.internal.util.Contracts.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.config.DatabricksAuthConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class ApiClientConfigTest {

    @Spy
    @InjectMocks
    private ApiClientConfig apiClientConfig;

    @Mock
    private ApiClient apiClient;

    @Mock
    private DatabricksConfig databricksConfig;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testCreateApiClient_Success() {
        ApiClientConfig.ApiClientConfigParams params = mock(ApiClientConfig.ApiClientConfigParams.class);
        when(params.getWorkspaceHost()).thenReturn("https://test.host");
        DatabricksAuthConfig databricksAuthConfig = mock(DatabricksAuthConfig.class);
        AzureAuthConfig azureAuthConfig = mock(AzureAuthConfig.class);

        when(params.getDatabricksAuthConfig()).thenReturn(databricksAuthConfig);
        when(params.getAzureAuthConfig()).thenReturn(azureAuthConfig);
        ApiClientConfig apiClientConfig = new ApiClientConfig();

        Object apiClient = apiClientConfig.createApiClient(params);

        assertNotNull(apiClient);
    }

    @Test
    public void testCreateApiClient_ExceptionThrown() {
        ApiClientConfig.ApiClientConfigParams params = mock(ApiClientConfig.ApiClientConfigParams.class);
        when(params.getWorkspaceHost()).thenThrow(new RuntimeException("Test Exception"));

        assertThrows(RuntimeException.class, () -> apiClientConfig.createApiClient(params));
    }

    @Test
    public void testBuildDatabricksConfig_Success() {
        ApiClientConfig.ApiClientConfigParams params = mock(ApiClientConfig.ApiClientConfigParams.class);
        DatabricksAuthConfig databricksAuthConfig = mock(DatabricksAuthConfig.class);
        AzureAuthConfig azureAuthConfig = mock(AzureAuthConfig.class);

        when(databricksAuthConfig.getAccountId()).thenReturn("testAccountId");
        when(azureAuthConfig.getTenantId()).thenReturn("testTenantId");
        when(azureAuthConfig.getClientId()).thenReturn("testClientId");
        when(azureAuthConfig.getClientSecret()).thenReturn("testClientSecret");

        DatabricksConfig config = apiClientConfig.buildDatabricksConfig(
                databricksAuthConfig, azureAuthConfig, "https://example.databricks.com");

        assertNotNull(config);
        assertEquals("https://example.databricks.com", config.getHost());
        assertEquals("testAccountId", config.getAccountId());
        assertEquals("testTenantId", config.getAzureTenantId());
        assertEquals("testClientId", config.getAzureClientId());
        assertEquals("testClientSecret", config.getAzureClientSecret());
    }
}
