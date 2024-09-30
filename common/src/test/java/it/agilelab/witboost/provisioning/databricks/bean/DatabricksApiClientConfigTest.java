package it.agilelab.witboost.provisioning.databricks.bean;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import it.agilelab.witboost.provisioning.databricks.bean.params.ApiClientConfigParams;
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
public class DatabricksApiClientConfigTest {

    @Spy
    @InjectMocks
    private DatabricksApiClientConfig databricksApiClientConfig;

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
        ApiClientConfigParams params = mock(ApiClientConfigParams.class);
        DatabricksAuthConfig databricksAuthConfig = mock(DatabricksAuthConfig.class);
        AzureAuthConfig azureAuthConfig = mock(AzureAuthConfig.class);

        when(params.getDatabricksAuthConfig()).thenReturn(databricksAuthConfig);
        when(params.getAzureAuthConfig()).thenReturn(azureAuthConfig);
        when(params.getWorkspaceHost()).thenReturn("https://test.databricks.com");

        doReturn(databricksConfig)
                .when(databricksApiClientConfig)
                .buildDatabricksConfig(any(DatabricksAuthConfig.class), any(AzureAuthConfig.class), any(String.class));

        ApiClient result = databricksApiClientConfig.createApiClient(params);

        verify(databricksApiClientConfig, times(1))
                .buildDatabricksConfig(databricksAuthConfig, azureAuthConfig, "https://test.databricks.com");
    }

    @Test
    public void testCreateApiClient_ExceptionThrown() {
        ApiClientConfigParams params = mock(ApiClientConfigParams.class);
        when(params.getWorkspaceHost()).thenThrow(new RuntimeException("Test Exception"));

        assertThrows(RuntimeException.class, () -> databricksApiClientConfig.createApiClient(params));
    }
}