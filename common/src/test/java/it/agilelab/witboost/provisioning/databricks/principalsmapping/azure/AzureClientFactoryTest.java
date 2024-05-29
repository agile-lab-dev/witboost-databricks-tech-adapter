// package it.agilelab.witboost.provisioning.databricks.principalsmapping.azure;
//
// import static org.junit.jupiter.api.Assertions.*;
// import static org.mockito.ArgumentMatchers.anyString;
// import static org.mockito.Mockito.*;
//
// import com.azure.identity.ClientSecretCredential;
// import com.azure.identity.ClientSecretCredentialBuilder;
// import com.microsoft.graph.serviceclient.GraphServiceClient;
// import io.vavr.control.Try;
// import it.agilelab.witboost.provisioning.databricks.TestConfig;
// import it.agilelab.witboost.provisioning.databricks.config.AzurePermissionsConfig;
// import org.junit.jupiter.api.Test;
// import org.mockito.MockedConstruction;
// import org.springframework.beans.factory.annotation.Autowired;
// import org.springframework.boot.test.context.SpringBootTest;
// import org.springframework.context.annotation.Bean;
// import org.springframework.context.annotation.Configuration;
// import org.springframework.context.annotation.Import;
//
// @SpringBootTest
// @Import(TestConfig.class)
// public class AzureClientFactoryTest {
//
//    @Autowired
//    private AzurePermissionsConfig azurePermissionsConfig;
//
//    @Configuration
//    static class TestConfig {
//
//        @Bean
//        public AzurePermissionsConfig azurePermissionsConfig() {
//            AzurePermissionsConfig config = new AzurePermissionsConfig();
//            config.setAuth_clientId("testClientId");
//            config.setAuth_tenantId("testTenantId");
//            config.setAuth_clientSecret("testClientSecret");
//            return config;
//        }
//    }
//
//    @Test
//    public void testGetClient_Success() {
//        try (MockedConstruction<ClientSecretCredentialBuilder> mockedBuilder =
//                        mockConstruction(ClientSecretCredentialBuilder.class, (mock, context) -> {
//                            when(mock.clientId(anyString())).thenReturn(mock);
//                            when(mock.tenantId(anyString())).thenReturn(mock);
//                            when(mock.clientSecret(anyString())).thenReturn(mock);
//                            ClientSecretCredential credential = mock(ClientSecretCredential.class);
//                            when(mock.build()).thenReturn(credential);
//                        });
//                MockedConstruction<GraphServiceClient> mockedClient = mockConstruction(GraphServiceClient.class)) {
//
//            Try<AzureClient> result = AzureClientFactory.getClient(azurePermissionsConfig);
//
//            assertTrue(result.isSuccess());
//            assertNotNull(result.get());
//            assertTrue(result.get() instanceof AzureGraphClient);
//        }
//    }
//
//    @Test
//    public void testGetClient_Failure() {
//        try (MockedConstruction<ClientSecretCredentialBuilder> mockedBuilder =
//                mockConstruction(ClientSecretCredentialBuilder.class, (mock, context) -> {
//                    when(mock.clientId(anyString())).thenReturn(mock);
//                    when(mock.tenantId(anyString())).thenReturn(mock);
//                    when(mock.clientSecret(anyString())).thenReturn(mock);
//                    when(mock.build()).thenThrow(new RuntimeException("Failed to build credential"));
//                })) {
//
//            Try<AzureClient> result = AzureClientFactory.getClient(azurePermissionsConfig);
//
//            assertTrue(result.isFailure());
//            assertTrue(result.getCause() instanceof RuntimeException);
//            assertEquals("Failed to build credential", result.getCause().getMessage());
//        }
//    }
// }
