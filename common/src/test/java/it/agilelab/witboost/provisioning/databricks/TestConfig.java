package it.agilelab.witboost.provisioning.databricks;

import com.azure.resourcemanager.AzureResourceManager;
import it.agilelab.witboost.provisioning.databricks.client.RepoManager;
import org.mockito.Mockito;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class TestConfig {

    @MockBean
    AzureResourceManager azureResourceManager;

    @Bean
    @Primary
    public RepoManager repoManager() {
        return Mockito.mock(RepoManager.class);
    }
}
