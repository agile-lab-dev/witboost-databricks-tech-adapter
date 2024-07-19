package it.agilelab.witboost.provisioning.databricks.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.agilelab.witboost.provisioning.databricks.TestConfig;
import java.util.ArrayList;
import java.util.List;
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
public class TemplatesConfigTest {

    @Autowired
    private TemplatesConfig templatesConfig;

    @TestConfiguration
    static class TemplatesTestConfig {

        @Primary
        @Bean
        public TemplatesConfig primaryTemplatesConfig() {
            TemplatesConfig config = new TemplatesConfig();

            return config;
        }
    }

    @Test
    public void testConfigurationProperties() {
        assertEquals(
                "urn:dmb:utm:databricks-workload-dlt-template",
                templatesConfig.getDlt().get(0));
        assertEquals(1, templatesConfig.getDlt().size());

        assertEquals(
                "urn:dmb:utm:databricks-workload-job-template",
                templatesConfig.getJob().get(0));
        assertEquals(1, templatesConfig.getJob().size());

        List<String> dlt = new ArrayList<>();
        dlt.add("dlt1");
        dlt.add("dlt2");
        List<String> job = new ArrayList<>();
        job.add("job1");
        job.add("job2");

        templatesConfig.setJob(job);
        templatesConfig.setDlt(dlt);

        assertEquals("dlt1", templatesConfig.getDlt().get(0));
        assertEquals("dlt2", templatesConfig.getDlt().get(1));
        assertEquals(2, templatesConfig.getDlt().size());

        assertEquals("job1", templatesConfig.getJob().get(0));
        assertEquals("job2", templatesConfig.getJob().get(1));
        assertEquals(2, templatesConfig.getJob().size());
    }
}
