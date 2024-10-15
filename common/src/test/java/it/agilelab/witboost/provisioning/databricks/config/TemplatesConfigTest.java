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
    private WorkloadTemplatesConfig workloadTemplatesConfig;

    @TestConfiguration
    static class TemplatesTestConfig {

        @Primary
        @Bean
        public WorkloadTemplatesConfig primaryTemplatesConfig() {
            WorkloadTemplatesConfig config = new WorkloadTemplatesConfig();

            return config;
        }
    }

    @Test
    public void testConfigurationProperties() {
        assertEquals(
                "urn:dmb:utm:databricks-workload-dlt-template",
                workloadTemplatesConfig.getDlt().get(0));
        assertEquals(1, workloadTemplatesConfig.getDlt().size());

        assertEquals(
                "urn:dmb:utm:databricks-workload-job-template",
                workloadTemplatesConfig.getJob().get(0));
        assertEquals(1, workloadTemplatesConfig.getJob().size());

        List<String> dlt = new ArrayList<>();
        dlt.add("dlt1");
        dlt.add("dlt2");
        List<String> job = new ArrayList<>();
        job.add("job1");
        job.add("job2");

        workloadTemplatesConfig.setJob(job);
        workloadTemplatesConfig.setDlt(dlt);

        assertEquals("dlt1", workloadTemplatesConfig.getDlt().get(0));
        assertEquals("dlt2", workloadTemplatesConfig.getDlt().get(1));
        assertEquals(2, workloadTemplatesConfig.getDlt().size());

        assertEquals("job1", workloadTemplatesConfig.getJob().get(0));
        assertEquals("job2", workloadTemplatesConfig.getJob().get(1));
        assertEquals(2, workloadTemplatesConfig.getJob().size());
    }
}
