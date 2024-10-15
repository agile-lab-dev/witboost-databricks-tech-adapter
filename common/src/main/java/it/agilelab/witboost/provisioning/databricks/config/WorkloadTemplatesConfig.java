package it.agilelab.witboost.provisioning.databricks.config;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Getter
@Setter
@Component
@ConfigurationProperties(prefix = "usecasetemplateid.workload")
public class WorkloadTemplatesConfig {

    private List<String> job;
    private List<String> dlt;
    private List<String> workflow;
}
