package it.agilelab.witboost.provisioning.databricks.config;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "usecasetemplateid.workload")
public class TemplatesConfig {

    private List<String> job;
    private List<String> dlt;
}
