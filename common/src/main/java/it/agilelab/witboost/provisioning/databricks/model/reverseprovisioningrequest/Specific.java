package it.agilelab.witboost.provisioning.databricks.model.reverseprovisioningrequest;

import com.databricks.sdk.service.jobs.Job;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Specific {

    private String workspace;
    private Job workflow;
}
