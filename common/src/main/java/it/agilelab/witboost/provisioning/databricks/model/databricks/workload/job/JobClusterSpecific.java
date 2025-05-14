package it.agilelab.witboost.provisioning.databricks.model.databricks.workload.job;

import com.databricks.sdk.service.compute.AzureAvailability;
import com.databricks.sdk.service.compute.RuntimeEngine;
import it.agilelab.witboost.provisioning.databricks.model.databricks.SparkConf;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class JobClusterSpecific {

    @NotBlank
    private String clusterSparkVersion;

    @NotBlank
    private String nodeTypeId;

    @Min(1L)
    private Long numWorkers;

    private Double spotBidMaxPrice;
    private Long firstOnDemand;
    private Boolean spotInstances;
    private AzureAvailability availability;
    private String driverNodeTypeId;
    private List<SparkConf> sparkConf;
    private List<DatabricksJobWorkloadSpecific.SparkEnvVar> sparkEnvVars;
    private RuntimeEngine runtimeEngine;
}
