package it.agilelab.witboost.provisioning.databricks.model.databricks.dlt;

import com.databricks.sdk.service.pipelines.PipelineClusterAutoscaleMode;
import it.agilelab.witboost.provisioning.databricks.model.databricks.SparkConf;
import it.agilelab.witboost.provisioning.databricks.service.validation.dlt.ValidDLTClusterSpecific;
import jakarta.validation.constraints.NotBlank;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@ValidDLTClusterSpecific
public class DLTClusterSpecific {

    private PipelineClusterAutoscaleMode mode;

    private Long minWorkers;
    private Long maxWorkers;
    private Long numWorkers;

    @NotBlank
    private String workerType; // node_type_id

    @NotBlank
    private String driverType; // driver_node_type_id

    @NotBlank
    private String policyId;

    private List<SparkConf> sparkConf;
    private Map<@NotBlank String, @NotBlank String> tags;
}
