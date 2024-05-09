package it.agilelab.witboost.provisioning.databricks.model.databricks;

import it.agilelab.witboost.provisioning.databricks.model.Specific;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DatabricksWorkloadSpecific extends Specific {
    private String workspace;
    private String jobName;

    private String description;
    private String gitReference;
    private String gitReferenceType;
    private String gitPath;
    private String gitRepo;

    private Boolean enableScheduling;
    private Optional<String> cronExpression;
    private Optional<String> javaTimezoneId;

    private String clusterSparkVersion;
    private String nodeTypeId;
    private int numWorkers;
    private Optional<Integer> spotBidMaxPrice;
    private Optional<Integer> firstOnDemand;
    private Optional<Boolean> spotInstances;
    private Optional<String> availability;
    private Optional<String> driverNodeTypeId;
    private Optional<List<String>> sparkConf;
    private Optional<List<String>> sparkEnvVars;
    private Optional<String> runtimeEngine;
}
