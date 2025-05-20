package it.agilelab.witboost.provisioning.databricks.model.databricks.workload;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Represents a Spark environment variable configuration for a Databricks workload (job or pipeline).
 * Fields:
 * - name: The name of the Spark environment variable.
 * - value: The value assigned to the Spark environment variable.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class SparkEnvVar {
    private String name;
    private String value;
}
