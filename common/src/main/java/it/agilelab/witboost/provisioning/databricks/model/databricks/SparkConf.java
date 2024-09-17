package it.agilelab.witboost.provisioning.databricks.model.databricks;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class SparkConf {
    private String name;
    private String value;
}
