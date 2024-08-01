package it.agilelab.witboost.provisioning.databricks.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "misc")
public record MiscConfig(String developmentEnvironmentName) {}
