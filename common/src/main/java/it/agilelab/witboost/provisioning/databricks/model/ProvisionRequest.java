package it.agilelab.witboost.provisioning.databricks.model;

public record ProvisionRequest<T>(DataProduct dataProduct, Component<T> component, Boolean removeData) {}
