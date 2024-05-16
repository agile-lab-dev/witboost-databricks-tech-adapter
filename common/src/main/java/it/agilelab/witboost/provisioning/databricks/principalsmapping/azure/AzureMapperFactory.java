package it.agilelab.witboost.provisioning.databricks.principalsmapping.azure;

import io.vavr.control.Try;
import it.agilelab.witboost.provisioning.databricks.config.AzureAuthConfig;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.Mapper;
import it.agilelab.witboost.provisioning.databricks.principalsmapping.MapperFactory;

public class AzureMapperFactory implements MapperFactory<AzureAuthConfig> {

    @Override
    public Try<Mapper> create(AzureAuthConfig azureAuthConfig) {
        return AzureClientFactory.getClient(azureAuthConfig)
                .flatMap(azureClient -> Try.success(new AzureMapper(azureClient)));
    }

    @Override
    public String configIdentifier() {
        return "azure";
    }
}
