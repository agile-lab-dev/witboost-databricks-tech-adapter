package it.agilelab.witboost.provisioning.databricks.principalsmapping;

import io.vavr.control.Try;

public interface MapperFactory<T> {

    /**
     * Creates a Mapper object that will be used to map subjects
     *
     * @param config the config object that might be needed to create the Mapper
     * @return the mapper
     */
    Try<Mapper> create(T config);

    /**
     * Creates a Mapper object with no config
     *
     * @return the mapper
     */
    default Try<Mapper> create() {
        return create(null);
    }

    /**
     * The {@code configIdentifier} defines the config key that the consumer will use to specify the configuration
     * of this plugin inside the `principalmappingplugin` block.
     * {@code
     * principalmappingplugin {
     *     mymapperid {
     *         foo: "bar"
     *     }
     * }
     * }
     *
     * @return the identifier
     */
    String configIdentifier();
}
