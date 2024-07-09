package it.agilelab.witboost.provisioning.databricks.bean;

import java.util.concurrent.ForkJoinPool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Spring configuration class for Azure Databricks manager bean.
 */
@Configuration
public class ForkJoinPoolBean {

    @Value("${forkjoin.parallelism}")
    private int parallelism;

    @Bean
    public ForkJoinPool forkJoinPool() {
        return new ForkJoinPool(parallelism);
    }
}
