package it.agilelab.witboost.provisioning.databricks.model.databricks.workload.dlt;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import it.agilelab.witboost.provisioning.databricks.model.databricks.workload.DatabricksWorkloadSpecific;
import jakarta.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@Valid
@JsonIgnoreProperties(ignoreUnknown = true)
public class DatabricksDLTWorkloadSpecific extends DatabricksWorkloadSpecific {

    @NotBlank
    private String pipelineName;

    @NotNull
    private ProductEdition productEdition;

    @NotNull
    private Boolean continuous;

    // Paths of notebooks to be executed
    @NotNull
    private List<@NotBlank String> notebooks;

    private List<@NotBlank String> files;

    // Can be null when the workspace creation is disabled
    @Nullable
    private String metastore;

    @NotBlank
    private String catalog;

    private String target;

    @NotNull
    private Boolean photon;

    private List<PipelineNotification> notifications;

    @NotNull
    private PipelineChannel channel;

    @NotNull
    private DLTClusterSpecific cluster;

    /**
     * Represents the edition of a product used in the context of Databricks Delta Live Tables (DLT).
     * The edition determines the set of features or capabilities made available for the workload.
     */
    public enum ProductEdition {
        ADVANCED("advanced"),
        CORE("core"),
        PRO("pro");

        private final String value;

        ProductEdition(String value) {
            this.value = value;
        }

        public String getValue() {
            return value.toLowerCase();
        }
    }

    /**
     * Represents a notification configuration for a Databricks pipeline.
     * This class contains information about the email address to notify
     * and the list of alert conditions or triggers.
     */
    @Getter
    @Setter
    @NoArgsConstructor
    public static class PipelineNotification {

        private String mail;
        private List<String> alert;

        public PipelineNotification(String mail, List<String> alert) {
            this.mail = mail;
            this.alert = alert;
        }
    }

    /**
     * Represents the channel type for a pipeline in a Databricks Delta Live Tables (DLT) workload.
     * A channel determines the state or version of the pipeline to be used during execution.
     */
    public enum PipelineChannel {
        PREVIEW("preview"),
        CURRENT("current");

        private final String value;

        PipelineChannel(String value) {
            this.value = value;
        }

        public String getValue() {
            return value.toLowerCase();
        }
    }
}
