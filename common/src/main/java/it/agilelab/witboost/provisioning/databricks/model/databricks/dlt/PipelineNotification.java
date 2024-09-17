package it.agilelab.witboost.provisioning.databricks.model.databricks.dlt;

import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class PipelineNotification {

    private String mail;
    private List<String> alert;

    public PipelineNotification(String mail, List<String> alert) {
        this.mail = mail;
        this.alert = alert;
    }
}
