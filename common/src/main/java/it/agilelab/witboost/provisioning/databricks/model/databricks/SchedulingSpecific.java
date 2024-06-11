package it.agilelab.witboost.provisioning.databricks.model.databricks;

import it.agilelab.witboost.provisioning.databricks.model.MandatoryFields;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class SchedulingSpecific {

    @NotNull(groups = {MandatoryFields.class})
    private String cronExpression;

    @NotNull(groups = {MandatoryFields.class})
    private String javaTimezoneId;

    @Override
    public String toString() {
        return "SchedulingSpecific(cronExpression=" + cronExpression + ", javaTimezoneId=" + javaTimezoneId + ')';
    }
}
