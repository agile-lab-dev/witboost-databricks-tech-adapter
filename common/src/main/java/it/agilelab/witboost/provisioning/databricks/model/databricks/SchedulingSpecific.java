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

    @NotNull
    private Boolean enableScheduling;

    @NotNull(groups = {MandatoryFields.class})
    private String cronExpression;

    @NotNull(groups = {MandatoryFields.class})
    private String javaTimezoneId;

    @Override
    public String toString() {
        return "SchedulingSpecific(enableScheduling=" + enableScheduling + ", cronExpression="
                + cronExpression + ", javaTimezoneId="
                + javaTimezoneId + ')';
    }

    //    @Override
    //    public boolean equals(Object o) {
    //        if (this == o) return true;
    //        if (o == null || getClass() != o.getClass()) return false;
    //        SchedulingSpecific that = (SchedulingSpecific) o;
    //        return enableScheduling.equals(that.enableScheduling)
    //                && Objects.equals(cronExpression, that.cronExpression)
    //                && Objects.equals(javaTimezoneId, that.javaTimezoneId);
    //    }

}
