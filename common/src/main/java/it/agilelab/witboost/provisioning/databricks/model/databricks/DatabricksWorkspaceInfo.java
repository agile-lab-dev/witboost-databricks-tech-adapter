package it.agilelab.witboost.provisioning.databricks.model.databricks;

import java.util.Objects;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class DatabricksWorkspaceInfo {
    private String name;
    private String id;
    private String url;

    public DatabricksWorkspaceInfo(String name, String id, String url) {
        this.name = name;
        this.id = id;
        this.url = url;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabricksWorkspaceInfo that = (DatabricksWorkspaceInfo) o;
        return Objects.equals(name, that.name) && Objects.equals(id, that.id) && Objects.equals(url, that.url);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, id, url);
    }

    @Override
    public String toString() {
        return "DatabricksWorkspaceInfo{" + "name='"
                + name + '\'' + ", id='"
                + id + '\'' + ", url='"
                + url + '\'' + '}';
    }
}
