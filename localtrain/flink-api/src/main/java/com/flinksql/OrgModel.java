package com.flinksql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author
 * @Date 2019-11-14 13:24
 **/

public class OrgModel {

    public String id;

    public String orgId;

    public String name;

    public String orgTs;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOrgId() {
        return orgId;
    }

    public void setOrgId(String orgId) {
        this.orgId = orgId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOrgTs() {
        return orgTs;
    }

    public void setOrgTs(String orgTs) {
        this.orgTs = orgTs;
    }

    @Override
    public String toString() {
        return "OrgModel{" +
                "id='" + id + '\'' +
                ", orgId='" + orgId + '\'' +
                ", name='" + name + '\'' +
                ", orgTs='" + orgTs + '\'' +
                '}';
    }
}
