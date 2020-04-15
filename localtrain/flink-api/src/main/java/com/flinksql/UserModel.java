package com.flinksql;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author
 * @Date 2019-11-14 13:24
 **/

public class UserModel {

    public String id;

    public String realName;

    public String userTs;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getRealName() {
        return realName;
    }

    public void setRealName(String realName) {
        this.realName = realName;
    }

    public String getUserTs() {
        return userTs;
    }

    public void setUserTs(String userTs) {
        this.userTs = userTs;
    }

    @Override
    public String toString() {
        return "UserModel{" +
                "id='" + id + '\'' +
                ", realName='" + realName + '\'' +
                ", userTs='" + userTs + '\'' +
                '}';
    }
}

