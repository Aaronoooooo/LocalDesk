package cn.flydiy.module;

import java.io.Serializable;

/**
 * 字段信息
 * Created by admin on 2017/2/23.
 */
public class ColumnInfo implements Serializable {
    private String columnCode;

    public String getColumnCode() {
        return columnCode;
    }

    public void setColumnCode(String columnCode) {
        this.columnCode = columnCode;
    }
}
