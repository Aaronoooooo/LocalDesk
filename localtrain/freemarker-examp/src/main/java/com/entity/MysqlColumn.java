package com.entity;

public class MysqlColumn {
    private String column;
    private String type;
    private boolean checked;
    private boolean disabled;

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return "MysqlColumn{" +
                "column='" + column + '\'' +
                ", type='" + type + '\'' +
                ", checked=" + checked +
                ", disabled=" + disabled +
                '}';
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public boolean isChecked() {
        return checked;
    }

    public void setChecked(boolean checked) {
        this.checked = checked;
    }

    public boolean isDisabled() {
        return disabled;
    }

    public void setDisabled(boolean disabled) {
        this.disabled = disabled;
    }

}
