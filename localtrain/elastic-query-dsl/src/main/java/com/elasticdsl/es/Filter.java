package com.elasticdsl.es;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

public class Filter<FIELD_TYPE> implements Serializable {
    private static final long serialVersionUID = 1L;
    private FIELD_TYPE equals;
    private FIELD_TYPE notEquals;
    private Boolean specified;
    private List<FIELD_TYPE> in;

    public Filter() {
    }

    public FIELD_TYPE getEquals() {
        return this.equals;
    }

    public Filter<FIELD_TYPE> setEquals(FIELD_TYPE equals) {
        this.equals = equals;
        return this;
    }

    public FIELD_TYPE getNotEquals() {
        return this.notEquals;
    }

    public Filter<FIELD_TYPE> setNotEquals(FIELD_TYPE notEquals) {
        this.notEquals = notEquals;
        return this;
    }

    public Boolean getSpecified() {
        return this.specified;
    }

    public Filter<FIELD_TYPE> setSpecified(Boolean specified) {
        this.specified = specified;
        return this;
    }

    public List<FIELD_TYPE> getIn() {
        return this.in;
    }

    public Filter<FIELD_TYPE> setIn(List<FIELD_TYPE> in) {
        this.in = in;
        return this;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            Filter<?> filter = (Filter)o;
            return Objects.equals(this.equals, filter.equals) && Objects.equals(this.notEquals, filter.notEquals) && Objects.equals(this.specified, filter.specified) && Objects.equals(this.in, filter.in);
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{this.equals, this.notEquals, this.specified, this.in});
    }

    public String toString() {
        return this.getFilterName() + " [" + (this.getEquals() != null ? "equals=" + this.getEquals() + ", " : "") + (this.getNotEquals() != null ? "notEquals=" + this.getNotEquals() + ", " : "") + (this.getIn() != null ? "in=" + this.getIn() + ", " : "") + (this.getSpecified() != null ? "specified=" + this.getSpecified() : "") + "]";
    }

    protected String getFilterName() {
        return this.getClass().getSimpleName();
    }
}
