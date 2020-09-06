package com.elasticdsl.es;

import java.util.Objects;

public class RangeFilter<FIELD_TYPE extends Comparable<? super FIELD_TYPE>> extends Filter<FIELD_TYPE> {
    private static final long serialVersionUID = 1L;
    private FIELD_TYPE greaterThan;
    private FIELD_TYPE lessThan;
    private FIELD_TYPE greaterOrEqualThan;
    private FIELD_TYPE lessOrEqualThan;

    public RangeFilter() {
    }

    public FIELD_TYPE getGreaterThan() {
        return this.greaterThan;
    }

    public RangeFilter<FIELD_TYPE> setGreaterThan(FIELD_TYPE greaterThan) {
        this.greaterThan = greaterThan;
        return this;
    }

    public FIELD_TYPE getGreaterOrEqualThan() {
        return this.greaterOrEqualThan;
    }

    public RangeFilter<FIELD_TYPE> setGreaterOrEqualThan(FIELD_TYPE greaterOrEqualThan) {
        this.greaterOrEqualThan = greaterOrEqualThan;
        return this;
    }

    public FIELD_TYPE getLessThan() {
        return this.lessThan;
    }

    public RangeFilter<FIELD_TYPE> setLessThan(FIELD_TYPE lessThan) {
        this.lessThan = lessThan;
        return this;
    }

    public FIELD_TYPE getLessOrEqualThan() {
        return this.lessOrEqualThan;
    }

    public RangeFilter<FIELD_TYPE> setLessOrEqualThan(FIELD_TYPE lessOrEqualThan) {
        this.lessOrEqualThan = lessOrEqualThan;
        return this;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            if (!super.equals(o)) {
                return false;
            } else {
                RangeFilter<?> that = (RangeFilter)o;
                return Objects.equals(this.greaterThan, that.greaterThan) && Objects.equals(this.lessThan, that.lessThan) && Objects.equals(this.greaterOrEqualThan, that.greaterOrEqualThan) && Objects.equals(this.lessOrEqualThan, that.lessOrEqualThan);
            }
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{super.hashCode(), this.greaterThan, this.lessThan, this.greaterOrEqualThan, this.lessOrEqualThan});
    }

    public String toString() {
        return this.getFilterName() + " [" + (this.getGreaterThan() != null ? "greaterThan=" + this.getGreaterThan() + ", " : "") + (this.getGreaterOrEqualThan() != null ? "greaterOrEqualThan=" + this.getGreaterOrEqualThan() + ", " : "") + (this.getLessThan() != null ? "lessThan=" + this.getLessThan() + ", " : "") + (this.getLessOrEqualThan() != null ? "lessOrEqualThan=" + this.getLessOrEqualThan() + ", " : "") + (this.getEquals() != null ? "equals=" + this.getEquals() + ", " : "") + (this.getSpecified() != null ? "specified=" + this.getSpecified() + ", " : "") + (this.getIn() != null ? "in=" + this.getIn() : "") + "]";
    }
}
