package com.elasticdsl.es;

import java.util.List;
import java.util.Objects;

public class StringFilter<C> extends Filter<String> {
    private static final long serialVersionUID = 1L;
    private String contains;
    private String startsWith;
    private String endsWith;
    private String equalsOrIsNull;
    private boolean appendEmptyCriteria = false;
    private C c;

    public StringFilter() {
    }

    public StringFilter(C c) {
        this.c = c;
    }

    public C and() {
        return this.c;
    }

    public C end() {
        return this.c;
    }

    public String getContains() {
        return this.contains;
    }

    public StringFilter setContains(String contains) {
        this.contains = contains;
        return this;
    }

    public String getStartsWith() {
        return this.startsWith;
    }

    public StringFilter setStartsWith(String startsWith) {
        this.startsWith = startsWith;
        return this;
    }

    public String getEndsWith() {
        return this.endsWith;
    }

    public StringFilter setEndsWith(String endsWith) {
        this.endsWith = endsWith;
        return this;
    }

    public String getEqualsOrIsNull() {
        return this.equalsOrIsNull;
    }

    public void setEqualsOrIsNull(String equalsOrIsNull) {
        this.equalsOrIsNull = equalsOrIsNull;
    }

    public boolean isAppendEmptyCriteria() {
        return this.appendEmptyCriteria;
    }

    public void setAppendEmptyCriteria(boolean appendEmptyCriteria) {
        this.appendEmptyCriteria = appendEmptyCriteria;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else if (o != null && this.getClass() == o.getClass()) {
            if (!super.equals(o)) {
                return false;
            } else {
                StringFilter that = (StringFilter)o;
                return Objects.equals(this.contains, that.contains) && Objects.equals(this.startsWith, that.startsWith) && Objects.equals(this.endsWith, that.endsWith);
            }
        } else {
            return false;
        }
    }

    public int hashCode() {
        return Objects.hash(new Object[]{super.hashCode(), this.contains, this.startsWith, this.endsWith});
    }

    public String toString() {
        return this.getFilterName() + " [" + (this.getContains() != null ? "contains=" + this.getContains() + ", " : "") + (this.getStartsWith() != null ? "startsWith=" + this.getStartsWith() + ", " : "") + (this.getEndsWith() != null ? "endsWith=" + this.getEndsWith() + ", " : "") + (this.getEquals() != null ? "equals=" + (String)this.getEquals() + ", " : "") + (this.getNotEquals() != null ? "notEquals=" + (String)this.getNotEquals() + ", " : "") + (this.getSpecified() != null ? "specified=" + this.getSpecified() : "") + (this.getEqualsOrIsNull() != null ? "equalsOrIsNull=" + this.getEqualsOrIsNull() : "") + "appendEmptyCriteria=" + this.isAppendEmptyCriteria() + "]";
    }

    public StringFilter<C> contains(String contains) {
        this.setContains(contains);
        return this;
    }

    public StringFilter<C> startsWith(String startsWith) {
        this.setStartsWith(startsWith);
        return this;
    }

    public StringFilter<C> endsWith(String endsWith) {
        this.setEndsWith(endsWith);
        return this;
    }

    public StringFilter<C> equals(String equals) {
        this.setEquals(equals);
        return this;
    }

    public StringFilter<C> notEquals(String notEquals) {
        this.setNotEquals(notEquals);
        return this;
    }

    public StringFilter<C> specified(Boolean specified) {
        this.setSpecified(specified);
        return this;
    }

    public StringFilter<C> in(List<String> in) {
        this.setIn(in);
        return this;
    }

    public StringFilter<C> appendEmptyCriteria(boolean appendEmptyCriteria) {
        this.setAppendEmptyCriteria(appendEmptyCriteria);
        return this;
    }
}