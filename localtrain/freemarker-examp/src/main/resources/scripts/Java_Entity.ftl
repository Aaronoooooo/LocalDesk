package ${packageName};

<#list imports as import>
    import ${import};
</#list>

public class ${className} {

<#list fields?keys as key>
    private ${fields[key].simpleName} ${key};

</#list>
<#list fields?keys as key>
    <#assign  fieldClass = fields[key].simpleName>
    public void set${key?cap_first}(${fieldClass} ${key}) {
    this.${key} = ${key};
    }

    public ${fieldClass} <#if fieldClass="boolean">is<#else>get</#if>${key?cap_first}() {
    return this.${key};
    }

</#list>

@Override
public String toString() {
final StringBuilder sb = new StringBuilder("[");
<#list fields?keys as key>
    sb.append("${key}:").append(${key}).append(";    ");
</#list>
sb.append("]");
return sb.toString();
}
}