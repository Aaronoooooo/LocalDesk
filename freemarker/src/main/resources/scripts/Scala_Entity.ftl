package ${packageName}

<#list imports as import>
import ${import};
</#list>

case class ${className} (

<#list fields?keys as key>
    ${key}: ${fields[key].simpleName}<#if key_has_next>,</#if>

</#list>
)