package cn.flydiy.flyauto.${BASE.projectCode}.${BASE.moduleCode}.entity;

import cn.flydiy.cloud.data.entity.BaseEntity;
<#if BASE.isActiviti == 'true'>
    import cn.flydiy.cloud2.activiti.other.ActivitiState;
</#if>
import com.fasterxml.jackson.annotation.JsonFormat;
import io.swagger.annotations.ApiModelProperty;
<#list COLUMN_LIST as column>
    <#if column.dataType == 'ENUM' >import cn.flydiy.flyauto.${BASE.projectCode}.${BASE.moduleCode}.entity.enumeration.${column.javaType};</#if>
</#list>
import org.hibernate.annotations.GenericGenerator;
import javax.persistence.*;
import javax.validation.constraints.*;
import java.math.BigDecimal;
import java.util.Objects;
import java.time.Instant;

/**
* ${BASE.remark}
*/
@Entity
@Table(name = "${BASE.tableCode}")
public class ${BASE.classCode}Entity extends BaseEntity<${BASE.primaryKeyType}> {

private static final long serialVersionUID = 1L;

<#--属性列表-->
<#list COLUMN_LIST as column>
    <#if column.isPrimaryKey() == false>
        <#if column.isNotNull()>@NotNull</#if>
        <#if column.javaType == 'String' && column.dataMinLength?? && column.dataMaxLength??>@Size(min = ${column.dataMinLength}, max = ${column.dataMaxLength})</#if>
        <#if column.regExpression?? >@Pattern(regexp = "${column.regExpression}")</#if>
        <#if column.dataType == 'ENUM' >@Enumerated(EnumType.STRING)</#if>
    <#--<#if column.dataType == 'DATE' >@JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")</#if>-->
        @Column(name = "${column.columnCode}"<#if col.dataMaxLength?? && col.dataMaxLength gt 0>,length = ${column.dataMaxLength}</#if><#if column.isNotNull()>,nullable = false</#if><#if column.isUnique()>,unique = true</#if><#if column.dataPrecision?? && col.dataPrecision gt 0>,precision = ${column.dataLength},scale = ${column.dataPrecision}</#if>)
        @ApiModelProperty(value = "${column.attrName}")
        <#if column.attrCode == 'AUTH_STATE'>
            private ${column.javaType} ${column.attrCode} = ActivitiState.PAUTH;
        <#else >
            private ${column.javaType} ${column.attrCode};
        </#if>
    <#else >
        @Id
        <#if column.javaType == 'String'>
            @GeneratedValue(generator = "uuidGenerator")
            @GenericGenerator(name = "uuidGenerator", strategy = "uuid")
        <#else >
            @GeneratedValue(strategy = GenerationType.${column.generationType})<#-- 主键生成策略 -->
        </#if>
        @Column(name = "${column.columnCode}")
        @ApiModelProperty(value = "${column.attrName}")
        private ${column.javaType} ${column.attrCode};
    </#if>
</#list>

<#--Get/Set方法-->
<#list COLUMN_LIST as column>
    public ${column.javaType} get${column.attrCode?cap_first}() {
    return ${column.attrCode};
    }
    public void set${column.attrCode?cap_first}(${column.javaType} ${column.attrCode}) {
    this.${column.attrCode} = ${column.attrCode};
    }
</#list>


@Override
public boolean equals(Object o) {
if (this == o) {
return true;
}
if (o == null || getClass() != o.getClass()) {
return false;
}
${BASE.classCode}Entity ${BASE.classCode?uncap_first} = (${BASE.classCode}Entity) o;
if (${BASE.classCode?uncap_first}.getId() == null || getId() == null) {
return false;
}
return Objects.equals(getId(), ${BASE.classCode?uncap_first}.getId());
}

@Override
public int hashCode() {
return Objects.hashCode(getId());
}

@Override
public String toString() {
//TODO 模板再调整整数
return "ProjectEntity{" +
"id=" + getId() +
<#list COLUMN_LIST as column>
    <#if column.attrCode != 'id'>
        ", ${column.attrCode}='" + get${column.attrCode?cap_first}() + "'" +
    </#if>
</#list>
"}";
}
}
