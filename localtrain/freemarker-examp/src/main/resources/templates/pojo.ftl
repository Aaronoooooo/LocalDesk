package ${package}.pojo;

import java.io.Serializable;
import java.util.Date;

/**
* ${table.comment}
*/
public class ${table.className} implements Serializable{
//生成私有属性
<#list table.columns as col>
    //${col.comment}
    private ${col.javaType} ${col.fieldName};
</#list>

//生成getter、setter方法
<#list table.columns as col>
    public void set${col.upperCaseColumnName} (${col.javaType} ${col.fieldName}){
    this.${col.fieldName} =${col.fieldName};
    }

    public ${col.javaType} get${col.upperCaseColumnName} (){
    return this.${col.fieldName};
    }
</#list>
}