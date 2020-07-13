import java.util.*;

public ${var.methResulValue} ${var.methName}(<#if var.methNameInParamName?? && var.methNameInParamType??>${var.methNameInParamType} ${var.methNameInParamName}</#if>) {

//开始(创建List)
List<${var.varCreateType}> ${var.varCreateName} = new ArrayList<${var.varCreateType}>();
<#list var.varCreateValue as varCreateValue>
    ${var.varCreateName}.add("${varCreateValue}")
</#list>

//判断1
if (!${var.varCreateName}.${var.varIfMethod1}()) {

//循环
for (int i = 0; i < ${var.varCreateName}.size(); i++) {
${var.varCreateType} ${var.varIfOutName} = ${var.varCreateName}.get(i);
//判断2
if (${var.varIfOutName}.${var.varIfMethod2}("<#list var.varChangeValue as varChangeValue>${varChangeValue}</#list>")) {

//修改
${var.varCreateName}.<#list var.varExeMethod as method>${method}(i);</#list>
        }
    }
}
//结束
<#if var.methResulValue?? && (var.methResulValue!="void")>
    return ${var.varCreateName};
</#if>
    System.out.println(${var.varCreateName}.toString());
}