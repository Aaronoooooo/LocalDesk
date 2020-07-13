import java.util.*;

public ${var.methResulValue}<${var.varCreateType}> ${var.methName}(<#if var.methNameInParamName?? && var.methNameInParamType??>${var.methNameInParamType} ${var.methNameInParamName}</#if>) {

//开始(创建List)
${var.methResulValue}<${var.varCreateType}> ${var.varCreateName} = new ArrayList<${var.varCreateType}>();
                <#list var.varCreateValue as varCreateValue >
                    ${var.varCreateName}.add("${varCreateValue}")
                </#list>
                <#--${varCreateName}.add("${varCreateValue}");-->

                // 判断1
                if (!${var.varCreateName}.<#list var.varIfMethod as varIfMethod>${varIfMethod}</#list>) {

                //循环
                for (int i = 0; i < ${var.varCreateName}.size(); i++) {
                ${var.varCreateType} ${var.varIfOutName} = ${var.varCreateName}.get(i);

                // 判断2
                if (${var.varIfOutName}.<#list var.varIfMethod as varIfMethod>${varIfMethod}</#list>("<#list var.varChangeValue as varChangeValue>${varChangeValue}</#list>")) {
                ${var.varCreateName}.<#list var.varExeMethod as varExeMethod>${varExeMethod}(i);</#list>
                } <#if (var.varChangeValue?size > 0)> <#list var.varChangeValue as varChangeValue>else if (${var.varIfOutName}.<#list var.varIfMethod as varIfMethod>${varIfMethod}</#list>("${varChangeValue}")) {
                    ${var.varCreateName}.<#list var.varExeMethod as varExeMethod>${varExeMethod}(i);</#list>
                </#list></#if>}
                }
                }
                }
                //结束
                <#if var.methResulValue?? && (var.methResulValue!="void")>
                return ${var.varCreateName};
                </#if>
                System.out.println(${var.varCreateName}.toString());
                }