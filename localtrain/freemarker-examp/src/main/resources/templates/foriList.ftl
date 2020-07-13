for (int i = 0; i < ${list}.size(); i++) {
Integer in = ${list}.get(i);
<#include "if.ftl" />
else {
System.out.println("年龄不匹配");
}
}