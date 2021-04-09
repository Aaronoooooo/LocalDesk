package ${packageName}

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.gree.model._
import org.slf4j.{Logger, LoggerFactory}

/**
* 将表数据转化为Json放入ES中
*/

object JsonBeanUtil {
    private val numberFormatUtil = new NumberFormatUtil()
    val logger: Logger = LoggerFactory.getLogger(JsonBeanUtil.getClass)
    //安装主表
    def getTblAzAssignLcLs(tblAzAssignLcLs: ${zhubiaoModule}): util.HashMap[String, Any] = {
        val json = new java.util.HashMap[String, Any]()
        try {
        <#list zhubiaoColumns as column>
        json.put("${column.columnCode}", tblAzAssignLcLs.${column.columnCode})
        </#list>
    } catch {
        case e: Exception => logger.error("同步安装主表出现异常" + e.getMessage)
    }
    json
    }
    
    
    //安装客户评价
    def getTblAzAssignSatisfaction(tblAzAssignSatisfaction: ${satisfactionModule}): util.HashMap[String, Any] = {
    val json = new java.util.HashMap[String, Any]()
    try {
        <#list satisfactionColumns as column>
        json.put("${column.columnCode}", tblAzAssignSatisfaction.${column.columnCode})
        </#list>
    } catch {
        case e: Exception => logger.error("同步安装客户评价表出现异常" + e.getMessage)
    }
    json
    }
    
    
    //安装工单明细
    def getTblAzAssignMx(tblAzAssignMx: ${assignModule}): util.HashMap[String, Any] = {
        val json = new java.util.HashMap[String, Any]()
    
    try {
        <#list assignColumns as column>
        json.put("${column.columnCode}", tblAzAssignMx.${column.columnCode})
        </#list>
    } catch {
        case e: Exception => logger.error("同步安装工单表出现异常" + e.getMessage)
    }
    json
    }
    
    
    //安装反馈明细
    def getTblAzAssignFkmx(tblAzAssignFkmx: ${fkmxModule}): util.HashMap[String, Any] = {
        val json = new java.util.HashMap[String, Any]()
    try {
        <#list fkmxColumns as column>
        json.put("${column.columnCode}", tblAzAssignFkmx.${column.columnCode})
        </#list>
    } catch {
        case e: Exception => logger.error("同步安装反馈明细表出现异常" + e.getMessage)
    }
    json
    }
    
    
    //用户预约改约
    def getTblAzAssignAppointment(tblAzAssignAppointment: ${appointmentModule}): util.HashMap[String, Any] = {
        val json = new java.util.HashMap[String, Any]()
    try {
        <#list appointmentColumns as column>
        json.put("${column.columnCode}", tblAzAssignAppointment.${column.columnCode})
        </#list>
    } catch {
        case e: Exception => logger.error("同步安装用户预约改约表出现异常" + e.getMessage)
    }
    json
    }
    
    //安装最大宽表
    def getMaxAnZhuangTable(maxKuanBiaoData: ${maxkuanbiaoModule}): util.HashMap[String, Any] = {
        this.synchronized {
            val json = new java.util.HashMap[String, Any]()
                try {
                    <#list maxkuanbiaoColumns as column>
                    json.put("${column.columnCode}", maxKuanBiaoData.${column.columnCode})
                    </#list>
        } catch {
            case e: Exception => logger.error("同步安装大宽表出现异常" + e.getMessage)
        }
        json
        }
    }
    
    //安装产品明细宽表
    def getAzCanPinMingXiTable(azChanPinMingXiKuanBiaoData: ${chanpinmingxiModule}): util.HashMap[String, Any] = {
    this.synchronized {
        val json = new java.util.HashMap[String, Any]()
        try {
            <#list chanpinmingxiColumns as column>
            json.put("${column.columnCode}", azChanPinMingXiKuanBiaoData.${column.columnCode})
            </#list>
            
        } catch {
            case e: Exception => logger.error("同步安装产品明细宽表出现异常" + e.getMessage)
        }
        json
        }
    }
    
    //安装财务导出宽表
    def getCaiWuExportWidenTable(azCaiWuDaoChuData: ${caiwuModule}): util.HashMap[String, Any] = {
    this.synchronized {
        val json = new java.util.HashMap[String, Any]()
        try {
            <#list caiwuColumns as column>
            json.put("${column.columnCode}", azCaiWuDaoChuData.${column.columnCode})
            </#list>
            
        } catch {
            case e: Exception => logger.error("同步安装财务导出宽表出现异常" + e.getMessage)
        }
        json
        }
    }

}
