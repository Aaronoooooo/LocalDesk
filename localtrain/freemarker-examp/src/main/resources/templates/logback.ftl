${r'<?xml version="1.0" encoding="UTF-8"?>
<configuration debug="false">
    <!--<property name="log.base" value="${log_dir}/" />-->
    <contextName>azkuanbiao_log</contextName>
    <property name="LOG_HOME" value="./logs/flink/azkuanbiao" /><!--配置日志输出路径-->
    <!-- 控制台输出 -->
    <appender name="STDOUT" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n
            </pattern>
        </encoder>
    </appender>

    <!-- 按照每天生成错误日志文件 -->
    <appender name="ERRORFILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <Prudent>true</Prudent>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <!--日志文件输出的文件名 -->
            <FileNamePattern>${LOG_HOME}gree_new_anzhuangkuanbiao_error-%d{yyyy-MM-dd}.log</FileNamePattern>
            <!--日志文件保留天数 -->
            <MaxHistory>5</MaxHistory>
        </rollingPolicy>
        <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
            <!--格式化输出：%d表示日期，%thread表示线程名，%-5level：级别从左显示5个字符宽度%msg：日志消息，%n是换行符 -->
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n
            </pattern>
        </encoder>
        <filter class="ch.qos.logback.classic.filter.LevelFilter"><!-- 只打印错误日志 -->
            <level>ERROR</level>
            <onMatch>ACCEPT</onMatch>
            <onMismatch>DENY</onMismatch>
        </filter>
    </appender>

  <!--  &lt;!&ndash; 按照每天生成警告日志文件 &ndash;&gt;
    <appender name="WARNFILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <Prudent>true</Prudent>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            &lt;!&ndash;日志文件输出的文件名 &ndash;&gt;
            <FileNamePattern>${log.base}gree_new_retail_warn-%d{yyyy-MM-dd}.log</FileNamePattern>
            &lt;!&ndash;日志文件保留天数 &ndash;&gt;
            <MaxHistory>5</MaxHistory>
        </rollingPolicy>
        <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
            &lt;!&ndash;格式化输出：%d表示日期，%thread表示线程名，%-5level：级别从左显示5个字符宽度%msg：日志消息，%n是换行符 &ndash;&gt;
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n
            </pattern>
        </encoder>
        <filter class="ch.qos.logback.classic.filter.LevelFilter">&lt;!&ndash; 只打印警告日志 &ndash;&gt;
            <level>WARN</level>
            <onMatch>ACCEPT</onMatch>
            <onMismatch>DENY</onMismatch>
        </filter>
    </appender>-->

    <!-- 按照每天生成INFO日志文件 -->
    <appender name="INFOFILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <Prudent>true</Prudent>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            <!--日志文件输出的文件名 -->
            <FileNamePattern>${LOG_HOME}gree_new_anzhuangkuanbiao-%d{yyyy-MM-dd}.log</FileNamePattern>
            <!--日志文件保留天数 -->
            <MaxHistory>5</MaxHistory>
        </rollingPolicy>
        <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
            <!--格式化输出：%d表示日期，%thread表示线程名，%-5level：级别从左显示5个字符宽度%msg：日志消息，%n是换行符 -->
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n
            </pattern>
        </encoder>
        <filter class="ch.qos.logback.classic.filter.LevelFilter"><!-- 只打印INFO日志 -->
            <level>INFO</level>
            <onMatch>ACCEPT</onMatch>
            <onMismatch>DENY</onMismatch>
        </filter>
    </appender>

    <!-- 按照每天生成DEBUG日志文件 -->
    <!--<appender name="DEBUGFILE" class="ch.qos.logback.core.rolling.RollingFileAppender">
        <Prudent>true</Prudent>
        <rollingPolicy class="ch.qos.logback.core.rolling.TimeBasedRollingPolicy">
            &lt;!&ndash;日志文件输出的文件名 &ndash;&gt;
            <FileNamePattern>${log.base}gree_new_retail_debug-%d{yyyy-MM-dd}.log</FileNamePattern>
            &lt;!&ndash;日志文件保留天数 &ndash;&gt;
            <MaxHistory>5</MaxHistory>
        </rollingPolicy>
        <encoder class="ch.qos.logback.classic.encoder.PatternLayoutEncoder">
            &lt;!&ndash;格式化输出：%d表示日期，%thread表示线程名，%-5level：级别从左显示5个字符宽度%msg：日志消息，%n是换行符 &ndash;&gt;
            <pattern>%d{yyyy-MM-dd HH:mm:ss.SSS} [%thread] %-5level %logger{50} - %msg%n
            </pattern>
        </encoder>
        <filter class="ch.qos.logback.classic.filter.LevelFilter">&lt;!&ndash; 只打印DEBUG日志 &ndash;&gt;
            <level>DEBUG</level>
            <onMatch>ACCEPT</onMatch>
            <onMismatch>DENY</onMismatch>
        </filter>
    </appender>-->

    <!-- 日志输出级别 -->
    <root level="INFO">
        <appender-ref ref="STDOUT" />
        <appender-ref ref="ERRORFILE" />
      <!--  <appender-ref ref="WARNFILE" />-->
        <appender-ref ref="INFOFILE" />
     <!--   <appender-ref ref="DEBUGFILE" />-->
    </root>

</configuration>
'}