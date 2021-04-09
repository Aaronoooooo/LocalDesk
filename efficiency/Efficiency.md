- 删除30天之前的旧文件  
```shell script
find /var/log/ -type f -mtime +30 -exec rm -f {} \;
```

- 文件内全部替换vim/vi  
```shell script
:%s#abc#123#g (如文件内有#,可用/替换,:%s/abc/123/g)

删除所有行vim/vi		复制所有行	复制所选行	向后删除		粘贴  
:.,$d || ggdG		ggyG		ggvG		dG		p
```

- windows如何将多个txt或html文档合并成一个文件?  
```shell script
# 适用所有文本文档格式，包括 .txt / .html / .htm / .php / .md / .js / .css 等等……
- 将需要合并的文件放一个文件夹下
- 按快捷键 Win+R 打开运行窗口,输入 cmd,回车进入cmd
> type C:\Users\crao\Downloads\ICE\Perf\*.txt>>C:\Users\crao\Downloads\ICE\Perf\comp.txt
```

- yum安装软件时,提示No package netstat available.
```shell script
step1:来查询这个命令在哪一个包中
> yum search
================================ Matched: netstat =======================================
dstat.noarch : Versatile resource statistics tool
net-snmp.x86_64 : A collection of SNMP protocol tools and libraries
net-tools.x86_64 : Basic networking tools

step2:选择安装Matched的包
> yum -y install net-tools
```
 
- 服务器配置
```shell script
- 永久修改主机名称centos7
> hostnamectl set-hostname "hostname"
- 添加用户
> useradd es #创建用户
> passwd es #修改密码
```

- Nginx启动命令:
```shell script
> $NGINX_HOME/nginx  #启动
> $NGINX_HOME/nginx -s stop #停止
> $NGINX_HOME/nginx  -s reload #热加载重启
```
- shell脚本kill所有进程
```shell script
> ps -ef|grep **the name of process**|grep -v grep|awk '{print $2}'|xargs kill -9
> ps -ef|grep **the name of process**|grep -v grep|cut -c 9-15|xargs kill -9
```
	
- redis
```shell script
# 批量删除key
> redis-cli keys "*" | xargs redis-cli del
# 登录redis客户端
> redis-cli -a `cat /data/redis/redis.conf | grep requirepass | awk '{print $2}'` 	
```
- Kafka-Shell操作
```shell script
# 删除  
/opt/module/kafka_2.11-2.2.1/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic chat-records

# 创建  
/opt/module/kafka_2.11-2.2.1/bin/kafka-topics.sh --zookeeper localhost:2181 --create --topic chat-records --partitions 1 --replication-factor 1 --config max.message.bytes=64000 --config flush.messages=1

# 查看  
/opt/module/kafka_2.11-2.2.1/bin/kafka-topics.sh --list --zookeeper mobikok-bigdata101:2181
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --list --zookeeper node30:2181/alonekafka

# 生产  
/usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list localhost:6669 --topic chat-records

/opt/module/kafka_2.11-2.2.1/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic chat-records

# 消费  
/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper node30:2181/alonekafka --topic topic_ad_send_new --max-messages 1
/usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper node30:2181/alonekafka  --bootstrap-server node30:9092 --topic chat-records  --from-beginning

/opt/module/kafka_2.11-2.2.1//bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic chat-records --from-beginning
/opt/module/kafka_2.11-2.2.1/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic chat-records --from-beginning --max-messages 1

# 主题详情  
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --describe --zookeeper localhost:2181/alonekafka --topic chat-records
```

- Elasticsearch系统参数设置:
```shell script
#单用户可以打开的最大文件数量，可以设置为官方推荐的65536或更大些
echo "* - nofile 655360" >>/etc/security/limits.conf

#单用户内存地址空间
echo "* - as unlimited" >>/etc/security/limits.conf

#单用户线程数
echo "* - nproc 2056474" >/etc/security/limits.conf

#单用户文件大小
echo "* - fsize unlimited" >>/etc/security/limits.conf

#单用户锁定内存
echo "* - memlock unlimited" >>/etc/security/limits.conf

#单进程可以使用的最大map内存区域数量
echo "vm.max_map_count = 655300" >>/etc/sysctl.conf

#TCP全连接队列参数设置，这样设置的目的是防止节点数较多（比如超过
echo net. ipv4. tcp abort on overflow=1>>/etc/sstl. conf
echo net core somaxconn =2048>>/etc/sysctl. conf

sysctl -p  #重置系统参数配置
```


- vim文件大小及查找操作
```shell script
df -h 	                        目前所有文件系统的可用空间及使用情形
du -sh *		        查看当前目录下各个文件及目录占用空间大小
du -h --max-depth=1 /home 	指定下级目录层级数查询空间大小
```

- docker
```shell script
1. 在使用docker run启动容器时,使用--restart参数来设置:
no  -容器退出时,不重启容器
on-failure -只有在非0状态退出时才从新启动容器
always -无论退出状态是如何,都重启容器

2. 如果创建时未指定 --restart=always ,可通过update 命令设置:
> docker update --restart=always aaron

3.还可以在使用on-failure策略时,指定Docker将尝试重新启动容器的最大次数。默认情况下,Docker将尝试永远重新启动容器。
> sudo docker run --restart=on-failure:10 aaron

4.docker push 本地镜像 denied: requested access to the resource is denied
# 使用 docker tag 修改镜像地址
> docker tag IMAGE IMAGE_NEW
> docker tag registry.us-west-1.aliyuncs.com/mobikok/opx/sdk-logs-web:v1.0.0 registry.us-west-1.aliyuncs.com/mobikok/sdk-logs-web:v1.0.0
```

- crontab定时任务规则
```shell script
# 1月1日的星期一每7:30,7:40执行:
30,40     */7      1          *                3,5
分钟	  小时     天(月)     月                天(星期)
0-59      0-23      1-31     1-12(JAN-DEC)    0-6,7(SUN-SAT,SUN)

*	任意值
,	多个值,分隔
-	指定范围
/	步长

# 1-12月,分别在每月的周三和周五的7:30,7:40分执行:
30,40 */7 * 1-12 3,5
```


- hive快速复制表
```shell script
0.非分区表快速复制(非分区表)
CREATE TABLE new_table AS SELECT * FROM old_table;

1.复制源表schema到临时表(分区表)
create table ssp_report_overall_dwr_tmp2021 like ssp_report_overall_dwr;

2.拷贝hdfs元数据到临时表
hadoop fs -cp /apps/hive/warehouse/ssp_report_overall_dwr/l_time=2021-03-14* /apps/hive/warehouse/ssp_report_overall_dwr_tmp2021/
hadoop fs -cp /apps/hive/warehouse/ssp_report_overall_dwr/l_time=2021-03-14 00%3A00%3A00/* /apps/hive/warehouse/ssp_report_overall_dwr_tmp2021/*

3.修复分区表
3.1 set hive.msck.path.validation=ignore;(适用于具有大量分区表)
3.2 MSCK REPAIR TABLE ssp_report_overall_dwr_tmp2021;
ps:在执行MSCK REPAIR 报错:Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask,需先执行 hive.msck.path.validation=ignore()
pps:可尝试手动添加分区 alter table 表名 drop partition(fields1='value',fields2="value") partition(fields3='value',fields4="value")
```

- emoji代码速查表  
[速查表](https://www.webfx.com/tools/emoji-cheat-sheet/)
	
- 阿里云任务下线步骤  
```text
a. 到 https://rosnext.console.aliyun.com/overview 概览>我的资源>资源分布进入,把里面的资源栈删除 (直接进入资源栈可能无法查看到自己所用资源,注意查看自己资源分布)。
b. 如果不放心，也可以在https://usercenter2.aliyun.com/home 关闭启用延停金额,关闭启用延停金额,关闭启用延停金额,然后在慢慢根据步骤删除资源。
c. 如果确保所以资源栈都已删除干净的前提下,下线应用,然后删除已建应用 (有同学反馈资源栈都已删除干净,无法下线删除应用,需要先充值https://usercenter2.aliyun.com/finance/fund-management/recharge?spm=5176.12818093.0.3.5b0e16d0FjsujS
根据欠费金额,进行充值即可,自动结算,无欠费后,在尝试下线删除应用即可)
```

-阿里云帐号注销流程
```shell script
- 删除AK:https://usercenter.console.aliyun.com/#/manage/ak
- 访问控制控制台链接:https://ram.console.aliyun.com/?spm=5176.12818093.products-grouped.dram.1bcd16d0611tGi	(用户、用户组、授权、RAM角色全部删除)
- 删除资源栈:https://rosnext.console.aliyun.com/overview
- 到容器镜像服务控制台,把实例删除释放了:https://cr.console.aliyun.com/cn-hangzhou/instances
- 删除财务单元资源:
  https://usercenter2.aliyun.com/finance/link-account/list 	删除关联子帐号
  https://usercenter2.aliyun.com/finance/finance-unit/list 	删除财务单元
- 进入发票管理页面,点击"发票索取":https://usercenter2.aliyun.com/invoice/list/aliyun?pageIndex=1&pageSize=20&ownerId=1037842620328768&invoiceType=aliyun&spm=5176.8351553.nav-right.ditem-inv.6b701991NMFHkM
```

- 阿里云工单提交地址  
[地址一](https://workorder.console.aliyun.com/console.htm#/ticket/add?productCode=account&commonQuestionId=312&isSmart=true&iatraceid=1611296782912-2084563a0cf81eb9936dce&channel=selfservice)  
[地址二](https://workorder.console.aliyun.com/console.htm#/ticket/add?productCode=idaas&commonQuestionId=4404&isSmart=true&iatraceid=1611297191664-29c44da4892fa2883c28aa&channel=selfservice)

- GitHub项目搜索技巧
```shell script
# 查找一个基于springboot开源项目(starts>4000,pushed更新时间>2020-01-01,language语言:java,forks>10000)
> in:name:springboot starts:>4000 pushed:>2020-01-01 language:java forks:>10000
# 查找一个基于python的开源项目(readme.md文档中包含 淘宝 关键字,language语言:python,pushed更新时间>2020-09-01)
> in:readme 淘宝 language:python pushed:>2020-09-01
# 常用条件

```