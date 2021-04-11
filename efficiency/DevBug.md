#### MySQL
- MySQL向新建表中插入中文数据提示:Incorrect string value: '\xE5\xBC\xA0\xE4\xB8\x89' for column 'UserName' at row 1 因为编码问题导致,应该选择utf-8类型编码： 
```sql
# 修改表中所有字段编码格式为utf8
> alter table `table_name` convert to character set utf8;
# CHARACTER SET 更改库/表字符集,COLLATE 更改库/表排序规则
> ALTER DATABASE `databasename` CHARACTER SET utf8 COLLATE utf8_unicode_ci;
> ALTER TABLE `tablename` CONVERT TO CHARACTER SET utf8 COLLATE utf8_unicode_ci;
```

#### Navicat 新建查询提示系统找不到路径
> 关闭当前数据库连接  
![找不到路径.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpdn3zqfajj20ss09ujs7.jpg)  
![关闭当前数据库链接.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpdn3zqa0sj20mu0eojt9.jpg)  

> 右键编辑连接 -> 高级 -> 修改Navicat设置位置
![设置位置.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpdn3zrhpdj210d0g8tbv.jpg)  

> 选择新建查询  
![新建查询成功.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpdn3zqhd0j20nf0ii40p.jpg)

#### Redis
- Redis外网无法反问:Could not get a resource from the pool  
```shell script
# 修改配置文件:
step1: #bind 127.0.0.1  	#注释掉bind配置(或者配置为0.0.0.0),所有IP可以访问,指定ip访问可以配置为:bind 127.0.0.1 192.168.0.23 192.168.0.25
step2: protected-mode no 	#配置为no以bind方式添加访问白名单
setp3: daemonize yes		#使用守护进程方式运行

# 启动客户端执行命令:
$ 127.0.0.1:6379> CONFIG SET protected-mode no
```

- Redis连接:(error) NOAUTH Authentication required  
```shell script
step1: cat $REDIS_HOME/redis.conf | grep requirepass >>>> requirepass iX0TVWCZrA3jf6wT

step2: redis-cli -a iX0TVWCZrA3jf6wT

step3: redis-cli -a `cat /data/redis/redis.conf | grep requirepass | awk '{print $2}'`  #一步到位
```
#### Spark
- spark-sql启动:JDOFatalInternalException: Error creating transactional connection factory  
```shell script
step1: 将hive-site.xml配置文件拷贝到$SPARK_HOME/conf/

step2: cp $HIVE_HOME/lib/mysql-connector-java-5.1.48-bin.jar $SPARK_HOME/jars/
```

- kafka与spark集成序列化问题:添加Kryo序列化配置  
```shell script
> sparkConf.registerKryoClasses(Array(classOf[Array[org.apache.kafka.clients.consumer.ConsumerRecord[String, String]]]))
> sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
```

- spark-submit提交后:ERROR yarn.ApplicationMaster: SparkContext did not initialize after waiting for 100000 ms.  
```shell script
# 本地代码配置SparkConf.setMaster("local[*]")
step1:代码中取消.setMaster("local[*]")
step2:spark-submit --master yarn-cluster
```

#### Mavne
- idea打包插件打包失败:Execution scala-compile-first of goal net.alchim31.maven:scala-maven-plugin:4.0.1:compile failed: org.scala-lang:scala-library is missing from project dependencies
```shell script
step1: 删除dependency-reduced-pom.xml后打包
```

- Maven打包异常问题: BUILD FAILURE,Could not resolve dependencies for project
![](http://ww1.sinaimg.cn/large/c9d5eefcgy1gp473qoyuzj219l05ojss.jpg)
```shell script
# 存在多模块相互依赖的情况
1. 在父模块中执行mvn clean install
2. 在子模块中执行mvn clean install package -Dmaven.test.skip=true

# 独立项目,无多模块依赖情况
1.mvn clean install package -Dmaven.test.skip=true
```

#### ElasticSearch7.6.2使用_delete_by_query进行批量查询版本冲突问题
```shell script
# 原因:直接使用_delete_by_query构建批量查询删除犹豫数据量太大,导致sockectTimeout
curl -X POST "mobikok-bigdata301:9200/mongo_message_*/_delete_by_query?pretty" -H 'Content-Type: application/json' -d'{
    "query": {
        "range": {
            "timestamp": {
                "lt": 1606751999000
            }
        }
    }
}'
```
> 优化一 : 开启多线程并发查询(导致版本冲突,由乐观锁并发控制引起)  
```shell script
curl -X POST "mobikok-bigdata301:9200/mongo_message_*/_delete_by_query?scroll_size=3000&slices=5&pretty" -H 'Content-Type: application/json' -d'{
    "query": {
        "range": {
            "timestamp": {
                "lt": 1606751999000
            }
        }
    }
}'
```
![版本冲突.jpg](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpg24y9wdej20x8066wf2.jpg)

> 优化二 : wait_for_completion=false 异步执行,不阻塞请求,后台自动运行,返回task编号; wait_for_completion=true 表示请求阻塞,直到操作完成结束(conflicts=proceed参数会跳过版本冲突,继续执行)  
```shell script
curl -X POST "mobikok-bigdata301:9200/mongo_message_*/_delete_by_query?scroll_size=3000&slices=5&conflicts=proceed&wait_for_completion=false&pretty" -H 'Content-Type: application/json' -d'{
    "query": {
        "range": {
            "timestamp": {
                "lt": 1606751999000
            }
        }
    }
}'
# 返回的task编号通过GET /_tasks/<task_id> 查看后台任务状态
```
![task状态](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpg29x61d2j20wd0gogo0.jpg)
#### Docker
- docker-compose -f docker-compose-server.yaml down  
  >ERROR: error while removing network: network pro-sdk id 167ad83de7fc4b25006cd74d5f965 has active endpoints
```shell script
1. 查看网络详情
> docker network inspect pro_sdk  

{
        "Name": "pro_sdk",  #参数一
        "Id": "167ad83de7fc4b25006cd74d5f9651563e9ec03ad7c949ee8f4a886be7ce0dba",
        "Created": "2021-04-01T17:23:12.725424629+08:00",
        "Scope": "local",
        "Driver": "bridge",
        "EnableIPv6": false,
        "IPAM": {
            "Driver": "default",
            "Options": null,
            "Config": [
                {
                    "Subnet": "172.21.0.0/16",
                    "Gateway": "172.21.0.1"
                }
            ]
        },
        "Internal": false,
        "Attachable": false,
        "Ingress": false,
        "ConfigFrom": {
            "Network": ""
        },
        "ConfigOnly": false,
        "Containers": {
            "4cbe7bb140b1a39ab14096aa8fd79ae21ac28d7392167b1bb041885b4a030505": {
                "Name": "pro_sdk-logs-task-file_1",  #参数二
                "EndpointID": "eeaf2a0a06ab3366c2af49f32c864ba1828f1cee4f0cb7b210f06a79730795c6",
                "MacAddress": "02:42:ac:15:00:05",
                "IPv4Address": "172.21.0.5/16",
                "IPv6Address": ""
            },
            "eb9408e87e1e697e114f36252947d253de0a7294c7cadcc5734a798d5661fedd": {
                "Name": "pro_sdk-nginx_1",  #参数二
                "EndpointID": "fb09744d54e377ed666bc15cd83790beee8ade44def693151e366a65ec1ad1df",
                "MacAddress": "02:42:ac:15:00:02",
                "IPv4Address": "172.21.0.2/16",
                "IPv6Address": ""
            }
        },
        "Options": {},
        "Labels": {}
    }
]

2. 断开网络,分别添加标记的参数一和参数二
> docker network disconnect -f pro_sdk pro_sdk-logs-task-file_1
> docker network disconnect -f pro_sdk pro_sdk-nginx_1

3. 重新执行停止命令
> docker-compose -f docker-compose-peer.yaml down
```
