# 介绍

`字典` 汉语解释 : 主要是以收字为主 , 为单字提供音韵 、意思解释 、例句 、用法等等的工具书。  
那么在 ClickHouse 中 `字典` 这一特性 , 具体又有怎样的用途呢 ? 让我们一起来看看官方文档的介绍 :

> A dictionary is a mapping (key -> attributes) that is convenient for various types of reference lists.  
ClickHouse supports special functions for working with dictionaries that can be used in queries. It is easier and more efficient to use dictionaries with functions than a JOIN with reference tables.


根据文档的介绍 , 大致意思就是 ClickHouse 的 `字典` 是以键值和属性 (key -> attributes) 映射的形式 , 将`字典`和函数结合使用比直接引用表进行 JOIN 会更加高效。  
继续查阅官方文档 , 对 ClickHouse 的 `字典` 这一特性总结如下 :

|   特性   | 描述                                                                                                                                                                                                        |
| :------: | :---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
|   分类   | 分为内部字典和外部字典                                                                                                                                                                                      |
|   存储   | 可以完全存储在内存中，或者部分在内存中                                                                                                                                                                      |
|   创建   | XML 文件 和 DDL 查询两种方式                                                                                                                                                                                |
|  数据源  | 内部字典 : 用于快速存取 geobase 地理数据<br /><br />外部字典 : 目前涵盖<br />Local file,Executable file,HTTP(s),DBMS,ODBC,MySQL,PostgreSQL,ClickHouse,MongoDB,Redis,Cassandra,PostgreSQL <br />共 12 种类型 |
| 动态加载 | 动态感知外置数据源变化,并支持不停机在线更新字典                                                                                                                                                             |

# 应用

针对`字典`的这一特性 , 在实际应用中最直接的提现就是 , 比如我们会把一些外部数据源中存放的维度表通过字典来 "挂载" 到内存中以实现更加高效的 JOIN 类型 。

# 实践

### 内置字典

用于快速读取 getbase 地理数据 , 内置字典默认是被禁用的,需要打开 path_to_regions_hierarchy_file 和 path_to_regions_names_files 配置 , 并按规定文件格式自行导入数据。
此处不单独演示 , 详情可查阅官方文档 : https://clickhouse.tech/docs/en/sql-reference/dictionaries/internal-dicts/

### 外部字典

可从多个外部数据源加载数据,并存储在内存中,动态感知外置数据源变化,并支持不停机在线更新字典。

##### 创建外部字典

###### 数据准备

- MySQL

```sql
# 建表
CREATE TABLE employee (
    id int NOT NULL AUTO_INCREMENT,
    number int DEFAULT NULL,
    name varchar(32) DEFAULT NULL,
    age int DEFAULT NULL,
    updatetime datetime DEFAULT NULL,
    PRIMARY KEY (id)
) ENGINE = InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET =utf8;

# 插入数据
INSERT INTO employee(number, name, age, updatetime)
VALUES (10001, 'zhangsan', 20, NOW()),
       (10002, 'lisi', 18, NOW()),
       (10003, 'wangwu', 19, NOW()),
       (10004, 'zhaoliu', 21, NOW());
```

- ClickHouse  
> DDL Query 创建字典表  

```sql
CREATE DICTIONARY IF NOT EXISTS employee_dic
(
  id UInt32,
	number UInt32,
	name String,
	age UInt32,
	updatetime DateTime
)
PRIMARY KEY id
SOURCE(MYSQL(
	port 3306
	user 'root'
	password 'spf@123'
	replica (host 'bigdata1' priority 1)
	db 'flinksink'
	table 'employee'
	invalidate_query 'select name from flinksink.employee where id = 16'
))
LAYOUT(FLAT())
LIFETIME(MIN 3 MAX 6);
```

![DDL Query](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpv18xbcfdj212u0kp0uv.jpg)
- DDL 配置解析  

| 参数             | 说明                                                                                                                                                                                                                                                                                              |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| port             | 数据库端口                                                                                                                                                                                                                                                                                        |
| user             | 数据库用户名                                                                                                                                                                                                                                                                                      |
| password         | 数据库密码                                                                                                                                                                                                                                                                                        |
| replica          | 数据库 Host 地址,支持 MySQL 集群,priority(表示连接优先级)                                                                                                                                                                                                                                         |
| db               | 数据库名称                                                                                                                                                                                                                                                                                        |
| table            | 字典对应的数据表                                                                                                                                                                                                                                                                                  |
| where            | 查询 table 时的过滤条件,非必填项                                                                                                                                                                                                                                                                  |
| invalidate_query | 指定一条 SQL 语句,用于在数据更新时判断是否需要更新,非必填项                                                                                                                                                                                                                                       |
| SOURCE           | 外部字典数据源,目前涵盖了 <br />Local file,Executable file,HTTP(s),DBMS,ODBC,MySQL,PostgreSQL,ClickHouse,MongoDB,Redis,Cassandra,PostgreSQL <br />共计 12 种数据源                                                                                                                                |
| LAYOUT           | 字典数据在内存中以何种形式存储,目前涵盖了<br />flat,hashed,sparse_hashed,cache,ssd_cache,direct,range_hashed,complex_key_hashed,complex_key_cache,ssd_cache,ssd_complex_key_cache,complex_key_direct,ip_trie<br />共计 13 中存储类型,建议使用 flat,hashed,complex_key_hashed 以提供最佳的处理速度 |
| LIFETIME         | 以秒为单位定义字典的更新间隔和缓存字典的无效间隔<br />LIFETIME(0)可防止字典更新<br />LIFETIME(MIN 3 MAX 6)表示一个时间间隔,为了在大量服务器上升级时将负载分配到字典源上，这是必要的                                                                                                               |

> XML 文件创建字典表
```xml
<!-- vim /etc/clickhouse-server/mysql_dictionary.xml -->
<?xml version="1.0"?>
<dictionaries>
<dictionary>
    <source>
      <mysql>
          <port>3306</port>
          <user>root</user>
          <password>spf@123</password>
          <replica>
              <host>bigdata1</host>
              <priority>1</priority>
          </replica>
          <replica>
              <host>bigdata2</host>
              <priority>1</priority>
          </replica>
          <db>flinksink</db>
          <table>employee</table>
          <where>id=16</where>
          <invalidate_query>SQL_QUERY</invalidate_query>
      </mysql>
    </source>
    <name>employee_dic</name>
    <layout>
        <flat/>
    </layout>
    <layout>
        <flat/>
    </layout>

    <structure>
        <id>
            <name>id</name>
        </id>
        <attribute>
            <name>number</name>
            <type>UInt64</type>
            <null_value></null_value>
        </attribute>
        <attribute>
            <name>name</name>
            <type>String</type>
            <null_value></null_value>
        </attribute>
        <attribute>
            <name>age</name>
            <type>UInt32</type>
            <null_value></null_value>
        </attribute>
        <attribute>
            <name>updatetime</name>
            <type>DateTime</type>
            <null_value></null_value>
        </attribute>
    </structure>

     <lifetime>
        <min>3</min>
        <max>6</max>
    </lifetime>
</dictionary>
</dictionaries>
```
- XML 配置解析  

| 参数      | 说明                                                                                                                                                                                                                                         |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| name      | 字典的名称，用于确定字典的唯一标识，必须全局唯一，多个字典之间不允许重复                                                                                                                                                                     |
| structure | 字典的数据结构,由键值 key 和属性 attribute 两部分组成<br />id/key: 每个字典必须包含 1 个键值 key 字段,用于定位数据,类似数据库的表主键,键值 key 分为数值型和复合型两类<br />attribute: 用于定义字典的属性字段,字典可以拥有 1 到多个属性字段。 |

> 说明:  
> 1. flat 使用 UInt64 数值型,数组的初始大小为 1024,上限为 500000,最多只能保存 500000 行数据。
> 2. ClickHouse会自动识别并加载/etc/clickhouse-server目录下所有以_dictionary.xml结尾的配置文件,并动态感知文件的变化并更新字典表数据。

- attribute 属性说明

| 标签         | 产品描述                                                                                                            | 必填项                 |
| ------------ | ------------------------------------------------------------------------------------------------------------------- | ---------------------- |
| name         | 字段名称                                                                                                            | 是                     |
| type         | 字段类型,ClickHouse 尝试将字典中的值转换为指定的数据类型。例如,对于 MySQL,该字段可能是 TEXT, VARCHAR                | 是                     |
| null_value   | 一个空字符串,查询条件 key 没有对应元素的默认值                                                                      | 是                     |
| expression   | 使用表达式为远程列创建别名                                                                                          |                        |
| hierarchical | 是否支持层次结构                                                                                                    | 否<br />默认值: false  |
| injective    | 是否支持集合单射优化,开启后,在 GROUP BY 查询中调用 dictGet\* 函数通过 key 获取 value,则可以直接从 GROUP BY 子句返回 | 否<br />默认值: false  |
| is_object_id | 表示是否开启对 MongoDB 文档执行优化                                                                                 | 否<br />默认值: false. |

##### 输入和输出数据的格式

输入支持的格式可用于解析提供给`INSERT`数据 , `SELECT`从外部文件或表（例如File , URL或HDFS）执行或`读取外部字典数据`。输出支持的格式可用于`SELECT`结果集排序 , `INSERT`支持的文件或表。  
- 支持的格式如下

| Format                                    | Input | Output |
| ----------------------------------------- | :---: | :----: |
| TabSeparated                              |   ✔   |   ✔    |
| TabSeparatedRaw                           |   ✔   |   ✔    |
| TabSeparatedWithNames                     |   ✔   |   ✔    |
| TabSeparatedWithNamesAndTypes             |   ✔   |   ✔    |
| Template                                  |   ✔   |   ✔    |
| TemplateIgnoreSpaces                      |   ✔   |   ✗    |
| CSV                                       |   ✔   |   ✔    |
| CSVWithNames                              |   ✔   |   ✔    |
| CustomSeparated                           |   ✔   |   ✔    |
| Values                                    |   ✔   |   ✔    |
| Vertical                                  |   ✗   |   ✔    |
| VerticalRaw                               |   ✗   |   ✔    |
| JSON                                      |   ✗   |   ✔    |
| JSONString                                |   ✗   |   ✔    |
| JSONCompact                               |   ✗   |   ✔    |
| JSONCompactString                         |   ✗   |   ✔    |
| JSONEachRow                               |   ✔   |   ✔    |
| JSONEachRowWithProgress                   |   ✗   |   ✔    |
| JSONStringEachRow                         |   ✔   |   ✔    |
| JSONStringEachRowWithProgress             |   ✗   |   ✔    |
| JSONCompactEachRow                        |   ✔   |   ✔    |
| JSONCompactEachRowWithNamesAndTypes       |   ✔   |   ✔    |
| JSONCompactStringEachRow                  |   ✔   |   ✔    |
| JSONCompactStringEachRowWithNamesAndTypes |   ✔   |   ✔    |
| TSKV                                      |   ✔   |   ✔    |
| Pretty                                    |   ✗   |   ✔    |
| PrettyCompact                             |   ✗   |   ✔    |
| PrettyCompactMonoBlock                    |   ✗   |   ✔    |
| PrettyNoEscapes                           |   ✗   |   ✔    |
| PrettySpace                               |   ✗   |   ✔    |
| Protobuf                                  |   ✔   |   ✔    |
| ProtobufSingle                            |   ✔   |   ✔    |
| Avro                                      |   ✔   |   ✔    |
| AvroConfluent                             |   ✔   |   ✗    |
| Parquet                                   |   ✔   |   ✔    |
| Arrow                                     |   ✔   |   ✔    |
| ArrowStream                               |   ✔   |   ✔    |
| ORC                                       |   ✔   |   ✗    |
| RowBinary                                 |   ✔   |   ✔    |
| RowBinaryWithNamesAndTypes                |   ✔   |   ✔    |
| Native                                    |   ✔   |   ✔    |
| Null                                      |   ✗   |   ✔    |
| XML                                       |   ✗   |   ✔    |
| CapnProto                                 |   ✔   |   ✗    |
| LineAsString                              |   ✔   |   ✗    |

##### 字典表查询 
```sql
# 查看数据  
SELECT * from employee_dic;
 
# 使用 dictGet* 函数查询  
SELECT dictGet('default.employee_dic', 'name', toUInt64(17)) 
 
# 查看字典表存储结构  
SELECT database,name,status,origin,type,key,attribute.names,attribute.types,loading_start_time,last_successful_update_time FROM system.dictionaries; 
```
  
![字典存储结构](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpv1w5fr62j20ny0crjso.jpg)  
- 系统表system.dictionaries各字段说明  

| database                    | 字典数据库名称                                               |
| --------------------------- | ------------------------------------------------------------ |
| name                        | 字典的名称，在使用字典函数时需要通过字典名称访问数据         |
| status                      | 字典加载状态                                                 |
| origin                      | 描述字典的配置文件的路径                                     |
| type                        | 字典所属类型                                                 |
| key                         | 字典的id/key值，数据通过key值定位。                          |
| attribute.names             | 属性名称，以数组形式保存。                                   |
| attribute.types             | 属性类型，以数组形式保存，其顺序与attribute.names相同。      |
| bytes_allocated             | 已载入数据在内存中占用的字节数。                             |
| query_count                 | 字典被查询的次数                                             |
| hit_rate                    | 字典数据查询的命中率                                         |
| element_count               | 已载入数据的行数                                             |
| load_factor                 | 数据的加载率                                                 |
| source                      | 数据源信息                                                   |
| lifetime_min                | 字典动态加载最小时间                                         |
| lifetime_max                | 字典动态加载最大时间                                         |
| loading_start_time          | 字典加载开始时间                                             |
| last_successful_update_time | 加载或更新字典的结束时间,有助于监测外部数据源的一些故障      |
| loading_duration            | 加载的持续时间                                               |
| last_exception              | 如果字典在创建或加载过程中发生错误，那么异常信息会写入此字段 |


##### 动态加载外部字典
###### 数据准备
- MySQL
```sql
# 数据插入
INSERT INTO employee(number, name, age, updatetime)
VALUES (10005, 'aaron', 18, NOW());

# 数据跟新
UPDATE employee SET name='zhaoliu_alter',updatetime=NOW() WHERE id=16;
```
![更新数据对比_mysql](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpv21f1m4fj20ej09aq4e.jpg)
- ClickHouse
> 通过 lifetime 配置动态更新外部字典  
>
![动态加载外部字典](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpv1bu56clj20n30dm3zt.jpg)  

##### 外部字典关联查询
###### 数据准备
- ClickHouse
```sql
# 创建ClickHouse表
CREATE TABLE employee_ck (
  id UInt64,
	number UInt32,
	name String,
	age UInt32,
	updatetime DateTime
)
ENGINE = MergeTree()
ORDER BY (id,updatetime);

# 插入数据
INSERT INTO employee_ck (id,number, name, age, updatetime)
VALUES (17, 10005, 'kevin', 18, toDateTime(now(), 'Asia/Shanghai'));
```
![ck_create](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpv25ke1v8j20go0jvq4e.jpg)  

###### 关联查询
```sql
SELECT *
FROM employee_ck AS ck
INNER JOIN employee_dic AS dic ON ck.id = dic.id;
```
![关联查询](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpv2bnfp3nj20o7074wf6.jpg)  

看到这里, 相信你对ClickHouse的数据字典已经了然于胸 , 那么还等什么 , 让我们赶快开始实践起来吧 !  

我是 Aaronoooooo , 向前走 , 就这么走 , 就算你不懂 !  

---

<center>关注 「<b><font color=#018574> 才懂编程 </font></b>」, 获取更多雕龙小技</center>  

![](http://ww1.sinaimg.cn/large/c9d5eefcgy1gpg6vo6vo8j2076076t96.jpg)

<font size=2 color=#F9C443>&nbsp;喜欢小懂记得一键三连哦~ </font>
<img src="http://ww1.sinaimg.cn/large/c9d5eefcgy1gpkrpca4j2g20ds01hmyj.gif" alt="欢迎三连" align=right/>  

