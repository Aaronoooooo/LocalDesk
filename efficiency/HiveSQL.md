#### 求每个用户当月及累计访问次数  
- 数据示例
```text
# 原表
userId  visitDate   visitCount
u01     2021/1/21   5
u02     2021/1/23   6
u03     2021/1/22   8
u01     2021/2/21   5

# 目标表
userId  visitDate   Count    Sum(count)
u01     2021-01     5        5
u01     2021-02     12       17
u02     2021-01     12       12
```
##### 实现  
- 建表
```sql
create table action
    (userId string,
    visitDate string,
    visitCount int) 
row format delimited fields terminated by "\t";
```
- 修改数据格式
```sql
select
    userId,
    date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') mn,
    visitCount
from
    action;t1
```
- 计算每人单月访问量
```sql
select
    userId,
    mn,
    sum(visitCount) mn_count
from
    t1
group by 
userId,mn;t2
```
- 按月累计访问量
```sql
select
    userId,
    mn,
    mn_count,
    sum(mn_count) over(partition by userId order by mn)
from t2;
```
- 最终sql
```sql
select
    userId,
    mn,
    mn_count,
    sum(mn_count) over(partition by userId order by mn)
from 
(   select
        userId,
        mn,
        sum(visitCount) mn_count
    from
         (select
             userId,
             date_format(regexp_replace(visitDate,'/','-'),'yyyy-MM') mn,
             visitCount
         from
             action)t1
group by userId,mn)t2;
```