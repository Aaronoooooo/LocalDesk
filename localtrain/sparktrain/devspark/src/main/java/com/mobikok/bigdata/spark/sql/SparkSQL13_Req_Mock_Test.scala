package com.mobikok.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object SparkSQL13_Req_Mock_Test {
  def main(args: Array[String]): Unit = {
    //private val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    //private val session: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    System.setProperty("HADOOP_USER_NAME", "root")
    val session: SparkSession = SparkSession.builder()
      .master("local[*]")
      .enableHiveSupport()
      .appName("sparkSQL")
      .getOrCreate()

    //session.sql(
    //  """
    //    |drop table if exists `user_visit_action`
    //  """.stripMargin)
    //session.sql(
    //  """
    //    |drop table if exists `product_info`
    //  """.stripMargin)
    //session.sql(
    //  """
    //    |drop table if exists `city_info`
    //  """.stripMargin)
    //
    //session.sql(
    //  """
    //    |CREATE TABLE `user_visit_action`(
    //    |  `date` string,
    //    |  `user_id` bigint,
    //    |  `session_id` string,
    //    |  `page_id` bigint,
    //    |  `action_time` string,
    //    |  `search_keyword` string,
    //    |  `click_category_id` bigint,
    //    |  `click_product_id` bigint,
    //    |  `order_category_ids` string,
    //    |  `order_product_ids` string,
    //    |  `pay_category_ids` string,
    //    |  `pay_product_ids` string,
    //    |  `city_id` bigint)
    //    |row format delimited fields terminated by '_'
    //  """.stripMargin)

    //session.sql(
    //  """
    //    |load data local inpath 'input/user_visit_action.txt' into table user_visit_action
    //  """.stripMargin)
    //
    //session.sql(
    //  """
    //    |CREATE TABLE `product_info`(
    //    |  `product_id` bigint,
    //    |  `product_name` string,
    //    |  `extend_info` string)
    //    |row format delimited fields terminated by '\t'
    //  """.stripMargin)
    //session.sql(
    //  """
    //    |load data local inpath 'input/product_info.txt' into table product_info
    //  """.stripMargin)
    //
    //session.sql(
    //  """
    //    |CREATE TABLE `city_info`(
    //    |  `city_id` bigint,
    //    |  `city_name` string,
    //    |  `area` string)
    //    |row format delimited fields terminated by '\t'
    //  """.stripMargin)
    //session.sql(
    //  """
    //    |load data local inpath 'input/city_info.txt' into table city_info
    //  """.stripMargin)

    //session.sql("""select * from user_visit_action limit 20""").show()
    //session.sql("""select * from product_info limit 20""").show()
    //session.sql("""select * from city_info limit 20""").show()

    session.sql(
      """
        |select
        |*
        |from
        |	(select
        |	*,
        |	rank() over(partition by area order by click_count desc) as rank
        |	from
        |	(select
        |		t1.area area,
        |		t1.product_name product_name,
        |		count(*) click_count
        |	from
        |		(select
        |			a.*,
        |			c.area,
        |			p.product_name
        |		from user_visit_action a
        |		join city_info c on a.city_id = c.city_id
        |		join product_info p on p.product_id = a.click_product_id
        |		where a.click_product_id > -1) t1
        |	group by area,product_name) t2
        |	) t3
        |where rank <=3
      """.stripMargin).show()

    //session.stop()
  }
}