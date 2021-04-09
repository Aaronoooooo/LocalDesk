package com.atguigu.spark.day09

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object sparkSQL_Req {

  def main(args: Array[String]): Unit = {


    // TODO 创建环境对象
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    // builder 构建，创建
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    import spark.implicits._
    spark.sql("use atguigu200213")

    spark.sql(
      """
        |select
        |   *
        |from (
        |    select
        |        *,
        |        rank() over( partition by area order by clickCount desc ) as rank
        |    from (
        |        select
        |            area,
        |            product_name,
        |            count(*) as clickCount
        |        from (
        |            select
        |               a.*,
        |               c.area,
        |               p.product_name
        |            from user_visit_action a
        |            join city_info c on c.city_id = a.city_id
        |            join product_info p on p.product_id = a.click_product_id
        |            where a.click_product_id > -1
        |        ) t1 group by area, product_name
        |    ) t2
        |) t3
        |where rank <= 3
        |""".stripMargin).show()

    spark.stop()

  }
}
