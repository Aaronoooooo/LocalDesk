package com.mobikok.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, MapType, StringType, StructField, StructType}


object SparkSQL15_Req1 {

    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "root")
        // TODO 创建环境对象
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        // builder 构建，创建

        // TODO 访问外置的Hive
        val spark = SparkSession.builder()
                .enableHiveSupport()
                .config(sparkConf).getOrCreate()
        import spark.implicits._

        spark.sql("use atguigu200213")

        // TODO 从Hive表中获取满足条件的数据
        spark.sql(
            """
              |select
              |   a.*,
              |   c.area,
              |   p.product_name,
              |   c.city_name
              |from user_visit_action a
              |join city_info c on c.city_id = a.city_id
              |join product_info p on p.product_id = a.click_product_id
              |where a.click_product_id > -1
            """.stripMargin).createOrReplaceTempView("t1")

        // TODO 将数据根据区域和商品进行分组，统计商品点击的数量
        // 北京，上海，北京，深圳
        //******************************************
        // in : cityname ; String
        // buffer : 2结构，（total, map）
        // out : remark : String
        // （ 商品点击总和， 每个城市点击总和 ）
        // （ 商品点击总和， Map（城市，点击Sum） ）
        // 城市点击sum / 商品点击总和 %
        // TODO 创建自定义聚合函数
        val udaf = new CityRemarkUDAF
        // TODO 注册聚合函数
        spark.udf.register("cityRemark", udaf)

        spark.sql(
            """
              |select
              |    area,
              |    product_name,
              |    count(*) as clickCount,
              |    cityRemark(city_name)
              |from t1 group by area, product_name
            """.stripMargin).createOrReplaceTempView("t2")

        // TODO 将统计结果根据数量进行排序（降序）
        spark.sql(
            """
              |select
              |    *,
              |    rank() over( partition by area order by clickCount desc ) as rank
              |from t2
            """.stripMargin).createOrReplaceTempView("t3")

        // TODO 将组内排序后的结果取前3名
        spark.sql(
            """
              |select
              |   *
              |from t3
              |where rank <= 3
            """.stripMargin).show

        spark.stop
    }
    // 自定义城市备注聚合函数
    class CityRemarkUDAF extends UserDefinedAggregateFunction {
        // TODO 输入的数据其实就是城市名称
        override def inputSchema: StructType = {
            StructType(Array(StructField("cityName", StringType)))
        }

        // TODO 缓冲区中的数据应该为：totalcnt, Map[cityname, cnt]
        override def bufferSchema: StructType = {
            StructType(Array(
                StructField("totalcnt", LongType),
                StructField("citymap", MapType( StringType, LongType ))
            ))
        }

        // TODO 返回城市备注的字符串
        override def dataType: DataType = {
            StringType
        }

        override def deterministic: Boolean = true

        // TODO 缓冲区的初始化
        override def initialize(buffer: MutableAggregationBuffer): Unit = {
            buffer(0) = 0L
            //buffer.update(0, 0L)
            buffer(1) = Map[String, Long]()
        }

        // TODO 更新缓冲区
        override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
            val cityName = input.getString(0)
            // 点击总和需要增加
            buffer(0) = buffer.getLong(0) + 1
            // 城市点击增加
            val citymap: Map[String, Long] = buffer.getAs[Map[String, Long]](1)

            val newClickCount = citymap.getOrElse(cityName,0L) + 1

            buffer(1) = citymap.updated(cityName, newClickCount)
        }

        // TODO 合并缓冲区
        override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

            // 合并点击数量总和
            buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
            //buffer1.update(0, buffer1.getLong(0) + buffer2.getLong(0))

            // Scala语法
//            val array = Array(1,2,3,4)
//            array(0) = 1
//            array.update(0,1)

            // 合并城市点击map
            val map1 = buffer1.getAs[Map[String, Long]](1)
            val map2 = buffer2.getAs[Map[String, Long]](1)

            buffer1(1) = map1.foldLeft(map2) {
                case ( map, (k, v) ) => {
                    map.updated(k, map.getOrElse(k, 0L) + v)
                }
            }
        }
        // TODO 对缓冲区进行计算并返回备注信息
        override def evaluate(buffer: Row): Any = {
            val totalcnt: Long = buffer.getLong(0)
            val citymap = buffer.getMap[String, Long](1)

//            if (citymap.size > 2) {
//
//            } else {
//
//            }
            val cityToCountList: List[(String, Long)] = citymap.toList.sortWith(
                (left, right) => left._2 > right._2
            ).take(2)

            val hasRest = citymap.size > 2

            var rest = 0L
            val s = new StringBuilder
            cityToCountList.foreach{
                case ( city, cnt ) => {
                    val r = ( cnt * 100 / totalcnt )
                    s.append(city + " " + r + "%,")
                    rest = rest + r
                }
            }
            s.toString() + "其他：" + (100 - rest) + "%"
//            if ( hasRest ) {
//
//            } else {
//                s.toString
//            }
        }
    }
}
