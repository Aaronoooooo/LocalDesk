//package com.flinksqlscala
//
///**
//  * @author pengfeisu
//  * @create 2020-03-02 21:25
//  */
//import java.util.Properties
//
//import org.apache.flink.api.common.functions.RuntimeContext
//import org.apache.flink.api.common.serialization.SimpleStringSchema
//import org.apache.flink.streaming.api.CheckpointingMode
//import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
//import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
//import org.apache.flink.table.api.{EnvironmentSettings, Table}
//import org.apache.flink.table.api.scala._
//import org.apache.kafka.clients.consumer.ConsumerConfig
//import org.elasticsearch.action.index.IndexRequest
//import org.elasticsearch.client.Requests
//
//object TestTable1 {
//
//  def main(args: Array[String]): Unit = {
//    demo
//  }
//
//  def demo: Unit ={
//
//    //环境设置
//    val fsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build()
//    //val fsSettings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build()
//    //创建流式环境
//    val sEnv = StreamExecutionEnvironment.getExecutionEnvironment
//
//    //启动检查点机制
//    sEnv.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE)
//    //配置checkpoint必须在3.5s内完成一次checkpoint，否则检查点终止
//    sEnv.getCheckpointConfig.setCheckpointTimeout(3500)
//    //设置checkpoint之间时间间隔 <=  Checkpoint interval
//    sEnv.getCheckpointConfig.setMinPauseBetweenCheckpoints(5)
//    //配置checkpoint并行度，不配置默认1
//    sEnv.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
//    //一旦检查点不能正常运行，Task也将终止
//    sEnv.getCheckpointConfig.setFailOnCheckpointingErrors(true)
//    //将检查点存储外围系统 filesystem、rocksdb,可以配置在cancel任务时候，系统是否保留checkpoint
//    sEnv.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
//
//    //创建流表环境
//    val fsTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(sEnv,fsSettings)
//
//    //设置并行度为
//    sEnv.setParallelism(1)
//
//    //数据输出到ES的连接
//    val httpConnect = EsUtil.getEsHttpConnect
//
//    //连接kafka配置
//    val ZOOKEEPER_HOST = "CentOS7:2181"
//    val KAFKA_BROKERS = "CentOS7:9092"
//    val TRANSACTION_GROUP = "sang3"
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("zookeeper.connect",ZOOKEEPER_HOST)
//    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKERS)
//    kafkaProps.setProperty("group.id",TRANSACTION_GROUP)
//    kafkaProps.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest")
//
//    /**
//      * flink消费kafka数据
//      */
//
//    val student: DataStream[Student1] = sEnv
//      .addSource(new FlinkKafkaConsumer[String]("test_student", new SimpleStringSchema(), kafkaProps))
//      .map(line => line.split("\\s+"))
//      .map(ts => new Student1(ts(0),ts(1),Integer.valueOf(ts(2))))
//    student.print("学生传入的数据为->")
//
//    val studentBiaoData = new ElasticsearchSink.Builder[Student1](
//      httpConnect,
//      new ElasticsearchSinkFunction[Student1] {
//        def process(studentBiao: Student1, ctx: RuntimeContext, indexer: RequestIndexer) {
//          val json = new java.util.HashMap[String, Any]()
//
//          json.put("studentId",studentBiao.getStudentId)
//          json.put("studentName",studentBiao.getStudentName)
//          json.put("studentAge",studentBiao.getStudentAge)
//
//
//          val rqst: IndexRequest = Requests.indexRequest
//            .index("sang_student")
//            .`type`("_doc")
//            .id(studentBiao.getStudentId)
//            .source(json)
//
//          indexer.add(rqst)
//
//        }
//      }
//    )
//
//    studentBiaoData.setBulkFlushMaxActions(1)
//    studentBiaoData.setBulkFlushMaxSizeMb(1)
//    studentBiaoData.setBulkFlushInterval(1)
//
//    student.addSink(studentBiaoData.build())
//
//    val course: DataStream[Course] = sEnv
//      .addSource(new FlinkKafkaConsumer[String]("test_course", new SimpleStringSchema(), kafkaProps))
//      .map(line => line.split("\\s+"))
//      .map(ts => Course(ts(0), ts(1), ts(2)))
//    course.print("课程传入的数据为->")
//
//    val teacher: DataStream[Teacher] = sEnv
//      .addSource(new FlinkKafkaConsumer[String]("test_teacher", new SimpleStringSchema(), kafkaProps))
//      .map(line => line.split("\\s+"))
//      .map(ts => Teacher(ts(0), ts(1),Integer.valueOf(ts(2))))
//    teacher.print("老师传入的数据为->")
//
//    /**
//      *三条流分别注册三个表
//      */
//
//    fsTableEnv.registerDataStream("Student", student, 'studentId, 'studentName,'studentAge)
//    fsTableEnv.registerDataStream("Course", course, 'courseId, 'courseName,'courseTeacher)
//    fsTableEnv.registerDataStream("Teacher", teacher, 'teacherId, 'teacherName,'teacherAge)
//
//    val resultStudent: Table = fsTableEnv.sqlQuery("select * from Student")
//    val resultCourse: Table = fsTableEnv.sqlQuery("select * from Course")
//    val resultTeacher: Table = fsTableEnv.sqlQuery("select * from Teacher")
//
//    fsTableEnv.toRetractStream[(String,String,Int)](resultStudent).print("学生数据->")
//    fsTableEnv.toRetractStream[(String,String,Int)](resultTeacher).print("老师数据->")
//    fsTableEnv.toRetractStream[(String, String, String)](resultCourse).print("课程数据->")
//
//    //安装财务导出宽表
//    val kuanBiaoData = new ElasticsearchSink.Builder[KuanBiao](
//      httpConnect,
//      new ElasticsearchSinkFunction[KuanBiao] {
//        def process(kuanBiao: KuanBiao, ctx: RuntimeContext, indexer: RequestIndexer) {
//          val json = new java.util.HashMap[String, Any]()
//
//          /*          json.put("studentId",kuanBiao.studentId)
//                    json.put("studentName",kuanBiao.courseName)
//                    json.put("studentAge",kuanBiao.studentAge)
//                    json.put("courseName",kuanBiao.courseName)*/
//          json.put("courseTeacher",kuanBiao.courseTeacher)
//          json.put("teacherName",kuanBiao.teacherName)
//          json.put("teacherAge",kuanBiao.studentAge)
//
//          val rqst: IndexRequest = Requests.indexRequest
//            .index("sang_indez")
//            .`type`("_doc")
//            .id(kuanBiao.studentId)
//            .source(json)
//
//          indexer.add(rqst)
//
//        }
//      }
//    )
//
//    kuanBiaoData.setBulkFlushMaxActions(1)
//    kuanBiaoData.setBulkFlushMaxSizeMb(1)
//    kuanBiaoData.setBulkFlushInterval(1000)
//
//    val kuanBiaoStream: DataStream[(String, String, Int, String, String, String, Int)] = fsTableEnv
//      .sqlQuery("select s.studentId,s.studentName,s.studentAge,c.courseName,c.courseTeacher," +
//        "t.teacherName,t.teacherAge from Student s join Course c on s.studentId=c.courseId join Teacher t" +
//        " on t.teacherId =s.studentId ")
//      .toAppendStream[(String, String, Int, String, String, String, Int)]
//    val value: DataStream[KuanBiao] = kuanBiaoStream.map(a => KuanBiao(a._1,a._2,a._3,a._4,a._5,a._6,a._7))
//
//    value.print("宽表数据为->")
//
//    value.addSink(kuanBiaoData.build())
//
//    sEnv.execute("TableDemo")
//  }
//}