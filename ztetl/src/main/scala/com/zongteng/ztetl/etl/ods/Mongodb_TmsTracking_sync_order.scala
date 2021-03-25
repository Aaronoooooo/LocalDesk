package com.zongteng.ztetl.etl.ods

import com.google.gson.{Gson, JsonObject, JsonParser}
import com.zongteng.ztetl.api.{HBaseConfig, SparkKafkaCon}
import com.zongteng.ztetl.entity.common.{Sync_order_Insert, Sync_order_date, Sync_order_update}
import com.zongteng.ztetl.util.DataNullUtil.{nvl, nvlNull}
import com.zongteng.ztetl.util.{DateUtil, Log}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

object Mongodb_TmsTracking_sync_order {
  private val gson: Gson = new Gson()

  def main(args: Array[String]): Unit = {
    //作业名称
    val appName = "Mongodb_TmsTracking_sync_order"

    //消费时间间隔
    val interval = 5

    //kafka中topic名称
    val topic_name = "mysql_binlog"

    //消费组
    val group_Id = "consumer_mongodb_binlog"

    val streamAndInuptStream: (StreamingContext, InputDStream[ConsumerRecord[String, String]]) = SparkKafkaCon.getConnectKafka(appName, interval, topic_name, group_Id)

    val streamingContext = streamAndInuptStream._1
    val kafkaDStream = streamAndInuptStream._2


    //如果使用SparkStream和Kafka直连方式整合，生成的kafkaDStream必须调用foreachRDD
    kafkaDStream.foreachRDD(kafkaRDD => {
      if (!kafkaRDD.isEmpty()) {
        //获取当前批次的RDD的偏移量

        val offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        //拿出kafka中的数据
        val lines = kafkaRDD.map(_.value())
        //过滤出需要操作的表
        lines.filter(f => f.contains("TmsTracking.sync_order")).foreach(x => {
          val connection = HBaseConfig.getConnection()

          try {
            val json = new JsonParser()
            val obj = json.parse(x).asInstanceOf[JsonObject]
            //再次判断
            if ("TmsTracking.sync_order".equalsIgnoreCase(nvl(obj.get("ns")))) {

              var op = nvl(obj.get("op"))
              if (op.equals("i")) {
                getInsertHbasePut(x)
              } else {
                getUpdateHbasePut(x)
              }
              // hbase列族
              val column_family: String = "order"
              // dw层hbase表名
              val table_name: String = "TmsTracking_sync_order"

              val tableName = TableName.valueOf(table_name)
              val table = connection.getTable(tableName)
              var put: Put = new Put(Bytes.toBytes(id))


              if (checkNullStr(TmsId)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("TmsId"), Bytes.toBytes(TmsId))
              }
              if (checkNullStr(MainId)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("MainId"), Bytes.toBytes(MainId))
              }
              if (checkNullStr(OrderId)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("OrderId"), Bytes.toBytes(OrderId))
              }
              if (checkNullStr(TrackingNumber)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("TrackingNumber"), Bytes.toBytes(TrackingNumber))
              }
              if (checkNullStr(TrackingNumberUniqueIdentifier)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("TrackingNumberUniqueIdentifier"), Bytes.toBytes(TrackingNumberUniqueIdentifier))
              }
              if (checkNullStr(ShipToZIPCode)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("ShipToZIPCode"), Bytes.toBytes(ShipToZIPCode))
              }
              if (checkNullStr(ShipToCountry)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("ShipToCountry"), Bytes.toBytes(ShipToCountry))
              }
              if (checkNullStr(ShipToCity)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("ShipToCity"), Bytes.toBytes(ShipToCity))
              }
              if (checkNullStr(ShipToState)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("ShipToState"), Bytes.toBytes(ShipToState))
              }
              if (checkNullStr(WarehouseCode)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("WarehouseCode"), Bytes.toBytes(WarehouseCode))
              }
              if (checkNullStr(TimezoneOriginatingPlace)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("TimezoneOriginatingPlace"), Bytes.toBytes(TimezoneOriginatingPlace))
              }
              if (checkNullStr(UserId)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("UserId"), Bytes.toBytes(UserId))
              }
              if (checkNullStr(ChannelName)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("ChannelName"), Bytes.toBytes(ChannelName))
              }
              if (checkNullStr(DeliveryMethod)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("DeliveryMethod"), Bytes.toBytes(DeliveryMethod))
              }
              if (checkNullStr(RuleId)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("RuleId"), Bytes.toBytes(RuleId))
              }
              if (checkNullStr(State)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("State"), Bytes.toBytes(State))
              }
              if (checkNullStr(Code)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("Code"), Bytes.toBytes(Code))
              }
              if (checkNullStr(CreationTime)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("CreationTime"), Bytes.toBytes(CreationTime))
              }
              if (checkNullStr(UpdateTime)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("UpdateTime"), Bytes.toBytes(UpdateTime))
              }
              if (checkNullStr(SyncTime)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("SyncTime"), Bytes.toBytes(SyncTime))
              }
              if (checkNullStr(TrackingBatchNo)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("TrackingBatchNo"), Bytes.toBytes(TrackingBatchNo))
              }
              if (checkNullStr(ShardingNo)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("ShardingNo"), Bytes.toBytes(ShardingNo))
              }
              if (checkNullStr(Status)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("Status"), Bytes.toBytes(Status))
              }
              if (checkNullStr(StatusVersion)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("StatusVersion"), Bytes.toBytes(StatusVersion))
              }
              if (checkNullStr(LastTrackingTime)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("LastTrackingTime"), Bytes.toBytes(LastTrackingTime))
              }
              if (checkNullStr(TrackingHash)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("TrackingHash"), Bytes.toBytes(TrackingHash))
              }
              if (checkNullStr(TrackingChangeTimes)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("TrackingChangeTimes"), Bytes.toBytes(TrackingChangeTimes))
              }
              if (checkNullStr(TrackingErrorTimes)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("TrackingErrorTimes"), Bytes.toBytes(TrackingErrorTimes))
              }
              if (checkNullStr(TrackingChangeTime)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("TrackingChangeTime"), Bytes.toBytes(TrackingChangeTime))
              }
              if (checkNullStr(NextTrackingTime)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("NextTrackingTime"), Bytes.toBytes(NextTrackingTime))
              }
              if (checkNullStr(NextTrackingPriority)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("NextTrackingPriority"), Bytes.toBytes(NextTrackingPriority))
              }
              if (checkNullStr(AScanDate)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("AScanDate"), Bytes.toBytes(AScanDate))
              }
              if (checkNullStr(DScanDate)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("DScanDate"), Bytes.toBytes(DScanDate))
              }
              if (checkNullStr(DataSource)) {
                put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("DataSource"), Bytes.toBytes(DataSource))
              }


              put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("data_process_status"), Bytes.toBytes(data_process_status))
              put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_insert_dt"), Bytes.toBytes(DateUtil.getNow()))
              put.addColumn(Bytes.toBytes(column_family), Bytes.toBytes("w_update_dt"), Bytes.toBytes(DateUtil.getNow()))

              table.put(put)
              table.close()
            }


          } catch {
            case e: Exception =>
              Log.error("写入HBase失败，{}", e)
              throw e
          }

        })
        //提交偏移量
        kafkaDStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)

      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def getInsertHbasePut(jsonStr: String) = {
    val sync_order_insert: Sync_order_Insert = gson.fromJson(jsonStr, classOf[Sync_order_Insert])
    data_process_status = "insert"
    TmsId = nvlNull(sync_order_insert.o.TmsId)
    MainId = nvlNull(sync_order_insert.o.MainId)
    OrderId = nvlNull(sync_order_insert.o.OrderId)
    TrackingNumber = nvlNull(sync_order_insert.o.TrackingNumber)
    TrackingNumberUniqueIdentifier = nvlNull(sync_order_insert.o.TrackingNumberUniqueIdentifier)
    ShipToZIPCode = nvlNull(sync_order_insert.o.ShipToZIPCode)
    ShipToCountry = nvlNull(sync_order_insert.o.ShipToCountry)
    ShipToCity = nvlNull(sync_order_insert.o.ShipToCity)
    ShipToState = nvlNull(sync_order_insert.o.ShipToState)
    WarehouseCode = nvlNull(sync_order_insert.o.WarehouseCode)
    TimezoneOriginatingPlace = nvlNull(sync_order_insert.o.TimezoneOriginatingPlace)
    UserId = nvlNull(sync_order_insert.o.UserId)
    ChannelName = nvlNull(sync_order_insert.o.ChannelName)
    DeliveryMethod = nvlNull(sync_order_insert.o.DeliveryMethod)
    RuleId = nvlNull(sync_order_insert.o.RuleId)
    State = nvlNull(sync_order_insert.o.State)
    Code = nvlNull(sync_order_insert.o.Code)
    CreationTime = nvlDate(sync_order_insert.o.CreationTime)
    UpdateTime = nvlDate(sync_order_insert.o.UpdateTime)
    SyncTime = nvlDate(sync_order_insert.o.SyncTime)
    TrackingBatchNo = nvlNull(sync_order_insert.o.TrackingBatchNo)
    ShardingNo = nvlNull(sync_order_insert.o.ShardingNo)
    Status = if (sync_order_insert.o.Status == null) "\\N" else sync_order_insert.o.Status.mkString
    StatusVersion = nvlNull(sync_order_insert.o.StatusVersion)
    LastTrackingTime = nvlDate(sync_order_insert.o.LastTrackingTime)
    TrackingHash = nvlNull(sync_order_insert.o.TrackingHash)
    TrackingChangeTimes = nvlNull(sync_order_insert.o.TrackingChangeTimes)
    TrackingErrorTimes = nvlNull(sync_order_insert.o.TrackingErrorTimes)
    TrackingChangeTime = nvlDate(sync_order_insert.o.TrackingChangeTime)
    NextTrackingTime = nvlDate(sync_order_insert.o.NextTrackingTime)
    NextTrackingPriority = nvlNull(sync_order_insert.o.NextTrackingPriority)
    AScanDate = nvlDate(sync_order_insert.o.AScanDate)
    DScanDate = nvlDate(sync_order_insert.o.DScanDate)
    DataSource = nvlNull(sync_order_insert.o.DataSource)
    id = nvlNull(sync_order_insert.o._id.$oid)
  }

  def getUpdateHbasePut(jsonStr: String) = {
    val sync_order_update: Sync_order_update = gson.fromJson(jsonStr, classOf[Sync_order_update])
    if ("u".equalsIgnoreCase(nvlNull(sync_order_update.op))) {
      data_process_status = "update"
    } else {
      data_process_status = "delete"
    }

    TmsId = nvlNull(sync_order_update.o.$set.TmsId)
    MainId = nvlNull(sync_order_update.o.$set.MainId)
    OrderId = nvlNull(sync_order_update.o.$set.OrderId)
    TrackingNumber = nvlNull(sync_order_update.o.$set.TrackingNumber)
    TrackingNumberUniqueIdentifier = nvlNull(sync_order_update.o.$set.TrackingNumberUniqueIdentifier)
    ShipToZIPCode = nvlNull(sync_order_update.o.$set.ShipToZIPCode)
    ShipToCountry = nvlNull(sync_order_update.o.$set.ShipToCountry)
    ShipToCity = nvlNull(sync_order_update.o.$set.ShipToCity)
    ShipToState = nvlNull(sync_order_update.o.$set.ShipToState)
    WarehouseCode = nvlNull(sync_order_update.o.$set.WarehouseCode)
    TimezoneOriginatingPlace = nvlNull(sync_order_update.o.$set.TimezoneOriginatingPlace)
    UserId = nvlNull(sync_order_update.o.$set.UserId)
    ChannelName = nvlNull(sync_order_update.o.$set.ChannelName)
    DeliveryMethod = nvlNull(sync_order_update.o.$set.DeliveryMethod)
    RuleId = nvlNull(sync_order_update.o.$set.RuleId)
    State = nvlNull(sync_order_update.o.$set.State)
    Code = nvlNull(sync_order_update.o.$set.Code)
    CreationTime = nvlDate(sync_order_update.o.$set.CreationTime)
    UpdateTime = nvlDate(sync_order_update.o.$set.UpdateTime)
    SyncTime = nvlDate(sync_order_update.o.$set.SyncTime)
    TrackingBatchNo = nvlNull(sync_order_update.o.$set.TrackingBatchNo)
    ShardingNo = nvlNull(sync_order_update.o.$set.ShardingNo)
    Status = if (sync_order_update.o.$set.Status == null) "\\N" else sync_order_update.o.$set.Status.mkString
    StatusVersion = nvlNull(sync_order_update.o.$set.StatusVersion)
    LastTrackingTime = nvlDate(sync_order_update.o.$set.LastTrackingTime)
    TrackingHash = nvlNull(sync_order_update.o.$set.TrackingHash)
    TrackingChangeTimes = nvlNull(sync_order_update.o.$set.TrackingChangeTimes)
    TrackingErrorTimes = nvlNull(sync_order_update.o.$set.TrackingErrorTimes)
    TrackingChangeTime = nvlDate(sync_order_update.o.$set.TrackingChangeTime)
    NextTrackingTime = nvlDate(sync_order_update.o.$set.NextTrackingTime)
    NextTrackingPriority = nvlNull(sync_order_update.o.$set.NextTrackingPriority)
    AScanDate = nvlDate(sync_order_update.o.$set.AScanDate)
    DScanDate = nvlDate(sync_order_update.o.$set.DScanDate)
    DataSource = nvlNull(sync_order_update.o.$set.DataSource)
    id = nvlNull(sync_order_update.id)
  }

  def nvlDate(str: Sync_order_date): String = {
    if (str != null) DateUtil.getBeijingTimes(str.$date) else "\\N"
  }
  def checkNullStr(str: String): Boolean = {
    var result = false
    if (!"\\N".equalsIgnoreCase(str) && str != null) {
      result=true
    }
    result
  }



  private var data_process_status: String = _
  private var id: String = _

  private var TmsId: String = _
  private var MainId: String = _
  private var OrderId: String = _
  private var TrackingNumber: String = _
  private var TrackingNumberUniqueIdentifier: String = _
  private var ShipToZIPCode: String = _
  private var ShipToCountry: String = _
  private var ShipToCity: String = _
  private var ShipToState: String = _
  private var WarehouseCode: String = _
  private var TimezoneOriginatingPlace: String = _
  private var UserId: String = _
  private var ChannelName: String = _
  private var DeliveryMethod: String = _
  private var RuleId: String = _
  private var State: String = _
  private var Code: String = _
  private var CreationTime: String = _
  private var UpdateTime: String = _
  private var SyncTime: String = _
  private var TrackingBatchNo: String = _
  private var ShardingNo: String = _
  private var Status: String = _
  private var StatusVersion: String = _
  private var LastTrackingTime: String = _
  private var TrackingHash: String = _
  private var TrackingChangeTimes: String = _
  private var TrackingErrorTimes: String = _
  private var TrackingChangeTime: String = _
  private var NextTrackingTime: String = _
  private var NextTrackingPriority: String = _
  private var AScanDate: String = _
  private var DScanDate: String = _
  private var DataSource: String = _

}
