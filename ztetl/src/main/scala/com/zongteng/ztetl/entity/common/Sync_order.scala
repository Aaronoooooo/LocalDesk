package com.zongteng.ztetl.entity.common

case class Sync_order_title(
                             TmsId: String,
                             MainId: String,
                             OrderId: String,
                             TrackingNumber: String,
                             TrackingNumberUniqueIdentifier: String,
                             ShipToZIPCode: String,
                             ShipToCountry: String,
                             ShipToCity: String,
                             ShipToState: String,
                             WarehouseCode: String,
                             TimezoneOriginatingPlace: String,
                             UserId: String,
                             ChannelName: String,
                             DeliveryMethod: String,
                             RuleId: String,
                             State: String,
                             Code: String,
                             CreationTime: Sync_order_date,
                             UpdateTime: Sync_order_date,
                             SyncTime: Sync_order_date,
                             TrackingBatchNo: String,
                             ShardingNo: String,
                             Status: Array[Object],
                             StatusVersion: String,
                             LastTrackingTime: Sync_order_date,
                             TrackingHash: String,
                             TrackingChangeTimes: String,
                             TrackingErrorTimes: String,
                             TrackingChangeTime: Sync_order_date,
                             NextTrackingTime: Sync_order_date,
                             NextTrackingPriority: String,
                             AScanDate: Sync_order_date,
                             DScanDate: Sync_order_date,
                             DataSource: String,
                             _id: Sync_order_Id
                           )


  case class Sync_order_Status(
                                Code: String,
                                Info: String,
                                Location: String,
                                OriginalCode: String,
                                OriginalTimestamp: Sync_order_date,
                                SubCode: String,
                                Timestamp: Sync_order_date
                              )

  case class Sync_order_date($date: String)

  case class Sync_order_Id($oid: String)



  case class Sync_order_update_o(
                                  $set: Sync_order_title,
                                  $V: String
                                )

  case class Sync_order_Insert(
                                id: String,
                                ns: String,
                                o: Sync_order_title,
                                op: String,
                                ts: String
                              )

  case class Sync_order_update(
                                id: String,
                                ns: String,
                                o: Sync_order_update_o,
                                op: String,
                                ts: String
                              )

