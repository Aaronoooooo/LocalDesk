package com.zongteng.ztetl.entity.common

import java.sql.Timestamp

case class SysOrder(
                     _id: String,
                     TmsId: Long,
                     MainId: String,
                     OrderId: String,
                     TrackingNumber: String,
                     TrackingNumberUniqueIdentifier: String,
                     ShipToZIPCode: String,
                     ShipToCountry: String,
                     ShipToCity: String,
                     ShipToState: String,
                     WarehouseCode: String,
                     TimezoneOriginatingPlace: Int,
                     UserId: Int,
                     ChannelName: String,
                     DeliveryMethod: String,
                     RuleId: String,
                     State: Int,
                     Code: String,
                     CreationTime: Timestamp,
                     UpdateTime: Timestamp,
                     SyncTime: Timestamp,
                     TrackingBatchNo: String,
                     ShardingNo: Long,
                     Status: String,
                     StatusVersion: String,
                     LastTrackingTime: Timestamp,
                     TrackingHash: String,
                     TrackingChangeTimes: Int,
                     TrackingErrorTimes: Int,
                     TrackingChangeTime: Timestamp,
                     NextTrackingTime: Timestamp,
                     NextTrackingPriority: Int,
                     AScanDate: Timestamp,
                     DScanDate: Timestamp,
                     DataSource: String
                   )
