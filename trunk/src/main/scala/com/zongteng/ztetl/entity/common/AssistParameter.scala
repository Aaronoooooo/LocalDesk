package com.zongteng.ztetl.entity.common

case class AssistParameter(
                            className: Class[_], // 样例类对应Class对象
                            primaryKey: String, // 主键
                            cf: String, // Hbase对应列族
                            datasource_num_id: String, // 数据来源
                            add_time_field: String, // mysql数据源新增时间
                            update_time_field_1: String, // mysql数据源时间戳
                            update_time_field_2: String, // mysql数据源修改时间
                            table: String,
                            database: String
                          )
