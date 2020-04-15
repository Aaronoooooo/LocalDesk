package com.fwmagic.flink.selfpartition;

import org.apache.flink.api.common.functions.Partitioner;

public class MyPartition implements Partitioner<Long> {

    @Override
    public int partition(Long value, int numPartitions) {
        System.out.println("分区总数："+numPartitions);
        if(value % 2 ==0){
            return 0;
        }else{
            return 1;
        }
    }
}
