package com.vince.xq.mapper;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.vince.xq.model.OrgModel;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.util.Collector;

/**
 * @author
 * @Date 2019-11-13 15:09
 **/

@Slf4j
public class OrgMapper extends RichFlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> collector) throws Exception {
        System.out.println("==value:" + value);
        JSONObject jsonObject = JSONObject.parseObject(value);
        JSONArray jsonArray = JSONArray.parseArray(jsonObject.get("data").toString());
        JSONObject object = JSONObject.parseObject(jsonArray.get(0).toString());
        System.out.println(object.get("orgId"));
        OrgModel orgModel = new OrgModel();
        orgModel.setId(object.getString("id"));
        orgModel.setOrgId(object.getString("orgId"));
        orgModel.setName(object.getString("name"));
        collector.collect(JSONObject.toJSONString(orgModel));
    }
}
