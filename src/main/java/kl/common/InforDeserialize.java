package kl.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import kl.utils.CDCUtil;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.io.IOException;
import java.util.HashMap;

/**
 *
 * @Auther: lixz
 * @Date: 2022/04/06/10:24
 * @Description: kafka infor CDC消息序列化类
 * 在序列化过程中将kafka消息主题、分区、偏移量、时间戳，以及cdc数据时间戳等信息添加到json串的columns节点下
 * ，这些信息将和普通数据字段一样进行存放，所以可以像查询普通字段一样查询这些信息，字段名由该类中的etl_fields常量指定
 */
public class InforDeserialize implements KafkaRecordDeserializationSchema<String> {
    /*
    etl字段（该字段不要轻易改动，各个模块中会使用）
     */
    public static final String[] etl_fields = {"etl_kafka_topic","etl_kafka_partition","etl_kafka_offset","etl_kafka_timestamp","etl_cdc_timestamp"};

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {}
    @Override
    public TypeInformation<String> getProducedType() { return BasicTypeInfo.STRING_TYPE_INFO;}
    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<String> out) throws IOException {
        String value = new String(record.value());
        JSONObject json = null;
        try{
            json = JSON.parseObject(value);
            //将偏移量、分区号、时间戳、数据时间戳等信息填充到cdc json里(columns层)
            CDCUtil.columnsDataAdd(json,etl_fields[0].toUpperCase(),String.valueOf(record.topic()));
            CDCUtil.columnsDataAdd(json,etl_fields[1].toUpperCase(),String.valueOf(record.partition()));
            CDCUtil.columnsDataAdd(json,etl_fields[2].toUpperCase(),String.valueOf(record.offset()));
            CDCUtil.columnsDataAdd(json,etl_fields[3].toUpperCase(),String.valueOf(record.timestamp()));
            CDCUtil.columnsDataAdd(json,etl_fields[4].toUpperCase(),((HashMap<String,String>)CDCUtil.ParseCDCJson(value).get("meta_data")).get("DTL__CAPXTIMESTAMP"));
            out.collect(json.toJSONString());
        }catch (Exception e){
            e.printStackTrace();
            System.out.println("CDC消息格式不合法!");
        }
    }
}
