package kl.etl.dws;

import kl.common.TableDesc;
import kl.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: Apache
 * @Date: 2022/04/14/16:34
 * @Description:
 */
public class StreamReadMOR {
    public static void main(String[] args){
        /*
        Flink 执行环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //注册hudi表
        TableDesc[] tableDescs = TableDesc.readTabDescCSV("ods_extract_tabs.csv");
        String sql = SQLUtil.createTabSql(tableDescs[0], "mor2");
        tEnv.executeSql(sql);
        //注册kafka表
        String kafka_sql = "CREATE TABLE KafkaTable (\n" +
                "  `contno` STRING,\n" +
                "  `appntname` STRING,\n" +
                "  `etl_kafka_offset` STRING\n" +
//                "  `ts` TIMESTAMP(3) METADATA FROM 'timestamp'\n" +
                ") WITH (\n" +
                "  'connector' = 'upsert-kafka',\n" +
                "  'topic' = 'listables-tmp',\n" +
                "  'properties.bootstrap.servers' = 'awnx1-cdata-pnode06:9092',\n" +
                "  'properties.group.id' = 'testGroup',\n" +
                "  'key.format'='json',"+
                "  'scan.startup.mode' = 'latest-offset',\n" +
                "  'value.format' = 'json'\n" +
                ")";
        tEnv.executeSql(kafka_sql);
        //输出到kafka表
//        tEnv.executeSql("select contno,appntname,agentcom,grpagentname,etl_kafka_offset,etl_kafka_timestamp from "+tableDescs[0].getTableName()).print();
        tEnv.executeSql("insert into KafkaTable select contno,appntname,etl_kafka_timestamp from "+tableDescs[0].getTableName());
    }
}
