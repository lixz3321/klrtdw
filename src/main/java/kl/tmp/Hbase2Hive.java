package kl.tmp;

import kl.func.KL_BUCKET;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connectors.hive.HiveOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: Apache
 * @Date: 2022/04/20/16:21
 * @Description: 失败，无法查询Hbase
 */
public class Hbase2Hive {
    public static void main(String[] args){
        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        //Flink执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        //设置HiveSource读取时的并行度最大值
//        tableEnv.getConfig().getConfiguration().setInteger(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX,20);
//        //设置hive catalog
//        String name            = "hive";
//        String hiveConfDir     = "/usr/hdp/3.1.5.0-152/hive/conf";
//        HiveCatalog hive = new HiveCatalog(name, "tmp", hiveConfDir);
//        tableEnv.registerCatalog("hive", hive);
//        tableEnv.executeSql("select * from hive.tmp.hbase_test").print();
        //注册Hbase表
        tableEnv.executeSql("CREATE TABLE hTable (\n" +
                " rowkey STRING,\n" +
                " f ROW<q STRING>,\n" +
                " PRIMARY KEY (rowkey) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'hbase-2.2',\n" +
                " 'table-name' = 'SAS:test1',\n" +
                " 'zookeeper.znode.parent' = '/hbase-secure',\n" +
                " 'zookeeper.quorum' = 'ip-10-129-24-101.cn-northwest-1.compute.internal:5181'\n" +
                ")");
        tableEnv.executeSql("select * from hTable").print();
    }
}
