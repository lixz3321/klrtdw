package kl.etl.ods;

import kl.common.TableDesc;
import kl.func.KL_BUCKET;
import kl.utils.SQLUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connectors.hive.HiveOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import static kl.common.InforDeserialize.etl_fields;

/**
 *
 * @Auther: lixz
 * @Date: 2022/04/01/13:26
 * @Description: Hudi表数据初始化作业，可将多张Hive表数据导入至Hudi
 *
 * 注意：
 * 1、开源的flink-sql-connector-hive-3.1.2_2.11-1.13.6.jar包内打包了guava包的依赖，
 * 会导致在HDP集群环境中任务执行失败，所以需要删掉link-sql-connector-hive-3.1.2_2.11-1.13.6.jar
 * 包内的com\google\common\base目录
 * 2、查询hive内部表可能报错“Reading or writing ACID table default.t1 is not supported”，需要hive修改相关配置
 * 3、任务资源要保证充足，批量读hive极容易把内存沾满，可以添加以下参数：
 * -Djobmanager.memory.process.size=2048mb
 * -Dtaskmanager.memory.process.size=4096mb
 * -Dtaskmanager.numberOfTaskSlots=4
 * 并根据任务执行时Task Manager内存模型监控
 * 进行调整
 *
 */


public class BatchJob {

    /**
     * 执行入口
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //hive数据库
        String hive_database = parameterTool.get("database");
        //hive抽取行数限制（可不设置）
        String row_limit = parameterTool.get("limit");
        //hudi建表模式
        String mod = parameterTool.get("mod");
        //Flink执行环境
        //方式一：批模式（可快速大批量写，但易产生小文件）
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        //方式二：流模式
        //虽然是批量任务，但由于用批处理执行insert/bulk_insert会有小文件问题以及用flink批处理的upsert可能任务繁重，所以决定用流模式跑upsert，这样可以设置1分钟的检查点，
        // 边读边往hudi写，而不是像flink批模式那样整表都读到内存再写hudi
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE);
//        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
//        env.getCheckpointConfig().setCheckpointStorage("hdfs://klbigdata:8020/flink/ckp_klrtdw_ods");
        //设置HiveSource读取时的并行度最大值
        tableEnv.getConfig().getConfiguration().setInteger(HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX,20);
        //注册取模函数
        tableEnv.createTemporarySystemFunction("KL_BUCKET", KL_BUCKET.class);
        //设置hive catalog
        String name            = "hive";
        String hiveConfDir     = "/usr/hdp/3.1.5.0-152/hive/conf";
        HiveCatalog hive = new HiveCatalog(name, hive_database, hiveConfDir);
        tableEnv.registerCatalog("hive", hive);
        //设置Fink使用Hive的Catalog，这样在Flink上建的表在Hive中也能看到，Flink的默认Catalog名为default_catalog，默认数据库名为default_database
//        tableEnv.useCatalog("hive");
        //获取表配置描述对象
        TableDesc[] tablesDesc = TableDesc.readTabDescCSV("ods_extract_tabs.csv");
        //注册多个hudi表(不建议使用bulk_insert模式)
        for(TableDesc tabDesc:tablesDesc){
//            String create_hudi_sql = SQLUtil.createBKHudiTableSQL(tabDesc);
            String create_hudi_sql = SQLUtil.createTabSql(tabDesc,mod);
            //执行创建hudi表SQL
            tableEnv.executeSql(create_hudi_sql.toString());
        }
        //写入Hudi
        for(TableDesc tabDesc:tablesDesc){
            //组装insert hudi语句
            StringBuffer insert_hudi_sql = new StringBuffer("insert into "+tabDesc.getTableName().toLowerCase()+" select ");
            //添加基础字段
            for(String field:tabDesc.getBaseFields()){
                insert_hudi_sql.append(field).append(",");
            }
            //添加etl字段
            for(String etl_field:etl_fields){
                insert_hudi_sql.append("'0' as "+etl_field.toLowerCase()).append(",");
            }
            //去掉末尾逗号
            insert_hudi_sql.setLength(insert_hudi_sql.length()-1);
            //添加分区字段
            StringBuffer cast_pk_sb = new StringBuffer();
            for(String pk:tabDesc.getPrimaryKeys()){
                cast_pk_sb.append("cast("+pk+" as string),");
            }
            cast_pk_sb.setLength(cast_pk_sb.length()-1);
            if(tabDesc.getPartitionNum()!=0){
                insert_hudi_sql.append(",KL_BUCKET(concat("+cast_pk_sb.toString()+"),"+tabDesc.getPartitionNum()+") as "+tabDesc.getPartition());
            }
            //from (表前缀需指定catalog和数据库名称)
            String limit = "";
            if(row_limit!=null && !row_limit.equals("")){
                limit = " limit "+row_limit;
            }
            insert_hudi_sql.append(" from hive."+hive_database+"."+tabDesc.getTableName().toLowerCase()+limit);
            //执行insert
            tableEnv.sqlUpdate(insert_hudi_sql.toString());
        }
        //启动作业
        tableEnv.execute("ODS_TABLE_INIT");
    }
}
