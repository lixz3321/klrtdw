package kl.tmp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: Apache
 * @Date: 2022/03/21/10:16
 * @Description: kafka & hudi
 */
public class App2 {
    public static void main(String[] args){
         /*
        作业参数
         */
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //kafka 消费主题
        final String[] topics = {parameterTool.get("topic")};
        //kafka 消费组
        String consumer_group = parameterTool.get("group");
        //kafka broker 地址
        String kafka_broker = parameterTool.get("broker");
        //checkpoint 在HDFS上的保存路径
        String ckp_path = parameterTool.get("ckp");
         /*
        Flink 执行环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(60000, CheckpointingMode.AT_LEAST_ONCE);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage(ckp_path);
        /*
        接入kafka
         */
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(kafka_broker)
                .setTopics(topics)
                .setGroupId(consumer_group)
                .setProperty("security.protocol","SASL_PLAINTEXT")
                .setProperty("sasl.mechanism","GSSAPI")
                .setProperty("sasl.kerberos.service.name","kafka")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        TypeInformation[] col_types = new TypeInformation[]{Types.INT,Types.STRING,Types.DOUBLE};
        RowTypeInfo rowType = new RowTypeInfo(col_types);
        //id,name,pay@id,name,pay@optype，解释：新记录@旧记录@操作类型
        //示例：
        //+I 1,aaa,10.5@null@i
        //+U 1,bbb,10.5@1,aaa,10.5@u
        //+D 1,aaa,10.5@null@d
        DataStream<Row> row_ds = ds.flatMap(new FlatMapFunction<String, Row>() {
            @Override
            public void flatMap(String s, Collector<Row> collector) throws Exception {
                String[] dataArr = s.split("@");
                String optype = dataArr[2];
                String[] newRow = dataArr[0].split(",");
                String[] oldRow = dataArr[1].split(",");
                switch (optype){
                    case "i"://新增
                        collector.collect(Row.ofKind(RowKind.INSERT, Integer.valueOf(newRow[0]), newRow[1],Double.valueOf(newRow[2])));
                        break;
                    case "u":
                        collector.collect(Row.ofKind(RowKind.UPDATE_BEFORE, Integer.valueOf(oldRow[0]), oldRow[1],Double.valueOf(oldRow[2])));
                        collector.collect(Row.ofKind(RowKind.UPDATE_AFTER, Integer.valueOf(newRow[0]), newRow[1],Double.valueOf(newRow[2])));
                        break;
                    case "d":
                        collector.collect(Row.ofKind(RowKind.DELETE, Integer.valueOf(newRow[0]), newRow[1],Double.valueOf(newRow[2])));
                        break;
                }
//                String[] arr = s.split(",");
//                collector.collect(Row.ofKind(RowKind.INSERT, Integer.valueOf(arr[0]), arr[1],Double.valueOf(arr[2])));
            }
        }).returns(rowType);
        //schema
        //构建schema并设置主键
        Schema schema = Schema.newBuilder().primaryKey("f1").build();
        //字段重命名为实际字段名
        Table tb = tEnv.fromChangelogStream(row_ds,schema)
                //重命名列（按顺序）
                .as("id","name","pay");
        //注册分流动态表(表名小写)
        tEnv.createTemporaryView("tb1", tb);
//        tEnv.executeSql("select * from tb1").print();
        //注册hudi表
        tEnv.executeSql("CREATE TABLE tb2(\n" +
                "  id int\n" +
                " ,name string\n" +
                " ,pay double\n" +
                ")\n" +
                "WITH (\n" +
                "  'connector' = 'hudi'\n" +
                ",'read.streaming.enabled'='true'\n"+
                ",'changelog.enabled'='true'\n"+
                ",'compaction.tasks' = '1'\n"+
                ",'compaction.trigger.strategy' = 'num_or_time'\n"+
                ",'compaction.delta_commits' = '30'\n"+
                ",'compaction.delta_seconds' = '3600'\n"+
                ",'read.streaming.check-interval'='5'\n"+
                " ,'path' = 'hdfs://klbigdata:8020/tmp/tb01'\n" +
                " ,'hoodie.datasource.write.recordkey.field' = 'id'\n" +
                " ,'table.type' = 'MERGE_ON_READ'\n" +
                ")");
        tEnv.executeSql("insert into tb2 select id,name,pay from tb1");
    }
}
