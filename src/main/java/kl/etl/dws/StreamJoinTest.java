package kl.etl.dws;

import kl.common.InforDeserialize;
import kl.common.TableDesc;
import kl.utils.SQLUtil;
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

import static kl.common.InforDeserialize.etl_fields;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: Apache
 * @Date: 2022/04/12/10:50
 * @Description: Flink消费kafka，实时关联hudi表
 * 注意：关联hudi表查询并不是lookup，而是预加载整个hudi表到内存里，官方也做出解释，目前hudi（0.10.1）
 * 还不支持维表
 */
public class StreamJoinTest {
    public static void main(String[] args){
         /*
        作业参数
         */
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if(parameterTool.has("help")){
            System.out.println("参数解释：\n" +
                    "--topic kafka消费主题\n"+
                    "--group kafka消费组ID\n"+
                    "--broker kafka broker地址\n"+
                    "--csv 表配置文件（csv格式）\n"
            );
            return;
        }
        //kafka 消费主题
        final String[] topics = {parameterTool.get("topic")};
        //kafka 消费组
        String consumer_group = parameterTool.get("group");
        //kafka broker 地址
        String kafka_broker = parameterTool.get("broker");
        //表配置文件
        String table_desc_path = parameterTool.get("csv");

        /*
        Flink 执行环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        /*
        数据源：kafka
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
        /*
        原始流
         */
        DataStream<String> kafka_ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        TypeInformation[] col_types = new TypeInformation[]{Types.STRING};
        DataStream<Row> row_ds = kafka_ds.flatMap(new FlatMapFunction<String, Row>() {
            @Override
            public void flatMap(String s, Collector<Row> collector) throws Exception {
                collector.collect(Row.ofKind(RowKind.INSERT, s));
            }
        }).returns(new RowTypeInfo(col_types));
        //构建schema并设置主键
        Schema schema = Schema.newBuilder().build();
        Table tb = tEnv.fromChangelogStream(row_ds,schema)
                //重命名列（按顺序）
                .as("id");
        tEnv.createTemporaryView("t1", tb);

        //注册所要查询的hudi表
        TableDesc[] tableDescs = TableDesc.readTabDescCSV("ods_extract_tabs.csv");
        String hudi_crt_sql = SQLUtil.createHudiTableSQL(tableDescs[0]);
        tEnv.executeSql(hudi_crt_sql);
//        tEnv.executeSql("select * from t1").print();
        //最终查询的字段少，预加载hudi表的数据就少，消耗内存就小
        tEnv.executeSql("select h1.insuredname from t1 join o_lis_lcpol h1 on t1.id = h1.polno").print();
    }
}
