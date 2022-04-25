package kl.etl.ods;

import jodd.util.ArraysUtil;
import kl.common.InforDeserialize;
import kl.common.InforData;
import kl.common.TableDesc;
import kl.func.KL_BUCKET;
import kl.utils.SQLUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Arrays;

import static kl.utils.SQLUtil.createHudiTableSQL;
import static kl.utils.SQLUtil.insertSQL;

/**
 *
 * @Auther: lixz
 * @Date: 2022/03/09/10:32
 * @Description: 实时同步oracle ods层表数据到hudi ods层
 * 接收kafka中的informatic-cdc数据，并以changelog方式写入hudi
 */
public class StreamJob {

    /*
    日志
     */
    public static Logger log = LoggerFactory.getLogger(StreamJob.class.getClass());


    /**
     * 执行入口
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        /*
        作业参数
         */
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        if(parameterTool.has("help")){
            System.out.println("参数解释：\n" +
                    "--topic kafka消费主题\n"+
                    "--group kafka消费组ID\n"+
                    "--broker kafka broker地址\n"+
                    "--ckp checkpint在hdfs上的保存路径\n"+
                    "--csv 表配置文件（csv格式）\n"+
                    "--interval checkpoint时间间隔\n"
            );
            return;
        }
        //kafka 消费主题
        final String[] topics = {parameterTool.get("topic")};
        //kafka 消费组
        String consumer_group = parameterTool.get("group");
        //kafka broker 地址
        String kafka_broker = parameterTool.get("broker");
        //checkpoint 在HDFS上的保存路径
        String ckp_path = parameterTool.get("ckp");
        //表配置文件
        String table_desc_path = parameterTool.get("csv");
        //checkpoint 时间间隔
        long interval = Long.valueOf(parameterTool.get("interval"));

        /*
        Flink 执行环境
         */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(interval, CheckpointingMode.AT_LEAST_ONCE);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage(ckp_path);
         /*
        注册自定义函数
         */
        //哈希值取模函数
        tEnv.createTemporarySystemFunction("KL_BUCKET", KL_BUCKET.class);
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
//                .setStartingOffsets(OffsetsInitializer.latest())
                //使用自定义序列化器，以获取kafka消息中的偏移量、时间戳等额外信息
                .setDeserializer(new InforDeserialize())
                .build();
        /*
        原始流
         */
        DataStream<String> kafka_josn_ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source");
        /*
        加载表配置描述信息
         */
        TableDesc[] tableDescs = TableDesc.readTabDescCSV(table_desc_path);
        if(tableDescs==null) return;
        /*
        创建分流标签
         */
        OutputTag<InforData>[] side_ds_tags = new OutputTag[tableDescs.length];
        for(int i=0;i<tableDescs.length;i++){
            side_ds_tags[i] = new OutputTag<InforData>("side-ds-"+i){};
        }
        /*
        对数据打标签以便分流
         */
        DataStream<InforData> out = kafka_josn_ds.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                //过滤掉空数据
                if (s == null || s.equals("")) {
                    return false;
                }
                return true;
            }
        }).process(new ProcessFunction<String, InforData>() {
            @Override
            public void processElement(String s, Context context, Collector<InforData> collector) throws Exception {
                //从当前消息中获取CDC表标识
                InforData data = null;
                try {
                    data = new InforData(s);
                }catch (Exception e){
                    e.printStackTrace();
                    return;
                }
                String table_tag = data.getTabTag();
                //循环匹配CDC表标识
                for (int i = 0; i < tableDescs.length; i++) {
                    if (table_tag.equals(tableDescs[i].getTableTag())) {
                        //对属于某张表的数据打上该表的分流标签
                        context.output(side_ds_tags[i], data);
                        break;
                    }
                }
            }
        });
        /*
        分流入湖
         */
        for(int i=0;i<side_ds_tags.length;i++){
            DataStream<InforData> side_ds = ((SingleOutputStreamOperator<InforData>) out).getSideOutput(side_ds_tags[i]);
            writeToHudi(tEnv,side_ds,tableDescs[i]);
        }
        //打印
//        tEnv.executeSql("select * from tb01_kafka").print();
        //启动
        tEnv.execute("ODS_ETL_JOB");
    }

    /**
     * 接入某旁路输出流并解析写入相应的hudi表
     * @param tEnv
     * @param side_ds 旁路输出
     * @param tabDesc 表表述对象
     */
    public static void writeToHudi(StreamTableEnvironment tEnv,DataStream<InforData> side_ds,TableDesc tabDesc){
        //注册kafka表
        createKafkaTable(tEnv,side_ds,tabDesc);
        //注册hudi表
//        String create_hudi_tab_sql = createHudiTableSQL(tabDesc);
        String create_hudi_tab_sql = SQLUtil.createTabSql(tabDesc,"mor2");
        tEnv.executeSql(create_hudi_tab_sql);
        //组装insert hudi语句
        String insert_hudi_sql = insertSQL(tabDesc);
        //执行insert
        tEnv.sqlUpdate(insert_hudi_sql);
    }

    /**
     * 注册changlog流动态表
     * @param tEnv
     * @param side_ds
     * @param tabDesc
     */
    public static void createKafkaTable(StreamTableEnvironment tEnv,DataStream<InforData> side_ds,TableDesc tabDesc){
        DataStream<Row> row_ds = side_ds.flatMap(new FlatMapFunction<InforData, Row>() {
            @Override
            public void flatMap(InforData data, Collector<Row> collector) throws Exception {
                Row[] rows = data.toRow(tabDesc);
                for(Row row:rows){
                    collector.collect(row);
                }
            }
        }).returns(tabDesc.getRowTypeInfo());
        //构建schema并设置主键
        Schema schema = Schema.newBuilder().primaryKey(tabDesc.getSmPk()).build();
        //字段重命名为实际字段名
        String[] allFieldNames = tabDesc.getAllFields();
        Table tb = tEnv.fromChangelogStream(row_ds,schema)
                //重命名列（按顺序）
                .as(allFieldNames[0], Arrays.copyOfRange(allFieldNames,1,allFieldNames.length));
        //注册分流动态表(表名小写)
        tEnv.createTemporaryView(tabDesc.getTableName().toLowerCase()+"_kafka", tb);
    }
}
