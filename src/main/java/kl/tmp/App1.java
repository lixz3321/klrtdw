package kl.tmp;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: Apache
 * @Date: 2022/04/15/14:37
 * @Description:
 */
public class App1 {
    public static void main(String[] args){
        //Flink执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);
        //自定义hbase source
        RichSourceFunction<String> sa = new RichSourceFunction<String>() {
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                System.setProperty("sun.security.krb5.debug", "true");
            }

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {

            }

            @Override
            public void close() throws Exception {
                super.close();
            }

            @Override
            public void cancel() {

            }
        };
    }
}
