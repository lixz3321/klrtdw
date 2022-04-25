package kl;


import kl.common.TableDesc;
import kl.utils.SQLUtil;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception {
        //CDC JSON测试数据
//        String str = "{\"meta_data\": [{\"name\": {\"string\": \"INFA_TABLE_NAME\"},\"value\": {\"string\": \"d8oracapt.tb01\"},\"type\": null},{\"name\": {\"string\": \"INFA_OP_TYPE\"},\"value\": {\"string\": \"UPDATE_EVENT\"},\"type\": null},{\"name\": {\"string\": \"DTL__CAPXTIMESTAMP\"},\"value\": {\"string\": \"202203291526570000000000\"},\"type\": null}],\"columns\": {\"array\": [{\"name\": {\"string\": \"id\"},\"value\": {\"string\": \"1001\"},\"isPresent\": {\"boolean\": true},\"beforeImage\": {\"string\": \"1001\"},\"isPresentBeforeImage\": {\"boolean\": true}},{\"name\": {\"string\": \"name\"},\"value\": {\"string\": \"lixz\"},\"isPresent\": {\"boolean\": true},\"beforeImage\": {\"string\": \"lixz\"},\"isPresentBeforeImage\": {\"boolean\": true}},{\"name\": {\"string\": \"age\"},\"value\": {\"string\": \"18\"},\"isPresent\": {\"boolean\": true},\"beforeImage\": {\"string\": \"18\"},\"isPresentBeforeImage\": {\"boolean\": true}},{\"name\": {\"string\": \"pay\"},\"value\": {\"string\": \"89.5\"},\"isPresent\": {\"boolean\": true},\"beforeImage\": {\"string\": \"89.5\"},\"isPresentBeforeImage\": {\"boolean\": true}},{\"name\": {\"string\": \"city\"},\"value\": {\"string\": \"beijing\"},\"isPresent\": {\"boolean\": true},\"beforeImage\": {\"string\": \"jinan\"},\"isPresentBeforeImage\": {\"boolean\": true}}]}}";
        //Flink执行环境
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
//        TableEnvironment tableEnv = TableEnvironment.create(settings);
//        TableDesc[] tableDescs = TableDesc.readTabDescCSV("ods_extract_tabs.csv");
//        String sql = SQLUtil.createBKHudiTableSQL(tableDescs[0]);
//        tableEnv.executeSql(sql);
//        tableEnv.executeSql("select * from "+tableDescs[0].getTableName().toUpperCase()+"_HUDI limit 10").print();
        //测试SQLUtil
//        TableDesc[] tableDescs = TableDesc.readTabDescCSV("ods_extract_tabs.csv");
//        String sql = SQLUtil.createTabSql(tableDescs[0],"mor2");
//        System.out.println(sql);
        //java测试
        System.out.println(65969104-63390687);
        System.out.println(65969693-63488685);
        System.out.println(65973944-63929786);
        System.out.println(65976170-64985185);
        System.out.println(65977419-65296504);
        System.out.println(65334641-65296504);

    }
}
