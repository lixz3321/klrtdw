package kl.utils;

import kl.common.InforDeserialize;
import kl.common.TableDesc;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2022/04/08/9:11
 * @Description:
 */
public class SQLUtil {
    /**
     * 组装insert语句
     * @param tabDesc
     * @return
     */
    public static String insertSQL(TableDesc tabDesc){
        StringBuffer insert_hudi_sql = new StringBuffer("insert into "+tabDesc.getTableName().toLowerCase()+" select ");
        //添加字段
        for(String field:tabDesc.getAllFields()){
            insert_hudi_sql.append(field).append(",");
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
        //from
        insert_hudi_sql.append(" from "+tabDesc.getTableName().toLowerCase()+"_kafka");
        return insert_hudi_sql.toString();
    }

    /**
     * 组装hudi表创建语句
     * @param tabDesc
     */
    public static String createHudiTableSQL(TableDesc tabDesc){
        StringBuffer create_tab_sql = new StringBuffer("create table "+tabDesc.getTableName().toLowerCase()+"(");
        //添加字段（包含etl字段）
        String[] allFields = tabDesc.getAllFields();
        String[] dataTypes = tabDesc.getDataTypes();
        for(int i=0;i<allFields.length;i++){
            String dataType = "string";
            //etl字段均用string类型
            if(i<tabDesc.getBaseFields().length){
                dataType = dataTypes[i];
            }
            create_tab_sql.append(" `"+allFields[i].toLowerCase()+"` "+dataType+",");
        }
        //去掉最后一个字段后面的逗号
        create_tab_sql.setLength(create_tab_sql.length()-1);
        //添加分区字段(如果分区数为0则不创建分区)
        int part_num = tabDesc.getPartitionNum();
        if(part_num==0){
            create_tab_sql.append(")");
        }else{
            create_tab_sql.append(",`"+tabDesc.getPartition()+"` int) partitioned by (`"+tabDesc.getPartition()+"`)");
        }
        //添加hudi表配置
        StringBuffer pk_sb = new StringBuffer();
        for(String pk:tabDesc.getPrimaryKeys()){
            pk_sb.append(pk).append(",");
        }
        pk_sb.setLength(pk_sb.length()-1);
        create_tab_sql.append(" with (   \n" +
                "  'connector' = 'hudi'  \n" +
                " ,'path' = '"+tabDesc.getTablePath()+"' \n" +
                " ,'table.type'= 'COPY_ON_WRITE' \n" +
                " ,'hoodie.datasource.write.recordkey.field' = '"+pk_sb.toString()+"' \n" +
                " ,'write.precombine.field' = '"+ InforDeserialize.etl_fields[3]+"' \n" +
                " ,'changelog.enabled'= 'true'  \n" +
                " ,'compaction.trigger.strategy' = 'num_or_time' \n" +
                " ,'compaction.delta_commits' = '30' \n" +
                " ,'hoodie.parquet.max.file.size' = '125829120' \n" +
                " ,'hoodie.parquet.small.file.limit' = '104857600' \n" +
//                " ,'hoodie.copyonwrite.insert.split.size' = '100' \n" +
                " ,'hoodie.datasource.write.hive_style_partitioning'='true' \n" +
                " ,'compaction.delta_seconds' = '3600' \n" +
                ")");
        return create_tab_sql.toString();
    }

    /**
     * 创建hudi表
     * @param tableDesc
     * @param mod
     * @return
     */
    public static String createTabSql(TableDesc tableDesc,String mod){
        String param;
        String sql="";
        switch (mod){
            case "cow1":
                param =
                        ",'table.type'= 'COPY_ON_WRITE'\n" +
                        ",'hoodie.datasource.write.hive_style_partitioning'='true'\n" +
                        ",'write.insert.cluster'='true'"+
                        //注意:bulk_insert写入操作在摄入时不提供自动调整文件大小，所以小文件会很多
                        ",'write.operation' = 'bulk_insert'\n";
                sql = createTabSqlHead(tableDesc)+createTabSqlLast(tableDesc,param);
                break;
            case "cow2":
                param =
                        ",'table.type'= 'COPY_ON_WRITE'\n" +
                        ",'write.rate.limit'= '5000'\n" +
                        ",'hoodie.datasource.write.hive_style_partitioning'='true'\n";
                sql = createTabSqlHead(tableDesc)+createTabSqlLast(tableDesc,param);
            break;
            case "cow3":
                param =
                    ",'table.type'= 'COPY_ON_WRITE'\n" +
                            ",'hoodie.datasource.write.hive_style_partitioning'='true'\n" +
                            ",'write.insert.cluster'='true'"+
                            //为避免产生小文件，设置100000行生成一个文件
                            ",'hoodie.copyonwrite.insert.auto.split'='false'"+
                            ",'hoodie.copyonwrite.insert.split.size'='100000'"+
                            ",'write.operation' = 'insert'\n";
                sql = createTabSqlHead(tableDesc)+createTabSqlLast(tableDesc,param);
                break;
            case "mor1":
                param =
                        ",'table.type'= 'MERGE_ON_READ'\n" +
                        ",'write.rate.limit'= '5000'\n" +
                        ",'hoodie.datasource.write.hive_style_partitioning'='true'\n";
                sql = createTabSqlHead(tableDesc)+createTabSqlLast(tableDesc,param);
                break;
            case "mor2":
                param =
                        ",'table.type'= 'MERGE_ON_READ'\n" +
                        ",'read.streaming.enabled'='true'"+
                        ",'changelog.enabled'='true'"+
                        ",'compaction.tasks' = '1'"+
                        ",'compaction.trigger.strategy' = 'num_or_time'"+
                        ",'compaction.delta_commits' = '30'"+
                        ",'compaction.delta_seconds' = '3600'"+
                        ",'read.streaming.check-interval'='5'"+
                        ",'hoodie.datasource.write.hive_style_partitioning'='true'\n";
                sql = createTabSqlHead(tableDesc)+createTabSqlLast(tableDesc,param);
                break;
            case "mor3":break;
            case "mor4":break;
        }
        return sql;
    }

    /**
     * 组装出Hudi建表SQl的前半部分，
     * 包括 select + 字段+分区等，
     * 不包括with关键字及后面的hudi表参数
     * @param tableDesc
     * @return 输出结果示例：create table if not exists table01_hudi(id int,name string)
     * partitiond by (etl_part)
     */
    public static String createTabSqlHead(TableDesc tableDesc){
        StringBuffer create_tab_sql = new StringBuffer("CREATE TABLE IF NOT EXISTS "+tableDesc.getTableName().toLowerCase()+"( \n");
        //添加字段（包含etl字段）
        String[] allFields = tableDesc.getAllFields();
        String[] dataTypes = tableDesc.getDataTypes();
        for(int i=0;i<allFields.length;i++){
            String dataType = "string";
            //etl字段均用string类型
            if(i<tableDesc.getBaseFields().length){
                dataType = dataTypes[i];
            }
            create_tab_sql.append(" `"+allFields[i].toLowerCase()+"` "+dataType+",\n");
        }
        //去掉最后一个字段后面的逗号
        create_tab_sql.deleteCharAt(create_tab_sql.lastIndexOf(","));
        //添加分区字段(如果分区数为0则不创建分区)
        int part_num = tableDesc.getPartitionNum();
        if(part_num==0){
            create_tab_sql.append(")");
        }else{
            create_tab_sql.append(",`"+tableDesc.getPartition()+"` int) \n PARTITIONED BY (`"+tableDesc.getPartition()+"`) \n");
        }
        return create_tab_sql.toString();
    }

    /**
     * 组装出Hudi建表SQL的后半部分，
     * 包括 with+hudi基础参数等，
     * @param tableDesc
     * @return 输出示例：with()
     */
    public static String createTabSqlLast(TableDesc tableDesc,String param){
        StringBuffer create_tab_sql = new StringBuffer();
        //表主键字段
        StringBuffer pk_sb = new StringBuffer();
        for(String pk:tableDesc.getPrimaryKeys()){
            pk_sb.append(pk).append(",");
        }
        pk_sb.setLength(pk_sb.length()-1);

        create_tab_sql.append(" WITH ( \n" +
                "  'connector' = 'hudi' \n" +
                //当两个记录主键相同时，取该字段值最大的一条
                ",'write.precombine.field'='"+InforDeserialize.etl_fields[3]+"'\n"+
                //hudi表存储路径
                " ,'path' = '"+tableDesc.getTablePath()+"'\n" +
                //其他参数
                param+
                //使用hive分区风格，使得hive能够正确识别分区目录
                " ,'hoodie.datasource.write.recordkey.field' = '"+pk_sb.toString()+"' \n" +
                ")");
        return create_tab_sql.toString();
    }

}
