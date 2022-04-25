package kl.common;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Stack;

import static kl.common.InforDeserialize.etl_fields;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2022/03/25/10:01
 * @Description: 表描述信息对象，用于CDC同步HUDI表动态配置表信息
 * CSV格式示例：表名,表标识(cdc表名),hudi表路径,字段1#字段2,字段1类型#字段2类型,主键字段1#主键字段2,分区字段,分区数
 * 【tb01,d8oracapt.tb01,hdfs://klbigdata:8020/tmp/tb01,id#name#age#pay#city,int#string#int#double#string,id#age,etl_partition,32】
 */
public class TableDesc implements Serializable {
    /*
    hudi表名
     */
    private String tableName;
    /*
    infor-cdc表标识
     */
    private String tableTag;
    /*
    hudi表路径
     */
    private String tablePath;
    /*
    分区字段
     */
    private String partition;
    /*
    固定分区数
     */
    private int partitionNum;
    /*
   表字段（基础字段，不含ETL字段）
    */
    private String[] baseFields;
    /*
    字段数据类型
     */
    private String[] dataTypes;
    /*
    主键字段
     */
    private String[] primaryKeys;


    /*
    seter/geter
     */

    public String getTableName() {
        return tableName;
    }

    public String getTableTag() {
        return tableTag;
    }

    public String getTablePath() {
        return tablePath;
    }

    public String getPartition() {
        return partition;
    }

    public int getPartitionNum() {
        return partitionNum;
    }

    public String[] getBaseFields() {
        return baseFields;
    }

    public String[] getDataTypes() {
        return dataTypes;
    }

    public String[] getPrimaryKeys() {
        return primaryKeys;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setTableTag(String tableTag) {
        this.tableTag = tableTag;
    }

    public void setTablePath(String tablePath) {
        this.tablePath = tablePath;
    }

    public void setPartition(String partition) {
        this.partition = partition;
    }

    public void setPartitionNum(int partitionNum) {
        this.partitionNum = partitionNum;
    }

    public void setBaseFields(String[] baseFields) {
        this.baseFields = baseFields;
    }

    public void setDataTypes(String[] dataTypes) {
        this.dataTypes = dataTypes;
    }

    public void setPrimaryKeys(String[] primaryKeys) {
        this.primaryKeys = primaryKeys;
    }

    /*
            私有化构造
             */
    private TableDesc(){}

    @Override
    public String toString() {
        return "TableDesc{" +
                "tableName='" + tableName + '\'' +
                ", tableTag='" + tableTag + '\'' +
                ", tablePath='" + tablePath + '\'' +
                ", partition='" + partition + '\'' +
                ", partitionNum=" + partitionNum +
                ", baseFields=" + Arrays.toString(baseFields) +
                ", dataTypes=" + Arrays.toString(dataTypes) +
                ", primaryKeys=" + Arrays.toString(primaryKeys) +
                '}';
    }

    /**
     * 生成flink中定义schema的主键
     *例如：f1,f2
     * @return
     */
    public String[] getSmPk(){
        String[] res = new String[primaryKeys.length];
        //主键字段名入栈
        Stack<String> stack = new Stack();
        for(String pkNoName:primaryKeys){
            stack.push(pkNoName);
        }
        //找出栈中主键字段在字段数组中所匹配到的位置并生成主键
        int res_index = 0;
        for(int i=0;i<baseFields.length;i++){
            if(stack.empty()){
                break;
            }
            String smpk = stack.peek();
            if(smpk.equals(baseFields[i])){
                res[res_index]="f"+(++i);
                //匹配到后弹出栈
                stack.pop();
                res_index++;
                //重新for循环
                i=-1;
            }
        }
        return res;
    }

    /**
     * 读取csv文件获取表配置信息
     * 格式示例：表名,表标识(cdc表名),hudi表路径,字段1#字段2,字段1类型#字段2类型,主键字段1#主键字段2,分区字段,分区数
     * 【tb01,d8oracapt.tb01,hdfs://klbigdata:8020/tmp/tb01,id#name#age#pay#city,int#string#int#double#string,id#age,etl_partition,32】
     * 注意：1.字段类型为Flink数据类型
     *       2.如果不想设置分区，分区数需设置为0
     *       3.表名、字段名、数据类型等配置均用小写
     * @param csv 文件名
     * @return
     */
    public static TableDesc[] readTabDescCSV(String csv){
        ArrayList<TableDesc> tableDescs = new ArrayList<>();
        InputStream in;
        BufferedReader br = null;
        try {
            in = TableDesc.class.getClassLoader().getResource(csv).openStream();
            br = new BufferedReader(new InputStreamReader(in));
            String line="";
            while((line=br.readLine())!=null) {
                String[] split = line.split(",");
                TableDesc tableDesc = null;
                if(split.length==8){
                    tableDesc = new TableDesc();
                    tableDesc.setTableName(split[0]);
                    tableDesc.setTableTag(split[1]);
                    tableDesc.setTablePath(split[2]);
                    tableDesc.setBaseFields(split[3].split("#"));
                    tableDesc.setDataTypes(split[4].split("#"));
                    tableDesc.setPrimaryKeys(split[5].split("#"));
                    tableDesc.setPartition(split[6]);
                    tableDesc.setPartitionNum(Integer.valueOf(split[7]));
                }else{
                    System.out.println("表描述配置格式有误，配置列数目不符合预期，请重新配置后启动作业");
                    return null;
                }
                tableDescs.add(tableDesc);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e1) {
                }
            }
        }
        TableDesc[] res = new TableDesc[tableDescs.size()];
        return tableDescs.toArray(res);
    }

    /**
     * 获取所有字段名称（包含ETL字段）
     * @return
     */
    public String[] getAllFields(){
        return (String[])ArrayUtils.addAll(baseFields,etl_fields);
    }

    /**
     * 获取kafka cdc数据字段（含etl）类型定义对象
     * etl字段类型均为String
     * @return
     */
    public RowTypeInfo getRowTypeInfo(){
        String[] baseFields = getBaseFields();
        TypeInformation[] col_types = new TypeInformation[baseFields.length+etl_fields.length];
        //设置基础字段数据类型（严格有序）
        for(int i=0;i<baseFields.length;i++){
            String dataType = dataTypes[i];
            switch (dataType){
                case "int":
                    col_types[i] = Types.INT;
                    break;
                case "string":
                    col_types[i] = Types.STRING;
                    break;
                case "double":
                    col_types[i] = Types.DOUBLE;
                    break;
            }
        }
        //设置ETL字段数据类型（严格有序）
        for(int i=0;i<etl_fields.length;i++){
            col_types[i+baseFields.length] = Types.STRING;
        }
        RowTypeInfo rowType = new RowTypeInfo(col_types);
        return rowType;
    }
}
