package kl.common;

import kl.etl.ods.StreamJob;
import kl.utils.CDCUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sound.sampled.Line;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2022/04/06/9:55
 * @Description: Informatic cdc数据封装对象
 */
public class InforData {
    /*
    日志
     */
    public Logger log = LoggerFactory.getLogger(StreamJob.class.getClass());
    /*
    meta
     */
    HashMap<String,String> meta_data;
    /*
    columns (数据体)
     */
    HashMap<String, ArrayList<String>> columns;

    /**
     * 构造
     * @param msg
     * @throws Exception
     */
    public InforData(String msg) throws Exception {
        HashMap<String,Object> cdc = CDCUtil.ParseCDCJson(msg);
        this.meta_data = (HashMap<String,String>)cdc.get("meta_data");
        this.columns = (HashMap<String,ArrayList<String>>)cdc.get("columns");
    }


    /**
     * 取出列字段值(after)
     * @param key
     * @return
     */
    public String getColumnsAfterValue(String key) throws Exception {
        key = key.toUpperCase();
        ArrayList<String> list = this.columns.get(key);
        if(list==null){
            throw new JobException("【"+this.getTabTag()+"】表找不到字段【"+key+"】，可能因为表配置字段与实际数据中的字段不符");
        }
        return this.columns.get(key).get(0);
    }

    /**
     * 取出列字段值(before)
     * @param key
     * @return
     */
    public String getColumnsBeforeValue(String key) throws Exception {
        key = key.toUpperCase();
        ArrayList<String> list = this.columns.get(key);
        if(list==null){
            throw new JobException("【"+this.getTabTag()+"】表找不到字段【"+key+"】，有可能因为表配置字段与实际数据中的字段不符");
        }
        return this.columns.get(key).get(1);
    }

    /**
     * 取出列字段值(包含after和befor)
     * @param key
     * @return
     */
    public ArrayList<String> getColumnsValue(String key){
        return this.columns.get(key.toUpperCase());
    }

    /**
     * 获取表标识
     * @return
     */
    public String getTabTag(){
        return this.meta_data.get("INFA_TABLE_NAME");
    }

    /**
     * 获取操作类型
     * @return
     */
    public String getOpType(){
        return this.meta_data.get("INFA_OP_TYPE");
    }

    /**
     * 获取Infor CDC数据时间戳
     * @return
     */
    public String getInforTimestamp(){
        return this.meta_data.get("DTL__CAPXTIMESTAMP");
    }

    /**
     * 获取所有列值（字段严格有序）
     * @return
     */
    public Object[][] getColumnsValues(TableDesc tableDesc) throws Exception {
        String[] baseFields = tableDesc.getBaseFields();
        String[] dataTypes = tableDesc.getDataTypes();
        Set<String> allFields = getColumnsNames();
        if((baseFields.length+InforDeserialize.etl_fields.length)!=allFields.size()){
            String[] confFields_arr = (String[])ArrayUtils.addAll(baseFields, InforDeserialize.etl_fields);
            Set<String> confFields = new HashSet();
            for(String col:confFields_arr){
                confFields.add(col.toUpperCase());
            }
            //求全差集
            Collection different = CollectionUtils.union(
                    CollectionUtils.subtract(allFields, confFields),
                    CollectionUtils.subtract(confFields, allFields)
            );
            throw new JobException(tableDesc.getTableName()+"表配置字段数与实际消息字段数不符,配置字段数："+(baseFields.length+InforDeserialize.etl_fields.length) +",实际字段数："+allFields.size()+",差异字段："+different);
        }
        //操作类型
        String opType = getOpType();
        //字段值容器（对于update操作类型，将会产生after和before两行数据）
        Object[][] objs = null;
        if(opType.equals("UPDATE_EVENT")){
            objs = new Object[2][allFields.size()];
        }else{
            objs = new Object[1][allFields.size()];
        }
        //填充基础字段值(遍历基础字段取出值，并按照其数据类型进行转换)
        for(int i=0;i<baseFields.length;i++){
            //字段类型
            String dataType = dataTypes[i];
            try {
                switch (getOpType()) {
                    case "INSERT_EVENT":
                        objs[0][i] = cast(dataType,getColumnsAfterValue(baseFields[i]));
                        break;
                    case "DELETE_EVENT":
                        objs[0][i] = cast(dataType,getColumnsAfterValue(baseFields[i]));
                        break;
                    case "UPDATE_EVENT":
                        objs[0][i] = cast(dataType,getColumnsAfterValue(baseFields[i]));
                        objs[1][i] = cast(dataType,getColumnsBeforeValue(baseFields[i]));
                        break;
                }
            }catch (Exception e){
                log.error("昆仑实时数仓作业异常：表"+tableDesc.getTableName()+"字段"+baseFields[i]+"数据类型错误 cast "+getColumnsAfterValue(baseFields[i])+"to "+dataType);
                e.printStackTrace();
            }
        }
        //填充etl字段值
        String[] etl_fields = InforDeserialize.etl_fields;
        for(int i=0;i<etl_fields.length;i++){
            int index = i+baseFields.length;
            switch (getOpType()) {
                case "INSERT_EVENT":
                    objs[0][index] = cast("string",getColumnsAfterValue(etl_fields[i]));
                    break;
                case "DELETE_EVENT":
                    objs[0][index] = cast("string",getColumnsAfterValue(etl_fields[i]));
                    break;
                case "UPDATE_EVENT":
                    //after
                    objs[0][index] = cast("string",getColumnsAfterValue(etl_fields[i]));
                    //before
                    objs[1][index] = cast("string",getColumnsBeforeValue(etl_fields[i]));
                    break;
            }
        }
        return objs;
    }

    /**
     * 从infor cdc数据中获取columns下的所有列名
     * @return
     */
    public Set<String> getColumnsNames(){
        return columns.keySet();
    }

    /**
     * 获取元数据
     * @return
     */
    public HashMap<String,String> getMetaData(){
        return this.meta_data;
    }

    /**
     * 获取数据体
     * @return
     */
    public HashMap<String, ArrayList<String>> getColumns(){
        return this.columns;
    }

    /**
     * 转换为Flink Row对象
     * @param tableDesc
     * @return
     */
    public Row[] toRow(TableDesc tableDesc) throws Exception {
        String opType = this.getOpType();
        Object[][] values = getColumnsValues(tableDesc);
        Row[] rows = null;
        switch (opType){
            case "INSERT_EVENT":
                rows = new Row[1];
                rows[0] = Row.ofKind(RowKind.INSERT,values[0]);
                break;
            case "DELETE_EVENT":
                rows = new Row[1];
                rows[0] = Row.ofKind(RowKind.DELETE,values[0]);
                break;
            case "UPDATE_EVENT":
                rows = new Row[2];
                rows[1] = Row.ofKind(RowKind.UPDATE_AFTER,values[0]);
                rows[0] = Row.ofKind(RowKind.UPDATE_BEFORE,values[1]);
                break;
        }
        return rows;
    }


    /**
     * 将字符串数值转换成指定数据类型
     * @param dataType
     * @param value
     * @return
     */
    public Object cast(String dataType,String value){
        if(value==null){
            return null;
        }
        Object obj = null;
        switch (dataType){
            case "int":
                obj = Integer.valueOf(value);
                break;
            case "string":
                obj = value;
                break;
            case "double":
                obj = Double.valueOf(value);
                break;
            default :System.out.println("无法匹配数据类型");break;
        }
        return obj;
    }

}
