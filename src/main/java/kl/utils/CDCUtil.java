package kl.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.*;

/**
 *
 * @Auther: lixz
 * @Date: 2021/12/09/14:30
 * @Description: CDC json数据解析工具
 */
public class CDCUtil {
    public static void main(String[] args) throws Exception {

        String str = "{\"meta_data\":[{\"name\":{\"string\":\"INFA_SEQUENCE\"},\"value\":{\"string\":\"2,PWX_GENERIC,1,,2,3,D4083C360D1FE8000000000000083C360D1FDE00000034000291750009C8C60078000100000000000100000000,00,000002012C4CBB29\"},\"type\":null},{\"name\":{\"string\":\"INFA_TABLE_NAME\"},\"value\":{\"string\":\"d8oracapt.lktranss_LKTRANSSTATUS\"},\"type\":null},{\"name\":{\"string\":\"INFA_OP_TYPE\"},\"value\":{\"string\":\"UPDATE_EVENT\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXRESTART1\"},\"value\":{\"string\":\"1Ag8Ng0f6AAAAAAAAAg8Ng0f3gAAADQAApF1AAnIxgB4AAEAAAAAAAEAAAAA\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXRESTART2\"},\"value\":{\"string\":\"AAACASxMuyk=\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXUOW\"},\"value\":{\"string\":\"MFgwMDAxLjAxQS4wMDAxNUQwRg==\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXUSER\"},\"value\":{\"string\":\"GGS\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXTIMESTAMP\"},\"value\":{\"string\":\"202204061455250000000000\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXACTION\"},\"value\":{\"string\":\"U\"},\"type\":null},{\"name\":{\"string\":\"DTL__CAPXROWID\"},\"value\":null,\"type\":null}],\"columns\":{\"array\":[{\"name\":{\"string\":\"TRANSCODE\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"REPORTNO\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"BANKCODE\"},\"value\":{\"string\":\"WX\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"WX\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"BANKBRANCH\"},\"value\":{\"string\":\"WX\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"WX\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"BANKNODE\"},\"value\":{\"string\":\"WX\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"WX\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"BANKOPERATOR\"},\"value\":{\"string\":\"WX\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"WX\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"TRANSNO\"},\"value\":{\"string\":\"110003063020220406443454\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"110003063020220406443454\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"FUNCFLAG\"},\"value\":{\"string\":\"accdetails\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"accdetails\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"TRANSDATE\"},\"value\":{\"string\":\"202204060000000000000000\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"202204060000000000000000\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"TRANSTIME\"},\"value\":{\"string\":\"14:55:20\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"14:55:20\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"MANAGECOM\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"RISKCODE\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"PROPOSALNO\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"PRTNO\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"POLNO\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"EDORNO\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"TEMPFEENO\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"TRANSAMNT\"},\"value\":{\"string\":\"0.00\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"0.00\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"BANKACC\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"RCODE\"},\"value\":{\"string\":\"1\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"TRANSSTATUS\"},\"value\":{\"string\":\"1\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"0\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"STATUS\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"DESCR\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"TEMP\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"MAKEDATE\"},\"value\":{\"string\":\"202204060000000000000000\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"202204060000000000000000\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"MAKETIME\"},\"value\":{\"string\":\"14:55:20\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"14:55:20\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"MODIFYDATE\"},\"value\":{\"string\":\"202204060000000000000000\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"202204060000000000000000\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"MODIFYTIME\"},\"value\":{\"string\":\"14:55:20\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"14:55:20\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"STATE_CODE\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"REQUESTID\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"OUTSERVICECODE\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"CLIENTIP\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"CLIENTPORT\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"ISSUEWAY\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"SERVICESTARTTIME\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"SERVICEENDTIME\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"RBANKVSMP\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"DESBANKVSMP\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"RMPVSKERNEL\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"DESMPVSKERNEL\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"RESULTBALANCE\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"DESBALANCE\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"BAK1\"},\"value\":{\"string\":\"2020021563446559\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"2020021563446559\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"BAK2\"},\"value\":{\"string\":\"2022\"},\"isPresent\":{\"boolean\":true},\"beforeImage\":{\"string\":\"2022\"},\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"BAK3\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}},{\"name\":{\"string\":\"BAK4\"},\"value\":null,\"isPresent\":{\"boolean\":true},\"beforeImage\":null,\"isPresentBeforeImage\":{\"boolean\":true}}]}}";
        //打印
        HashMap<String, Object> data = ParseCDCJson(str);
        HashMap<String, ArrayList<String>> columns = (HashMap<String, ArrayList<String>>) data.get("columns");

        System.out.println(columns.get("BAK4").get(0));
    }

    /**
     *Informatic CDC JSON串转换为Map+ArrayList结构
     * 注：data中的每个字段值为数组类型，该数组的第一个元素为新值，第二个为旧值
     * 转换后结构示例：
     * {
     *     meta_data:{INFA_TABLE_NAME:lccont_LCCONT,INFA_OP_TYPE:UPDATE_EVENT},
     *     columns:{GRPCONTNO:[000,111],CONTNO:[P2021,P2021],PRTNO:[新值,旧值]}
     * }
     * @param jsonStr Informatic CDC JSON串
     * @return
     * @exception Exception 解析异常直接抛出，如有异常发生，说明该条数据不可用
     */
    public static HashMap<String,Object> ParseCDCJson(String jsonStr) throws Exception{
        //结果容器
        HashMap<String,Object> res = new HashMap<>();
        HashMap<String,String> meta_res = new HashMap<>();
        HashMap<String,ArrayList<String>> data_res = new HashMap<>();
        //转换成JSON对象
        JSONObject json = JSON.parseObject(jsonStr);
        /**
         * 解析元数据meta_data
         */
        JSONArray meta_data = (JSONArray)json.get("meta_data");
        Iterator meta_data_it = meta_data.iterator();
        while(meta_data_it.hasNext()){
            JSONObject meta_data_one = (JSONObject)meta_data_it.next();
            JSONObject name = (JSONObject)meta_data_one.get("name");
            JSONObject value = (JSONObject)meta_data_one.get("value");
            String nv = null;
            String vv = null;
            if(name!=null){
                nv = name.getString("string");
            }
            if(value!=null){
                vv = value.getString("string");
            }
            meta_res.put(nv,vv);
        }
        /**
         * 解析数据columns
         */
        JSONObject columns = (JSONObject)json.get("columns");
        JSONArray array = columns.getJSONArray("array");
        Iterator<Object> array_it = array.iterator();
        while(array_it.hasNext()){
            JSONObject array_one = (JSONObject)array_it.next();
            JSONObject name = (JSONObject)array_one.get("name");
            JSONObject value = (JSONObject)array_one.get("value");
            JSONObject beforeImage = (JSONObject)array_one.get("beforeImage");
            ArrayList<String> v = new ArrayList<>();
            if(value!=null){
                v.add(value.getString("string"));
            }else{
                v.add(null);
            }
            if(beforeImage!=null){
                v.add(beforeImage.getString("string"));
            }else{
                v.add(null);
            }
            data_res.put(name.getString("string"),v);
        }
        //数据放入容器
        res.put("meta_data",meta_res);
        res.put("columns",data_res);
        return res;
    }

    /**
     * 获取CDC json中的表名，即INFA_TABLE_NAME值
     * @param jsonStr
     * @return
     * @throws Exception
     */
    public static String getTableName(String jsonStr) throws Exception {
        HashMap<String,String> meta = (HashMap<String,String>)ParseCDCJson(jsonStr).get("meta_data");
        return meta.get("INFA_TABLE_NAME");
    }

    /**
     * 获取CDC json中的所有数据列列名
     * @param jsonStr
     * @return
     */
    public static Set<String> getColumonNames(String jsonStr) throws Exception {
        HashMap<String, ArrayList<String>> data = (HashMap<String, ArrayList<String>>)ParseCDCJson(jsonStr).get("columns");
        return data.keySet();
    }


    /**
     * 为json中的meta_data字段添加新数据
     * 即构造meta_data下的元素：
     *    {
     *       "name": {
     *         "string": "INFA_SEQUENCE"
     *       },
     *       "value": {
     *         "string": "2,PWX_GENERIC,1,,2,3,D4083C301E1F64000000000000083C301E1F630000000000028E6D000FCAD00010000200000000000100000000,00,00000200FC09ACA1"
     *       },
     *       "type": null
     *     }
     * @param key
     * @param v
     */
    public static void metaDataAdd(JSONArray root,String key,String v){
        JSONObject node = new JSONObject();
        //添加key
        JSONObject name = new JSONObject();
        name.put("string",key);
        node.put("name",name);
        //添加值
        JSONObject value = new JSONObject();
        value.put("string",v);
        node.put("value",value);
        //添加类型
        node.put("type",null);
        //添加到meta节点
        root.add(node);
    }

    /**
     * 在cdc json串中的columns层添加新的字段和值
     * @param root CDC消息json对象
     * @param key 要添加的字段
     * @param v 要添加的字段值
     */
    public static void columnsDataAdd(JSONObject root,String key,String v){
        //获取columns节点
        JSONObject columns = root.getJSONObject("columns");
        //获取columns.array节点
        JSONArray array = columns.getJSONArray("array");
        JSONObject node = new JSONObject();
        //组装name节点
        JSONObject name = new JSONObject();
        name.put("string",key);
        node.put("name",name);
        //组装value节点
        JSONObject value = new JSONObject();
        value.put("string",v);
        node.put("value",value);
        //组装isPresent节点(不区分befor_after,均视为相同值)
        JSONObject isPresent = new JSONObject();
        isPresent.put("boolean",true);
        node.put("isPresent",isPresent);
        //组装beforeImage节点
        JSONObject beforeImage = new JSONObject();
        beforeImage.put("string",v);
        node.put("beforeImage",beforeImage);
        //组装isPresentBeforeImage节点
        JSONObject isPresentBeforeImage = new JSONObject();
        isPresentBeforeImage.put("boolean",true);
        node.put("isPresentBeforeImage",isPresentBeforeImage);
        //将组装好的节点放入array节点
        array.add(node);
    }
}
