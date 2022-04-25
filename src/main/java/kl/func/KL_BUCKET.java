package kl.func;

import org.apache.flink.table.functions.ScalarFunction;

/**
 *
 * 哈希值取模
 * @Auther: lixz
 * @Date: 2022/03/11/13:49
 * @Description:
 */
public class KL_BUCKET extends ScalarFunction {
    public Integer eval(String s, Integer i) {
        return Math.abs(s.hashCode()%i);
    }
}
