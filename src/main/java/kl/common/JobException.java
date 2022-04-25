package kl.common;

/**
 * Created with IntelliJ IDEA.
 * Copyright@ Apache Open Source Organization
 *
 * @Auther: lixz
 * @Date: 2022/04/08/15:11
 * @Description: 作业异常
 * 通用异常类，如需抛出异常应当使用该类，检索日志时可快速定位
 */
public class JobException extends Exception{
    public JobException(String msg){
        super("昆仑实时数仓作业异常："+msg);
    }
}
