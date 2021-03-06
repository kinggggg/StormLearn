package com.zeek.mystorm.trident.test;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;

import java.util.Map;

/**
 * @ClassName Filter1
 * @Description
 * @Author liweibo
 * @Date 2018/11/23 下午2:31
 * @Version v1.0
 **/
public class Filter1 extends BaseFilter {

    public Filter1() {
        System.out.println("new Filter1()");
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        System.out.println("Filter1.prepare()");
    }


    public boolean isKeep(TridentTuple tuple) {
        int s = tuple.getInteger(0);
        System.out.println(this + "filter1 : " + s);
        return true;
    }
}
