package com.zeek.mystorm.trident.test;

import org.apache.storm.shade.org.apache.commons.lang.StringUtils;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @ClassName MyFunction
 * @Description
 * @Author liweibo
 * @Date 2018/11/23 下午4:22
 * @Version v1.0
 **/
public class MyFunction extends BaseFunction {

    public MyFunction() {
        System.out.println("MyFunction");
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        System.out.println(this + "MyFunction.prepare()");
    }

    // tuple中包含的字段为a, b；发送出去的tuple字段的名称为a, b, a
    public void execute(TridentTuple tuple, TridentCollector collector) {
        Integer a = tuple.getInteger(0);
        Integer b = tuple.getInteger(1);
        collector.emit(new Values(tuple.get(0)));
        //System.out.println("==========>" + this + StringUtils.join(new Values(tuple.get(0)), ","));
        System.out.println(this + " MyFunction.execute() : " + "a = " + a + ", b = " + b);
    }
}
