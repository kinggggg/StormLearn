package com.zeek.mystorm.trident.test;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @ClassName MyFunction
 * @Description
 * @Author liweibo
 * @Date 2018/11/23 下午4:22
 * @Version v1.0
 **/
public class PrintFunction extends BaseFunction {

    public PrintFunction() {
        System.out.println(this + " PrintFunction");
    }

    @Override
    public void prepare(Map conf, TridentOperationContext context) {
        super.prepare(conf, context);
        System.out.println(this + "PrintFunction.prepare()");
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
        Fields fields = tuple.getFields();
        StringBuilder builder = new StringBuilder();
        for(String f : fields) {
            builder.append(f + ":" + tuple.getValueByField(f) + ", ");
        }
        System.out.println(this + " PrintFunction.execute() : " + builder.toString());
    }
}
