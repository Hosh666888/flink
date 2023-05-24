import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/24/2023 3:37 PM
 * @desc:
 **/
public class MyProcessFunction extends ProcessFunction<OrderDTO, AggDTO> {

    //间隔分组
    private final long timeGap;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        ValueStateDescriptor<Long> beginTimeDescriptor = new ValueStateDescriptor<Long>("beginTime", TypeInformation.of(Long.class));
        beginTime = getRuntimeContext().getState(beginTimeDescriptor);

        ValueStateDescriptor<Long> endTimeDescriptor = new ValueStateDescriptor<Long>("endTime", TypeInformation.of(Long.class));
        endTime = getRuntimeContext().getState(endTimeDescriptor);

        ValueStateDescriptor<AggDTO> outDescriptor = new ValueStateDescriptor<AggDTO>("out", TypeInformation.of(AggDTO.class));
        out = getRuntimeContext().getState(outDescriptor);

    }

    private transient ValueState<Long> beginTime;

    //当前水位线超过它时collect数据
    private transient ValueState<Long> endTime;

    private transient ValueState<AggDTO> out;


    public MyProcessFunction(long timeGap) {
        this.timeGap = timeGap;
    }


    public void processElement(OrderDTO orderDTO, Context context, Collector<AggDTO> collector) throws Exception {

        //event time
        long now = orderDTO.createTime;
        double amount = orderDTO.amount;
        AggDTO value = out.value();

        if (beginTime.value() == null) {
            //第一条消息来了才能确定需要flush的时间
            beginTime.update(now);
            endTime.update(now + timeGap);
            value.timeGap = String.format("%s ~ %s", new Timestamp(now), new Timestamp(now + timeGap));
            value.orderCount = 1;
            value.amount = amount;
            out.update(value);
        } else if (endTime.value() < now) {
            value.amount += amount;
            value.orderCount++;
            out.update(value);
        } else {
            //时间到了 需要collect
            collector.collect(value);
            beginTime.update(endTime.value());
            endTime.update(beginTime.value() + timeGap);
            value.orderCount = 1;
            value.amount = amount;
            value.timeGap = String.format("%s ~ %s", new Timestamp(beginTime.value()), new Timestamp(endTime.value()));
            out.update(value);
        }

    }
}
