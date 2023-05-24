import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;


/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/24/2023 3:03 PM
 * @desc:
 **/
public class Solution {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<OrderDTO> operator = env.addSource(new MyHandInputSourceFunction(), TypeInformation.of(OrderDTO.class))
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<OrderDTO>(new MyWatermarkAssigner()));


        DataStream<AggDTO> result = operator.name("实时订单金额统计1")
                // .process(new MyProcessFunction(2000));

                .process(new MyProcessFunction(2000));


        result.print();

        env.execute();
    }
}
