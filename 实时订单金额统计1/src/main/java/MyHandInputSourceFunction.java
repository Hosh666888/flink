import org.apache.flink.streaming.api.functions.source.SourceFunction;


/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/24/2023 4:48 PM
 * @desc:
 **/
public class MyHandInputSourceFunction implements SourceFunction<OrderDTO> {
    public void run(SourceContext<OrderDTO> sourceContext) throws Exception {
        while (true){
            OrderDTO orderDTO = new OrderDTO();
            orderDTO.amount = Math.random()*1000;
            orderDTO.createTime = System.currentTimeMillis();
            sourceContext.collect(orderDTO);
            Thread.sleep(((long) (Math.random() * 1000)));
        }
    }

    public void cancel() {

    }
}
