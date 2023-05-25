import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.UUID;


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
            orderDTO.orderNum = UUID.randomUUID().toString();
            orderDTO.amount = Math.random()*2000;
            orderDTO.createTime = System.currentTimeMillis();
            orderDTO.key = "123";
            sourceContext.collect(orderDTO);
            Thread.sleep(((long) (Math.random() * 800)));
        }
    }

    public void cancel() {

    }
}
