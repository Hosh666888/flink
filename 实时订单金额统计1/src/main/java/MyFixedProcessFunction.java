import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.scala.function.RichProcessAllWindowFunction;
import org.apache.flink.util.Collector;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.BinaryOperator;

/**
 *  如果事件时间乱序到达 会出现混乱
 */

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/25/2023 8:24 AM
 * @desc:
 **/
@Deprecated
public class MyFixedProcessFunction extends ProcessFunction<OrderDTO, AggDTO> {

    private final long timeGap;

    private long begin;

    private long end;

    private List<Double> amounts;

    public MyFixedProcessFunction(long timeGap) {
        this.timeGap = timeGap;
        amounts = new ArrayList<Double>(500);
    }


    public void processElement(OrderDTO orderDTO, Context context, Collector<AggDTO> collector) throws Exception {


        long l = context.timerService().currentWatermark();
        if (l <= 0) {
            begin = orderDTO.createTime;
            end = begin + timeGap;
            context.timerService().registerEventTimeTimer(end);
        }
        amounts.add(orderDTO.amount);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<AggDTO> out) throws Exception {
        super.onTimer(timestamp, ctx, out);

        AggDTO aggDTO = new AggDTO(
                String.format("%s ~ %s", new Timestamp(begin), new Timestamp(end))
                , amounts.stream().reduce(0.0, Double::sum)
                , amounts.size()
        );
        out.collect(aggDTO);

        CompletableFuture.runAsync(() -> {
            File file = new File(String.format("%s_%s.agg", "实时计算订单金额", LocalDate.now()));

            try(RandomAccessFile rw = new RandomAccessFile(file, "rw")){
                rw.seek(rw.length());
                rw.write(aggDTO.toString().getBytes(StandardCharsets.UTF_8));
            }catch (Exception e){
                e.printStackTrace();
            }
        });
        amounts.clear();
        begin = ctx.timerService().currentWatermark();
        end = begin + timeGap;
        ctx.timerService().registerEventTimeTimer(end);

    }
}
