import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/24/2023 3:10 PM
 * @desc:
 **/
public class MyWatermarkAssigner implements AssignerWithPeriodicWatermarks<OrderDTO> {

    long currentTimestamp;

    @Nullable
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimestamp);
    }

    public long extractTimestamp(OrderDTO orderDTO, long l) {
        currentTimestamp = orderDTO.createTime;
        return currentTimestamp;
    }
}
