import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.script.End;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/24/2023 3:03 PM
 * @desc:
 **/
public class Solution {

    private static Logger log = LoggerFactory.getLogger(Solution.class);

    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setBoolean("rest.flamegraph.enabled", true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("kafkaConfig.properties");
        Properties props = new Properties();
        props.load(is);



        FlinkKafkaConsumer<OrderDTO> kafkaConsumer = new FlinkKafkaConsumer<>("orders_v2", new KafkaDeserializationSchema<OrderDTO>() {
            @Override
            public boolean isEndOfStream(OrderDTO orderDTO) {
                return false;
            }

            @Override
            public OrderDTO deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
                log.info("接收到了:" + consumerRecord.value().length);

                try (final ByteArrayInputStream inputStream = new ByteArrayInputStream(consumerRecord.value());
                     final ObjectInputStream ois = new ObjectInputStream(inputStream)
                ) {
                    return (OrderDTO) ois.readObject();
                } catch (IOException | ClassNotFoundException e) {
                    log.error(e.getMessage(), e);
                }
                return null;
            }

            @Override
            public TypeInformation<OrderDTO> getProducedType() {
                return TypeInformation.of(OrderDTO.class);
            }
        }, props);



        SingleOutputStreamOperator<OrderDTO> operator =
        //         env.addSource(kafkaConsumer, TypeInformation.of(OrderDTO.class))
                        env.addSource(new MyHandInputSourceFunction(), TypeInformation.of(OrderDTO.class))
                        .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<OrderDTO>(new MyWatermarkAssigner()));

        operator.print("source");


        SingleOutputStreamOperator<Object> result = operator.name("实时订单金额统计")
                .windowAll(TumblingEventTimeWindows.of(Time.of(5, TimeUnit.SECONDS)))
                .aggregate(new AggregateFunction<OrderDTO, List<Double>, AggDTO>() {
                    @Override
                    public List<Double> createAccumulator() {
                        return new ArrayList<>(100);
                    }

                    @Override
                    public List<Double> add(OrderDTO orderDTO, List<Double> doubles) {
                        doubles.add(orderDTO.amount);
                        return doubles;
                    }

                    @Override
                    public AggDTO getResult(List<Double> doubles) {
                        return new AggDTO("", doubles.stream().reduce(Double::sum).get(), doubles.size());
                    }

                    @Override
                    public List<Double> merge(List<Double> doubles, List<Double> acc1) {
                        doubles.addAll(acc1);
                        return doubles;
                    }
                }, new ProcessAllWindowFunction<AggDTO, Object, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<AggDTO> iterable, Collector<Object> collector) throws Exception {
                        AggDTO aggDTO = new AggDTO();
                        aggDTO.timeGap = new Timestamp(context.window().getStart()) + " ~ " + new Timestamp(context.window().getEnd());

                        for (AggDTO item : iterable) {
                            aggDTO.orderCount += item.orderCount;
                            aggDTO.amount += item.amount;
                        }

                        collector.collect(aggDTO);
                    }
                });

        result.print("output");

        env.execute();
    }
}
