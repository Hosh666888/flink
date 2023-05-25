import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.runtime.operators.util.AssignerWithPeriodicWatermarksAdapter;
import scala.collection.script.End;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Properties;


/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/24/2023 3:03 PM
 * @desc:
 **/
public class Solution {
    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        configuration.setBoolean("rest.flamegraph.enabled",true);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("kafkaConfig.properties");
        Properties props = new Properties();
        props.load(is);



        SingleOutputStreamOperator<OrderDTO> operator =
                // env.addSource(new MyKafkaSource(Collections.singleton("orders")), TypeInformation.of(OrderDTO.class))
                env.addSource(new MyHandInputSourceFunction(), TypeInformation.of(OrderDTO.class))
                // env.addSource(new FlinkKafkaConsumer<OrderDTO>("orders",new TypeInformationSerializationSchema<OrderDTO>(TypeInformation.of(OrderDTO.class),new ExecutionConfig()),props), TypeInformation.of(OrderDTO.class))
                    .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarksAdapter.Strategy<OrderDTO>(new MyWatermarkAssigner()));

        operator.print("source");


        DataStream<AggDTO> result = operator.name("实时订单金额统计")
                .keyBy(o -> o.key)
                // .process(new MyProcessFunction(2000));
                .process(new MyFixedProcessFunction(1000 * 5))
                .setParallelism(1);


        result.print("output");

        env.execute();
    }
}
