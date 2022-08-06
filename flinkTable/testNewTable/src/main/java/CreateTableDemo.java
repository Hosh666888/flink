
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;



/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 2022/7/31 11:48
 * @desc:
 **/
public class CreateTableDemo {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> dataStream = env.addSource(new MySource());

        dataStream.print();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table input = tableEnv.fromDataStream(dataStream);


        TableResult tableResult = tableEnv.executeSql("select age from" + input);

        tableResult.print();


        // tableEnv.toAppendStream(output,Event.class).print("output:");

        env.execute();

    }
}
