import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.api.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.types.DataType;
import scala.Tuple2;


/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 2022/7/31 11:48
 * @desc:
 **/
public class CreateTableDemo {

   static class Input{
        public String name;
        public long age;

       public Input(String name, long age) {
           this.name = name;
           this.age = age;
       }
   }

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        // DataStream<Input> source = env.readTextFile("log.txt")
        //         .map(line -> {
        //             String[] split = line.split(",");
        //             return new Input(split[0], Long.parseLong(split[1]));
        //         });

        MapOperator<String, Input> map = env.readTextFile("log.txt").map(line -> {
            String[] split = line.split(",");
            return new Input(split[0], Long.parseLong(split[1]));
        });


        Table table = tableEnv.fromDataSet(map);


        DataSet<Input> inputDataSet = tableEnv.toDataSet(table, Input.class);

        inputDataSet.print();





    }
}
