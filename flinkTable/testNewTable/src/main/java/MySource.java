import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;
import java.util.Random;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 2022/8/6 18:57
 * @desc:
 **/
public class MySource implements SourceFunction<Event> {

    private static String[] users = {"Alice","Jack","Tom","Tim"};

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        Random random = new Random();
        while (true){
            int nextInt = random.nextInt(users.length)%users.length;
            Event event = new Event();
            event.age = random.nextInt(50);
            event.name = users[nextInt];
            event.date = new Date();
            sourceContext.collect(event);
            Thread.sleep(1500L);
        }
    }

    @Override
    public void cancel() {
        System.out.println("canceled...");
    }
}
