import java.util.Date;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 2022/8/6 18:57
 * @desc:
 **/
public class Event {
    public String name;
    public Integer age;
    public Date date;

    @Override
    public String toString() {
        return "Event{" +
                "name='" + name + '\'' +
                ", age=" + age +
                ", date=" + date +
                '}';
    }
}
