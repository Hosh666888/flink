import java.io.Serializable;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/24/2023 3:33 PM
 * @desc:
 **/
public class AggDTO implements Serializable {

    private static final long serialVersionUID = -3723194775435856670L;

    public String timeGap;

    public double amount;

    public long orderCount;

    public AggDTO() {
    }

    public AggDTO(String timeGap, double amount, long orderCount) {
        this.timeGap = timeGap;
        this.amount = amount;
        this.orderCount = orderCount;
    }

    @Override
    public String toString() {
        return "AggDTO{" +
                "timeGap='" + timeGap + '\'' +
                ", amount=" + amount +
                ", orderCount=" + orderCount +
                '}' + "\n";
    }
}
