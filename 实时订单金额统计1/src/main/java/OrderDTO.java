import java.io.Serializable;

/**
 * @author: Double>J
 * @email: zjj20001031@foxmail.com
 * @editTime: 5/24/2023 3:00 PM
 * @desc:
 **/
public class OrderDTO implements Serializable {
    private static final long serialVersionUID = 759791698473786334L;

    public String orderNum;

    public double amount;

    public long createTime;

    public String key;

    public OrderDTO() {
    }

    public OrderDTO(String orderNum, double amount, long createTime) {
        this.orderNum = orderNum;
        this.amount = amount;
        this.createTime = createTime;
    }

    public OrderDTO(String orderNum, double amount, long createTime, String key) {
        this.orderNum = orderNum;
        this.amount = amount;
        this.createTime = createTime;
        this.key = key;
    }

    @Override
    public String toString() {
        return "OrderDTO{" +
                "orderNum='" + orderNum + '\'' +
                ", amount=" + amount +
                ", createTime=" + createTime +
                ", key='" + key + '\'' +
                '}';
    }
}
