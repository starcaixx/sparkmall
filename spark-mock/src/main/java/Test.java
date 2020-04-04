import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;

/**
 * @author star
 * @create 2019-03-20 10:27
 */
public class Test {
    public static void main(String[] args) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        System.out.println(Instant.now());
        Date date = new Date(1553686736088l);
        System.out.println(simpleDateFormat.format(date));
        Instant instant = Instant.ofEpochMilli(1553686736088l);
//        System.out.println(instant.atOffset(ZoneOffset));
    }
}
