package com.atguigu.etl;

import com.alibaba.fastjson.JSON;
import com.atguigu.support.SparkUtils;
import com.atguigu.support.date.DateStyle;
import com.atguigu.support.date.DateUtil;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class RemindEtl {
    public static List<FreeRminder>freeRminderList(SparkSession session){
        ZoneId zoneId = ZoneId.systemDefault();
        LocalDate now= LocalDate.of(2019,Month.NOVEMBER,30);
        Date nowDaySeven = Date.from(now.atStartOfDay(zoneId).toInstant());
        //优惠券八天失效，所以要加一天
        Date tomorrow = DateUtil.addDay(nowDaySeven,1);
        Date pickDay = DateUtil.addDay(nowDaySeven,-7);
        String sql="select date_format(create_time,'yyyy-MM-dd') as day,"+
                "count(member_id) as freeCount from usertags.t_coupon_member where coupon_id= 1 " +
                "and coupon_channel=2 and create_time >='%s'"+
                "group by date_format(create_time,'yyyy-MM-dd')";
        sql = String.format(sql,DateStyle.YYYY_MM_DD_HH_MM_SS);
        Dataset<Row> dataset= session.sql(sql);
        List<String> list = dataset.toJSON().collectAsList();
       // list<FreeRminder> collect = list.stream().map(str-> JSON.parseObject(str,FreeRminder.class)).collect(Collectors.toList());
        List<FreeRminder> collect = list.stream().map(str -> JSON.parseObject(str, FreeRminder.class)).collect(Collectors.toList());
        return collect;


    }
    public static List<CouponReminder> couponReminders(SparkSession session){

        ZoneId zoneId = ZoneId.systemDefault();
        LocalDate now = LocalDate.of(2019, Month.NOVEMBER, 30);
        Date nowDaySeven = Date.from(now.atStartOfDay(zoneId).toInstant());

        // 优惠券 8 天失效的，所以需要加一天
        Date tomorrow = DateUtil.addDay(nowDaySeven, 1);
        Date pickDay = DateUtil.addDay(tomorrow, -8);

        String sql ="select date_format(create_time,'yyyy-MM-dd') as day,count(member_id) as couponCount " +
                " from usertags.t_coupon_member where coupon_id != 1 " +
                " and create_time >= '%s' " +
                " group by date_format(create_time,'yyyy-MM-dd')";
        sql = String.format(sql,DateUtil.DateToString(pickDay, DateStyle.YYYY_MM_DD_HH_MM_SS));
        Dataset<Row> dataset = session.sql(sql);
        List<String> list = dataset.toJSON().collectAsList();
        List<CouponReminder> collect = list.stream().map(str -> JSON.parseObject(str, CouponReminder.class)).collect(Collectors.toList());
        return collect;

    }

    public static void main(String[] args) {
    SparkSession session = SparkUtils.initSession();
    List<FreeRminder> freeRminders = freeRminderList(session);
    List<CouponReminder> couponReminders = couponReminders(session);
    System.out.println("+++++++++"+freeRminders);
    System.out.println("++++++++++++++++"+freeRminders);
    }





    @Data
    static class  FreeRminder{
        private String day;
        private Integer freeCount;
    }



    @Data
    static class CouponReminder{
        private String day;
        private Integer couponCount;
    }
}
