package com.atguigu.etl.es;

import com.atguigu.support.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.io.Serializable;
import java.util.List;

public class EsMappingEtl {
    public static void etl(SparkSession session){
        Dataset<Row> member = session.sql("select id as memberid,phone,sex,member_channel,mp_open_id as subOpenId," +
                " address_default_id as address, date_format(create_time,'yyyy-MM-dd') as regTime" +
                " from usertags.t_member");
        // order_commodity collect_list -- group_concat [1,2,3,4,5]
        Dataset<Row> order_commodity = session.sql("select o.member_id as memberId," +
                " date_format(max(o.create_time),'yyyy-MM-dd') as orderTime," +
                " count(o.order_id) as orderCount," +
                " collect_list(DISTINCT oc.commodity_id) as favGoods, " +
                " sum(o.pay_price) as orderMoney " +
                " from usertags.t_order as o left join usertags.t_order_commodity as oc" +
                " on o.order_id = oc.order_id group by o.member_id");
        Dataset<Row> freeCoupon = session.sql("select member_id as memberId," +
                " date_format(create_time,'yyyy-MM-dd') as freeCouponTime " +
                " from usertags.t_coupon_member where coupon_id=1");
        Dataset<Row> couponTimes = session.sql("select member_id as memberId," +
                " collect_list(date_format(create_time,'yyyy-MM-dd')) as couponTimes" +
                " from usertags.t_coupon_member where coupon_id !=1 group by member_id");
        Dataset<Row> chargeMoney = session.sql("select cm.member_id as memberId,sum(c.coupon_price/2)as chargeMoney " +
                " from usertags.t_coupon_member as cm left join usertags.t_coupon as c " +
                " on cm.coupon_id = c.id where cm.coupon_channel = 1 group by cm.member_id");
        Dataset<Row> overTime = session.sql("select (to_unix_timestamp(max(arrive_time)) - to_unix_timestamp(max(pick_time))) " +
                " as overTime, member_id as memberId " +
                " from usertags.t_delivery group by member_id");

        Dataset<Row> feedback = session.sql("select fb.feedback_type as feedback,fb.member_id as memberId" +
                " from usertags.t_feedback as fb " +
                " left join (select max(id) as mid,member_id as memberId " +
                " from usertags.t_feedback group by member_id) as t " +
                " on fb.id = t.mid");
        member.registerTempTable("member");
        order_commodity.registerTempTable("oc");
        freeCoupon.registerTempTable("freeCoupon");
        couponTimes.registerTempTable("couponTimes");
        chargeMoney.registerTempTable("chargeMoney");
        overTime.registerTempTable("overTime");
        feedback.registerTempTable("feedback");

        Dataset<Row> result = session.sql("select m.*,o.orderCount,o.orderTime,o.orderMoney,o.favGoods," +
                " fb.freeCouponTime,ct.couponTimes, cm.chargeMoney,ot.overTime,f.feedBack" +
                " from member as m " +
                " left join oc as o on m.memberId = o.memberId " +
                " left join freeCoupon as fb on m.memberId = fb.memberId " +
                " left join couponTimes as ct on m.memberId = ct.memberId " +
                " left join chargeMoney as cm on m.memberId = cm.memberId " +
                " left join overTime as ot on m.memberId = ot.memberId " +
                " left join feedback as f on m.memberId = f.memberId ");

        JavaEsSparkSQL.saveToEs(result,"/tag/_doc");





    }

    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();
        etl(session);
    }












    @Data
    public static class  MemberTag implements Serializable{
        private String memberId;
        private String phone;
        private String sex;
        private String channel;
        private String subOponId;
        private String address;
        private String regTime;
        //i_order
        private Long orderCount;
        //max(create_time)i_order.t_order
        private String orderTime;
        private Double orderMoney;
        private List<String> couponTime;

        private Integer overTime;
        private Integer feedBack;

    }
}
