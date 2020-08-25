package com.atguigu.etl;

import com.alibaba.fastjson.JSON;
import com.atguigu.support.SparkUtils;
import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.stream.Collectors;

public class MemberEtl {

    /**
     * member sex etl
     *
     * @param session
     * @return
     */
    public static List<MemberSex> memberSex(SparkSession session) {
        Dataset<Row> dataset = session.sql("select sex as memberSex, count(id) as sexCount " +
                " from usertags.t_member group by sex");
        List<String> list = dataset.toJSON().collectAsList();
        List<MemberSex> collect = list.stream()
                .map(str -> JSON.parseObject(str, MemberSex.class))
                .collect(Collectors.toList());
        return collect;
    }

    /**
     * member reg channel
     *
     * @param session
     * @return
     */
    public static List<MemberChannel> memberRegChannel(SparkSession session) {
        Dataset<Row> dataset = session.sql("select member_channel as memberChannel, count(id) as channelCount " +
                " from usertags.t_member group by member_channel");
        List<String> list = dataset.toJSON().collectAsList();
        List<MemberChannel> collect = list.stream()
                .map(str -> JSON.parseObject(str, MemberChannel.class))
                .collect(Collectors.toList());

        return collect;

    }

    /**
     * mp sub etl
     *
     * @param session
     * @return
     */
    public static List<MemberMpSub> memberMpSub(SparkSession session) {
        Dataset<Row> sub = session.sql("select count(if(mp_open_id !='null',id,null)) as subCount, " +
                " count(if(mp_open_id ='null',id,null)) as unSubCount " +
                " from usertags.t_member");
        List<String> list = sub.toJSON().collectAsList();
        List<MemberMpSub> collect = list.stream()
                .map(str -> JSON.parseObject(str, MemberMpSub.class))
                .collect(Collectors.toList());

        return collect;

    }

    /**
     * member heat
     *
     * @param session
     * @return
     */
    public static MemberHeat memberHeat(SparkSession session) {
        // reg , complete , order , orderAgain, coupon
        // reg , complete ==> i_member.t_member  phone = 'null'
        // order,orderAgain ==> i_order.t_order
        // coupon ==> i_marketing.t_coupon_member

        Dataset<Row> reg_complete = session.sql("select count(if(phone='null',id,null)) as reg," +
                " count(if(phone !='null',id,null)) as complete " +
                " from usertags.t_member");

        // order,orderAgain
        Dataset<Row> order_again = session.sql("select count(if(t.orderCount =1,t.member_id,null)) as order," +
                "count(if(t.orderCount >=2,t.member_id,null)) as orderAgain from " +
                "(select count(order_id) as orderCount,member_id from usertags.t_order group by member_id) as t");

        // coupon
        Dataset<Row> coupon = session.sql("select count(distinct member_id) as coupon from usertags.t_coupon_member ");

        Dataset<Row> result = coupon.crossJoin(reg_complete).crossJoin(order_again);

        List<String> list = result.toJSON().collectAsList();
        List<MemberHeat> collect = list.stream().map(str -> JSON.parseObject(str, MemberHeat.class)).collect(Collectors.toList());
        return collect.get(0);

    }

    public static void main(String[] args) {
        SparkSession session = SparkUtils.initSession();
        List<MemberSex> memberSexes = memberSex(session);
        List<MemberChannel> memberChannels = memberRegChannel(session);
        List<MemberMpSub> memberMpSubs = memberMpSub(session);
        MemberHeat memberHeat = memberHeat(session);

        MemberVo vo = new MemberVo();
        vo.setMemberChannels(memberChannels);
        vo.setMemberSexes(memberSexes);
        vo.setMemberMpSubs(memberMpSubs);
        vo.setMemberHeat(memberHeat);
        System.out.println("=====" + JSON.toJSONString(vo));
    }

    @Data
    static class MemberSex {
        private Integer memberSex;
        private Integer sexCount;
    }

    @Data
    static class MemberChannel {
        private Integer memberChannel;
        private Integer channelCount;
    }

    @Data
    static class MemberMpSub {
        private Integer subCount;
        private Integer unSubCount;
    }

    @Data
    static class MemberVo {
        private List<MemberSex> memberSexes;
        private List<MemberChannel> memberChannels;
        private List<MemberMpSub> memberMpSubs;
        private MemberHeat memberHeat;
    }

    @Data
    static class MemberHeat {
        private Integer reg;
        private Integer complete;
        private Integer order;
        private Integer orderAgain;
        private Integer coupon;
    }
}

