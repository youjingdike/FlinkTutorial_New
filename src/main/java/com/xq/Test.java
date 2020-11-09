package com.xq;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Test {
    /**
     8      * 时间戳转换成日期格式字符串
     9      * @param seconds 精确到秒的字符串
     10      */
     public static String timeStamp2Date(String seconds,String format) {
              if(seconds == null || seconds.isEmpty() || seconds.equals("null")){
                      return "";
                  }
              if(format == null || format.isEmpty()){
                      format = "yyyy-MM-dd HH:mm:ss";
                   }
             SimpleDateFormat sdf = new SimpleDateFormat(format);
              return sdf.format(new Date(Long.valueOf(seconds+"000")));
          }
     /**
     * 日期格式字符串转换成时间戳
      * @param date_str 字符串日期
       */
         public static String date2TimeStamp(String date_str,String format){
                try {
                      SimpleDateFormat sdf = new SimpleDateFormat(format);
                    System.out.println("kkkkk:"+sdf.parse(date_str).getTime());
                    return String.valueOf(sdf.parse(date_str).getTime()/1000);
                  } catch (Exception e) {
                    e.printStackTrace();
                  }
             return "";
         }

           /**
    * 取得当前时间戳（精确到秒）
      * @return
      */
           public static String timeStamp(){
               long time = System.currentTimeMillis();
              String t = String.valueOf(time/1000);
              return t;
          }

           public static void main(String[] args) {
//              String timeStamp = timeStamp();
              String timeStamp = "1598952485";
              String timeStamp1 = "1598952486";

//              System.out.println("timeStamp="+timeStamp);
//              System.out.println(System.currentTimeMillis());
              //该方法的作用是返回当前的计算机时间，时间的表达格式为当前计算机时间和GMT时间(格林威治时间)1970年1月1号0时0分0秒所差的毫秒数

               String date = timeStamp2Date(timeStamp, "yyyy-MM-dd HH:mm:ss");
              System.out.println("date="+date);
              String date1 = timeStamp2Date(timeStamp1, "yyyy-MM-dd HH:mm:ss");
              System.out.println("date1="+date1);
             String timeStamp2 = date2TimeStamp(date, "yyyy-MM-dd HH:mm:ss");
               System.out.println(timeStamp2);
               System.out.println(date2TimeStamp("2020-11-08 14:32:00", "yyyy-MM-dd HH:mm:ss"));
           }

}
