package com.bin.delay.file.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;


public class DelayUtil {
    private static final Logger log = LoggerFactory.getLogger(DelayUtil.class);
    private static long delayEndZeroTime = 0;

    public static long getCruSecond(){
        long time = System.currentTimeMillis();
        return time / 1000 ;
    }

    public static long getSecond(long time){
        return time / 1000;
    }


    public static void main(String[] args) {
        int timeSpan = 30;
        long timeSecond = System.currentTimeMillis();
        System.out.println("currentTimeMillis=" + timeSecond);
        long delayMinute = getTimeToMinuteTime(timeSecond);

        System.out.println("getTimeToMinuteTime=" + delayMinute);

        Long aLong = parseMinuteTime(delayMinute);
        System.out.println("parseMinuteTime=" + aLong);


        long delayMinuteTime = getDelayMinuteTime(timeSecond, timeSpan);
        System.out.println("getDelayMinuteTime=" + delayMinuteTime);

        log.warn("测试");

        System.out.println("getCruDelaySpan=" + getCruDelaySpan(getSecond(timeSecond), timeSpan));

        System.out.println("nextSecond=" + computeNextSecondTimeMillis());

        System.out.println(isTimetoDelete());

        System.out.println("getNextLoadTime=" + getNextLoadTime(timeSpan));

        System.out.println("getNextLoadTime=" +getNextLoadTime(getSecond(timeSecond), timeSpan));

        long nexTime = getNexTime(timeSpan);
        System.out.print(nexTime +  "-");
        System.out.print(getTimeToMinuteTime(nexTime));
        System.out.println();
        System.out.println("getPrevLoadTime=" + getPrevLoadTime(getSecond(timeSecond), timeSpan));
    }

    static DateTimeFormatter df = DateTimeFormatter.ofPattern("yyyyMMddHHmm");

    // 时间戳 -> 202404151137
    public static long getTimeToMinuteTime(long time){
        return Long.valueOf(df.format(milliToldt(time)));
    }

    // 202404151137 ->  时间戳 截断到秒 1713167700000
    public static Long parseMinuteTime(long delayMinute){
        LocalDateTime parse = LocalDateTime.parse(String.valueOf(delayMinute), df);

        return ldtToMill(LocalDateTime.from(parse));

    }

    /**
     * 获取当前时间窗口的起始 202404151530
     */
    public static long getDelayMinuteTime(long time,int timeSpan){

        LocalDateTime localDateTime =  milliToldt(time);
        int minute =  localDateTime.getMinute();
        int delayMinute = ( minute / timeSpan ) * 30;
        String str = df.format(localDateTime.withMinute(delayMinute));
        return Long.valueOf(str);
    }


    /**
     * 获取当前延时时间片的区间
     * timeSecond 秒的时间  timeSpan 分钟
     *  TimeSpan [startTimeMs=1713160800000, endTimeMs=1713162600000, minuteTime=202404151400]
     */
    public static TimeWindow getCruDelaySpan(long timeSecond, int timeSpanMinute) {

        LocalDateTime localDateTime = milliToldt(timeSecond * 1000)
                                             .withSecond(0).withNano(0);

        int minute = localDateTime.getMinute();
        int delayMinute = ( minute / timeSpanMinute ) * timeSpanMinute;

        long startTimeMs = ldtToMill(localDateTime.withMinute(delayMinute));

        String startTime = df.format(milliToldt(startTimeMs));


        int spanEndMinute = ( minute / timeSpanMinute + 1) * timeSpanMinute;

        LocalDateTime localDateTime1 = null;
        if (spanEndMinute == 60) {
            localDateTime1 = localDateTime.plusHours(1).withMinute(0);
        }else {
            localDateTime1 = localDateTime.withMinute(spanEndMinute);
        }

        long endTimeMs =  ldtToMill(localDateTime1);
        TimeWindow timeWindow = new TimeWindow();
        timeWindow.setStartTimeMs(startTimeMs);
        timeWindow.setEndTimeMs(endTimeMs);
        timeWindow.setMinuteTime(Long.valueOf(startTime));
        return timeWindow;
    }


    // 当前时间下一秒
    public static long computeNextSecondTimeMillis() {
       return ldtToMill(LocalDateTime.now().plusSeconds(1).withNano(0));
    }
    /**
     *  1点 到 6点
     * @return
     */
    public static boolean isTimetoDelete(){
        LocalDateTime now = LocalDateTime.now();
        int hour = now.getHour();
        if(hour >=1 && hour <= 6)
            return true;
        return false;
    }

    public static long getDelayEndTime(String delayEndStr){
        if(delayEndStr == null || delayEndStr.isEmpty())
            return delayEndZeroTime;
        long time = delayEndZeroTime;
        try{
            time = Long.valueOf(delayEndStr);
        }catch(Exception ex){
            log.error("prase delayEndStr error {}",delayEndStr);
        }
        return getSecond(time);

    }
    /**
     *  时间轮 下一个窗口的起始时间 202404151600
     */
    public static long getNextLoadTime(int timeSpan){
        LocalDateTime now = LocalDateTime.now();
        int minute = now.getMinute();
        int nextMinute = ( minute / timeSpan + 1) * timeSpan;
        LocalDateTime localDateTime1 = null;
        if (nextMinute == 60) {
            localDateTime1 = now.plusHours(1).withMinute(0);
        }else {
            localDateTime1 = now.withMinute(nextMinute);
        }
        String str = df.format(localDateTime1);
        return Long.valueOf(str);
    }

    public static TimeWindow getPrevLoadTime(long pulledSecond, int timeSpanMinute) {
        LocalDateTime localDateTime = milliToldt(pulledSecond * 1000)
                .withSecond(0).withNano(0);
        localDateTime = localDateTime.minusMinutes(timeSpanMinute);
        int minute = localDateTime.getMinute();
        int delayMinute = ( minute / timeSpanMinute ) * timeSpanMinute;

        long startTimeMs = ldtToMill(localDateTime.withMinute(delayMinute));

        String startTime = df.format(milliToldt(startTimeMs));


        int spanEndMinute = ( minute / timeSpanMinute + 1) * timeSpanMinute;

        LocalDateTime localDateTime1 = null;
        if (spanEndMinute == 60) {
            localDateTime1 = localDateTime.plusHours(1).withMinute(0);
        }else {
            localDateTime1 = localDateTime.withMinute(spanEndMinute);
        }

        long endTimeMs =  ldtToMill(localDateTime1);
        TimeWindow timeWindow = new TimeWindow();
        timeWindow.setStartTimeMs(startTimeMs);
        timeWindow.setEndTimeMs(endTimeMs);
        timeWindow.setMinuteTime(Long.valueOf(startTime));
        return timeWindow;
    }
    /**
     *  pulledSecond 秒 下一个时间窗口的下一个窗口
     *  [startTimeMs=1713168000000, endTimeMs=1713169800000, minuteTime=202404151600]
     */
    public static TimeWindow getNextLoadTime(long pulledSecond, int timeSpanMinute) {
        LocalDateTime localDateTime = milliToldt(pulledSecond * 1000)
                .withSecond(0).withNano(0);
        int minute = localDateTime.getMinute();
        int nextMinute = ( minute / timeSpanMinute + 1) * timeSpanMinute;
        if (nextMinute == 60) {
            localDateTime = localDateTime.plusHours(1).withMinute(0);
        } else {
            localDateTime = localDateTime.withMinute(nextMinute);
        }
        String startTime = df.format(localDateTime);

        long startTimeMs = ldtToMill(localDateTime);
        TimeWindow timeWindow = new TimeWindow();
        timeWindow.setStartTimeMs(startTimeMs);
        timeWindow.setEndTimeMs(startTimeMs+ 60000l * timeSpanMinute);
        timeWindow.setMinuteTime(Long.valueOf(startTime));
        return timeWindow;
    }


    /**
     *  下一个时间窗口的起始 时间戳  1713168000000
     */
    public static long getNexTime(int timeSpan) {
        LocalDateTime now = LocalDateTime.now().withNano(0).withSecond(0);
        int minute = now.getMinute();
        int nextMinute = ( minute / timeSpan + 1) * timeSpan;
        LocalDateTime localDateTime1 = null;
        if (nextMinute == 60) {
            localDateTime1 = now.plusHours(1).withMinute(0);
        }else {
            localDateTime1 = now.withMinute(nextMinute);
        }
       return ldtToMill(localDateTime1);
    }


    private static long ldtToMill(LocalDateTime ldt) {
      return   ldt.atZone(ZoneId.systemDefault()).
                toInstant().toEpochMilli();
    }

    private static LocalDateTime milliToldt(long time) {
       return LocalDateTime.ofInstant(
                Instant.ofEpochMilli(time),
                ZoneId.systemDefault());
    }

    public static DelayLogFileName parseFileName(final String fileName){
        //201812191530 + 00000000000001709864
        if(fileName == null || fileName.length() != 32)
            return null;
        String minuteStr = fileName.substring(0, 12);
        long minute = Long.valueOf(minuteStr);
        String offsetStr = fileName.substring(12,fileName.length());
        long offset = Long.valueOf(offsetStr);
        DelayLogFileName delayLogFileName = new DelayLogFileName();
        delayLogFileName.setMinute(minute);
        delayLogFileName.setOffset(offset);
        return delayLogFileName ;
    }

    public static class DelayLogFileName {
        long minute ;
        long offset ;
        public long getMinute() {
            return minute;
        }
        public void setMinute(long minute) {
            this.minute = minute;
        }
        public long getOffset() {
            return offset;
        }
        public void setOffset(long offset) {
            this.offset = offset;
        }
        @Override
        public String toString() {
            return "DelayLogFileName [minute=" + minute + ", offset=" + offset + "]";
        }


    }





    public static String file2String(final String fileName) throws IOException {
        File file = new File(fileName);
        return file2String(file);
    }

    public static String file2String(final File file) throws IOException {
        if (file.exists()) {
            // file.length() 磁盘文件大小
            byte[] data = new byte[(int) file.length()];
            boolean result;

            FileInputStream inputStream = null;
            try {
                inputStream = new FileInputStream(file);
                int len = inputStream.read(data);
                result = len == data.length;
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }

            if (result) {
                return new String(data);
            }
        }
        return null;
    }

    public static String file2String(final URL url) {
        InputStream in = null;
        try {
            URLConnection urlConnection = url.openConnection();
            urlConnection.setUseCaches(false);
            in = urlConnection.getInputStream();
            int len = in.available();
            byte[] data = new byte[len];
            in.read(data, 0, len);
            return new String(data, StandardCharsets.UTF_8);
        } catch (Exception ignored) {
        } finally {
            if (null != in) {
                try {
                    in.close();
                } catch (IOException ignored) {
                }
            }
        }

        return null;
    }

    /**
     *  新内容写入 tmp 临时文件
     *  获取磁盘中的数据文件
     *  吧磁盘的数据写入  .bak 备份上一个内容
     *  把上一个文件删除
     *  tmp临时文件 偷换成 新文件
     *
     *  总结：备份前一个文件，写入新文件
     */
    public static void string2File(final String str, final String fileName) throws IOException {

        String tmpFile = fileName + ".tmp";
        string2FileNotSafe(str, tmpFile);

        String bakFile = fileName + ".bak";
        // 获取磁盘中的数据文件
        String prevContent = file2String(fileName);
        if (prevContent != null) {
            // fileN 写入 bakFile
            string2FileNotSafe(prevContent, bakFile);
        }

        File file = new File(fileName);
        file.delete();

        file = new File(tmpFile);
        file.renameTo(new File(fileName));
    }

    public static void string2FileNotSafe(final String str, final String fileName) throws IOException {
        File file = new File(fileName);
        File fileParent = file.getParentFile();
        if (fileParent != null) {
            fileParent.mkdirs();
        }
        FileWriter fileWriter = null;

        try {
            fileWriter = new FileWriter(file);
            fileWriter.write(str);
        } catch (IOException e) {
            throw e;
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

    public static String offset2FileName(final long offset) {
        final NumberFormat nf = NumberFormat.getInstance();
        nf.setMinimumIntegerDigits(20);
        nf.setMaximumFractionDigits(0);
        nf.setGroupingUsed(false);
        return nf.format(offset);
    }


}
