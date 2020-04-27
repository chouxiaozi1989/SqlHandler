package org.caiyi.SqlHandler.util;

import java.text.DateFormatSymbols;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;

public class DateUtil {
    public DateUtil() {
    }

    public static String FormatDate(Date date, String format) throws Exception {
        return dateToString(date, format);
    }

    public static String dateToString(Date date, String format) throws Exception {
        if (date == null) {
            return null;
        } else if (format != null && !format.equalsIgnoreCase("")) {
            Hashtable<Integer, String> h = new Hashtable();
            StringBuilder javaFormat = new StringBuilder();
            if (format.contains("yyyy")) {
                h.put(format.indexOf("yyyy"), "yyyy");
            } else if (format.contains("yy")) {
                h.put(format.indexOf("yy"), "yy");
            }

            if (format.contains("MM")) {
                h.put(format.indexOf("MM"), "MM");
            } else if (format.contains("mm")) {
                h.put(format.indexOf("mm"), "MM");
            }

            if (format.contains("dd")) {
                h.put(format.indexOf("dd"), "dd");
            }

            if (format.contains("hh24")) {
                h.put(format.indexOf("hh24"), "HH");
            } else if (format.contains("hh")) {
                h.put(format.indexOf("hh"), "HH");
            } else if (format.contains("HH")) {
                h.put(format.indexOf("HH"), "HH");
            }

            if (format.contains("mi")) {
                h.put(format.indexOf("mi"), "mm");
            } else if (format.contains("mm") && h.containsValue("HH")) {
                h.put(format.lastIndexOf("mm"), "mm");
            }

            if (format.contains("ss")) {
                h.put(format.indexOf("ss"), "ss");
            }

            if (format.contains("SSS")) {
                h.put(format.indexOf("SSS"), "SSS");
            }

            int i;
            for (i = 0; format.indexOf("-", i) != -1; ++i) {
                i = format.indexOf("-", i);
                h.put(i, "-");
            }

            for (i = 0; format.indexOf(".", i) != -1; ++i) {
                i = format.indexOf(".", i);
                h.put(i, ".");
            }

            for (i = 0; format.indexOf("/", i) != -1; ++i) {
                i = format.indexOf("/", i);
                h.put(i, "/");
            }

            for (i = 0; format.indexOf(" ", i) != -1; ++i) {
                i = format.indexOf(" ", i);
                h.put(i, " ");
            }

            for (i = 0; format.indexOf(":", i) != -1; ++i) {
                i = format.indexOf(":", i);
                h.put(i, ":");
            }

            if (format.contains("年")) {
                h.put(format.indexOf("年"), "年");
            }

            if (format.contains("月")) {
                h.put(format.indexOf("月"), "月");
            }

            if (format.contains("日")) {
                h.put(format.indexOf("日"), "日");
            }

            if (format.contains("时")) {
                h.put(format.indexOf("时"), "时");
            }

            if (format.contains("分")) {
                h.put(format.indexOf("分"), "分");
            }

            if (format.contains("秒")) {
                h.put(format.indexOf("秒"), "秒");
            }

            boolean var9 = false;

            while (h.size() != 0) {
                Enumeration<Integer> e = h.keys();
                int n = 0;

                while (e.hasMoreElements()) {
                    i = (Integer) e.nextElement();
                    if (i >= n) {
                        n = i;
                    }
                }

                String temp = (String) h.get(n);
                h.remove(n);
                javaFormat.insert(0, temp);
            }

            SimpleDateFormat df = new SimpleDateFormat(javaFormat.toString(), new DateFormatSymbols());
            return df.format(date);
        } else {
            throw new Exception("传入参数中的[时间格式]为空");
        }
    }
}