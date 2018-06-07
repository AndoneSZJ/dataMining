package com.seven.spark.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DecimalFormat;

/**
 * Created by IntelliJ IDEA.
 *         __   __
 *         \/---\/
 *          ). .(
 *         ( (") )
 *          )   (
 *         /     \
 *        (       )``
 *       ( \ /-\ / )
 *        w'W   W'w
 *
 * author   seven
 * email    sevenstone@yeah.net
 * date     2018/5/16 上午10:37
 */
public class Utils {
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    private static final String SLASH = File.separator;
    private static final String DOT = ".";

    private static final int BUFFER_SIZE = 2 << 12;

    private static final String NUMBER_FORMAT = "0000";

    /**
     * 根据文件路径获取文件名
     *
     * @param filePath 文件路径
     * @return 文件名
     */
    public static String getFileNamePrefix(String filePath) {
        String fileName = filePath.trim();
        if (fileName.lastIndexOf(SLASH) != -1) {
            fileName = fileName.substring(fileName.lastIndexOf(SLASH) + 1);
        }
        if (fileName.lastIndexOf(DOT) != -1) {
            fileName = fileName.substring(0, fileName.lastIndexOf(DOT));
        }
        return fileName;
    }

    public static String getFileFormat(String filePath) {
        if (filePath.lastIndexOf(DOT) != -1) {
            filePath = filePath.substring(filePath.lastIndexOf(DOT) + 1, filePath.length());
        }
        return filePath;
    }

    /**
     * 文件转换成字节数组
     *
     * @param filePath 文件路径
     * @return 文件对应的字节数组
     * @throws IOException
     */
    public static byte[] fileToByteArray(String filePath)
            throws IOException {
        return fileToByteArray(new File(filePath));
    }

    /**
     * 文件转换成字节数组
     *
     * @param file 文件对象
     * @return 文件对应的字节数组
     * @throws IOException
     */
    public static byte[] fileToByteArray(File file)
            throws IOException {
        if (!file.exists()) {
            LOG.error("file [{}] not exists", file.getAbsolutePath());
            throw new FileNotFoundException(file.getAbsolutePath());
        }
        ByteArrayOutputStream out = null;
        BufferedInputStream in = null;
        try {
            in = new BufferedInputStream(new FileInputStream(file));
            out = new ByteArrayOutputStream((int) file.length());
            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = in.read(buffer, 0, buffer.length)) != -1) {
                out.write(buffer, 0, len);
            }
            return out.toByteArray();
        } finally {
            close(in, out);
        }
    }

    /**
     * 字节数组转换成文件
     *
     * @param filePath 写入的文件路径
     * @param data     字节数组
     * @return 文件对象
     * @throws IOException
     */
    public static File byteArrayToFile(String filePath, byte[] data)
            throws IOException {
        FileOutputStream out = null;
        File file = new File(filePath);
        try {
            out = new FileOutputStream(file);
            out.write(data, 0, data.length);
        } finally {
            close(out);
        }
        return file;
    }

    public static byte[] inputStreamToByteArray(InputStream in) throws IOException {
        ByteArrayOutputStream bos = null;
        try {
            byte[] buffer = new byte[BUFFER_SIZE];
            bos = new ByteArrayOutputStream();
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                bos.write(buffer, 0, bytesRead);
            }
            bos.flush();
            return bos.toByteArray();
        } finally {
            close(bos);
        }
    }

    /**
     * 拷贝一个流到另外一个流
     *
     * @param in  输入流
     * @param out 输出流
     * @throws IOException
     */
    public static void copyBytes(InputStream in, OutputStream out)
            throws IOException {
        copyBytes(in, out, BUFFER_SIZE);
    }

    /**
     * 拷贝一个流到另外一个流
     *
     * @param in       输入流
     * @param out      输出流
     * @param buffSize 缓冲区大小
     * @throws IOException
     */
    public static void copyBytes(InputStream in, OutputStream out, int buffSize)
            throws IOException {
        PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        byte buf[] = new byte[buffSize];
        int bytesRead = in.read(buf);
        while (bytesRead >= 0) {
            out.write(buf, 0, bytesRead);
            if ((ps != null) && ps.checkError()) {
                throw new IOException("Unable to write to output stream.");
            }
            bytesRead = in.read(buf);
        }
    }

    /**
     * 拷贝一个流到另外一个流
     *
     * @param in       输入流
     * @param out      输出流
     * @param buffSize 缓冲区大小
     * @param close    是否关闭流
     * @throws IOException
     */
    public static void copyBytes(InputStream in, OutputStream out, int buffSize, boolean close)
            throws IOException {
        try {
            copyBytes(in, out, buffSize);
            if (close) {
                out.close();
                out = null;
                in.close();
                in = null;
            }
        } finally {
            if (close) {
                close(out, in);
            }
        }
    }

    /**
     * 关闭实现了Closeable接口的资源
     *
     * @param closeables 可关闭的资源
     */
    public static void close(Closeable... closeables) {
        cleanup(null, closeables);
    }

    public static void cleanup(Logger log, Closeable... closeables) {
        for (Closeable c : closeables) {
            if (c != null) {
                try {
                    c.close();
                } catch (IOException e) {
                    if (log != null && log.isDebugEnabled()) {
                        log.debug("Exception in closing " + c, e);
                    }
                }
            }
        }
    }

    /**
     * 根据类名实例化对象
     *
     * @param className 类名
     * @param <T>       类型
     * @return className对应类的实例
     */
    public static <T> T newInstance(String className) {
        try {
            Class clazz = Class.forName(className);
            return (T) clazz.newInstance();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String incrementAndGet(long id) {
        return incrementAndGet(String.valueOf(id));
    }

    /**
     * 字符串+1
     *
     * @param id 字符串数字
     * @return 字符串+1,不足位数前补0
     */
    public static String incrementAndGet(String id) {
        return incrementAndGet(id, NUMBER_FORMAT);
    }

    /**
     * 字符串+1
     *
     * @param id     字符串数字
     * @param format 格式
     * @return 字符串+1,不足位数前补0
     */
    public static String incrementAndGet(String id, String format) {
        Long currentId = Long.parseLong(id);
        currentId++;
        DecimalFormat df = new DecimalFormat(format);
        return df.format(currentId);
    }

    public static String numberFormat(Long id, String format) {
        DecimalFormat df = new DecimalFormat(format);
        return df.format(id);
    }

    /**
     * 字符串
     *
     * @param id     字符串数字
     * @param format 格式
     * @return 字符串数字格式化,不足位数前补0
     */
    public static String numberFormat(String id, String format) {
        return numberFormat(Long.parseLong(id), format);
    }
}
