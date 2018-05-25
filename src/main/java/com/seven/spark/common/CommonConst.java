package com.seven.spark.common;

/**
 * Created by seven
 */
public class CommonConst {
    public static final String RESOURCE_PROTOCOL_LOCAL="local";
    public static final String RESOURCE_PROTOCOL_SFTP="sftp";
    public static final String RESOURCE_PROTOCOL_FTP="ftp";
    public static final String RESOURCE_PROTOCOL_HDFS="hdfs";
    public static final String HDFS_PATH_PREFIX = "hdfs://";
    public static final String FTP_PATH_PREFIX = "ftp://";
    public static final String SFTP_PATH_PREFIX = "sftp://";
    public static final String WRITETYPE_HDFS= "hdfs";
    public static final String WRITETYPE_FTP = "ftp";
    public static final String WRITETYPE_SFTP= "sftp";
    public static final String WRITETYPE_LOCAL = "local";

    /** 状态有效 **/
    public static final String VALID = "1";
    /** 状态无效 **/
    public static final String INVALID = "0";


    /**
     * 文件表
     */
    public static final String HBASE_FILE_TABLE_NAME = "imagelocation_file";
    public static final String HBASE_FILE_COLUMN_FAMILY = "m"; // media
    public static final String DEFAULT_HBASE_COLUMN_FAMILY = "cf"; // family
    public static final String DEFAULT_HBASE_QUALIFIER_THUMBNAIL = "t"; // thumbnail
}
