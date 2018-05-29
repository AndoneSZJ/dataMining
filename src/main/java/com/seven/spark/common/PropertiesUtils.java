package com.seven.spark.common;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA.
 * author   seven
 * email    sevenstone@yeah.net
 * date     2018/5/27 上午10:00
 */
public class PropertiesUtils {
    private static final Logger LOG = LoggerFactory.getLogger(PropertiesUtils.class);

    private static Configuration conf;

    private PropertiesUtils() {

    }

    static {
        try {
            LOG.debug("Loading config.properties...");
            conf = new PropertiesConfiguration("config.properties");
            LOG.debug("Loaded.");
        } catch (ConfigurationException e) {
            LOG.error("Failed to load config.properties." + ExceptionUtils.getStackTrace(e));
        }
    }

    public static Configuration getConfiguration() {
        return conf;
    }
}
