package com.github.mxb.flink.sql.util;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;

public class JarUtils {

    public static List<File> getJars(String jarDir) {
        if (StringUtils.isBlank(jarDir)) {
            return Collections.emptyList();
        }

        Collection<File> files = FileUtils.listFiles(new File(jarDir), new String[]{"jar"}, true);
        return new LinkedList<>(files);
    }

    public static List<URL> toURLs(List<File> files) throws MalformedURLException {
        LinkedList<URL> urls = new LinkedList<>();
        for (File file : files) {
            urls.add(file.toURI().toURL());
        }
        return urls;
    }
}
