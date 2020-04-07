package com.github.mxb.flink.sql.util;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

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

    public static Map<String, Map<String, byte[]>> readJarFilesAsMap(List<File> files) {
        Validate.notNull(files, "files不能为null");
        Validate.isTrue(!files.contains(null), "files不能包含null元素");

        LinkedHashMap<String, Map<String, byte[]>> fileEntriesMap = new LinkedHashMap<>();
        for (File file : files) {
            String jarAbsolutePath = file.getAbsolutePath();
            LinkedHashMap<String, byte[]> jarEntries = new LinkedHashMap<>();

            try (FileInputStream fileInputStream = new FileInputStream(file)) {
                byte[] jarBytes = IOUtils.toByteArray(fileInputStream);
                try (ZipArchiveInputStream zipArchiveInputStream =
                             new ZipArchiveInputStream(new ByteArrayInputStream(jarBytes))) {
                    ZipArchiveEntry nextZipEntry;
                    while ((nextZipEntry = zipArchiveInputStream.getNextZipEntry()) != null) {
                        String resourceName = nextZipEntry.getName();
                        byte[] resourceBytes = IOUtils.toByteArray(zipArchiveInputStream);
                        jarEntries.put(resourceName, resourceBytes);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("读取文件失败: " + jarAbsolutePath, e);
            }

            fileEntriesMap.put(jarAbsolutePath, jarEntries);
        }

        return fileEntriesMap;
    }
}
