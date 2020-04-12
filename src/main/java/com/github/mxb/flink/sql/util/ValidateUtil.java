package com.github.mxb.flink.sql.util;

import org.apache.flink.table.api.ValidationException;

import java.math.BigInteger;
import java.util.Map;
import java.util.function.Consumer;

/**
 * @description     ValidateUtil
 * @auther          moxianbin
 * @create          2020-04-12 11:40:19
 */
public class ValidateUtil {
    private ValidateUtil(){}

    /**
     * Validates that a certain value is present under the given key.
     */
    public static void validateValue(Map<String, String> properties,String key, String value, boolean isOptional) {
        validateOptional(
                properties,
                key,
                isOptional,
                (v) -> {
                    if (!v.equals(value)) {
                        throw new ValidationException(
                                "Could not find required value '" + value + "' for property '" + key + "'.");
                    }
                });
    }

    public static void validateOptional(Map<String, String> properties, String key, boolean isOptional) {
        if (!properties.containsKey(key)) {
            if (!isOptional) {
                throw new ValidationException("Could not find required property '" + key + "'.");
            }
        } else {
            final String value = properties.get(key);
        }
    }

    private static void validateOptional(Map<String, String> properties, String key, boolean isOptional, Consumer<String> valueValidation) {
        if (!properties.containsKey(key)) {
            if (!isOptional) {
                throw new ValidationException("Could not find required property '" + key + "'.");
            }
        } else {
            final String value = properties.get(key);
            valueValidation.accept(value);
        }
    }

    /**
     * 校验为正数则返回该数字，否则抛出异常.
     */
    public static int positive(String reason, int x) {

        if (x <= 0) {
            throw new IllegalArgumentException(reason + " (" + x + ") must be > 0");
        }
        return x;
    }

    /**
     * 校验为正数则返回该数字，否则抛出异常.
     */
    public static long positive(String reason, long x) {
        if (x <= 0) {
            throw new IllegalArgumentException(reason + " (" + x + ") must be > 0");
        }
        return x;
    }

    /**
     * 校验为正数则返回该数字，否则抛出异常.
     */
    static BigInteger positive(String reason, BigInteger x) {
        if (x.signum() <= 0) {
            throw new IllegalArgumentException(reason + " (" + x + ") must be > 0");
        }
        return x;
    }

    /**
     * 校验为正数则返回该数字，否则抛出异常.
     */
    public static int nonNegative(String reason, int x) {
        if (x < 0) {
            throw new IllegalArgumentException(reason + " (" + x + ") must be >= 0");
        }
        return x;
    }

    /**
     * 校验为正数则返回该数字，否则抛出异常.
     */
    public static long nonNegative(String reason, long x) {
        if (x < 0) {
            throw new IllegalArgumentException(reason + " (" + x + ") must be >= 0");
        }
        return x;
    }

    /**
     * 校验为正数则返回该数字，否则抛出异常.
     */
    public static BigInteger nonNegative(String reason, BigInteger x) {
        if (x.signum() < 0) {
            throw new IllegalArgumentException(reason + " (" + x + ") must be >= 0");
        }
        return x;
    }

    /**
     * 校验为正数则返回该数字，否则抛出异常.
     */
    public static double nonNegative(String reason, double x) {
        if (!(x >= 0)) { // not x < 0, to work with NaN.
            throw new IllegalArgumentException(reason + " (" + x + ") must be >= 0");
        }
        return x;
    }

}
