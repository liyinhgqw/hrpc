package com.github.hrpc.util;

import java.lang.ref.WeakReference;
import java.util.*;

/**
 * Provides access to configuration parameters.
 *
 */
public class Option {

    private HashMap<String, String> optionMap = new HashMap<String, String>();

    private static final Map<ClassLoader, Map<String, WeakReference<Class<?>>>>
            CACHE_CLASSES = new WeakHashMap<ClassLoader, Map<String, WeakReference<Class<?>>>>();

    private ClassLoader classLoader;
    {
        classLoader = Thread.currentThread().getContextClassLoader();
        if (classLoader == null) {
            classLoader = Option.class.getClassLoader();
        }
    }

    private static final Class<?> NEGATIVE_CACHE_SENTINEL =
            NegativeCacheSentinel.class;

    /**
     * A unique class which is used as a sentinel value in the caching
     * for getClassByName. {@see Configuration#getClassByNameOrNull(String)}
     */
    private static abstract class NegativeCacheSentinel {}


    /**
     * Get the value of the <code>name</code> property, <code>null</code> if
     * no such property exists.
     *
     * @param name the property name.
     * @return the value of the <code>name</code>,
     *         or null if no such property exists.
     */
    public String get(String name) {
        String result = null;
        if (optionMap.containsKey(name)) {
            result = optionMap.get(name);
        }
        return result;
    }

    /**
     * Get the value of the <code>name</code>. If the key is deprecated,
     * it returns the value of the first key which replaces the deprecated key
     * and is not null.
     * If no such property exists,
     * then <code>defaultValue</code> is returned.
     *
     * @param name property name.
     * @param defaultValue default value.
     * @return property value, or <code>defaultValue</code> if the property
     *         doesn't exist.
     */
    public String get(String name, String defaultValue) {
        String result = this.get(name);
        return result == null ? defaultValue : result;
    }

    /**
     * Get the value of the <code>name</code> property as a trimmed <code>String</code>,
     * <code>null</code> if no such property exists.
     * If the key is deprecated, it returns the value of
     * the first key which replaces the deprecated key and is not null
     *
     * Values are processed for <a href="#VariableExpansion">variable expansion</a>
     * before being returned.
     *
     * @param name the property name.
     * @return the value of the <code>name</code> or its replacing property,
     *         or null if no such property exists.
     */
    public String getTrimmed(String name) {
        String value = get(name);

        if (null == value) {
            return null;
        } else {
            return value.trim();
        }
    }

    /**
     * Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
     * @param str a comma separated <String> with values
     * @return an array of <code>String</code> values
     */
    public static String[] getTrimmedStringsUtil(String str){
        if (null == str || str.trim().isEmpty()) {
            return emptyStringArray;
        }

        return str.trim().split("\\s*,\\s*");
    }

    final public static String[] emptyStringArray = {};

    /**
     * Get the comma delimited values of the <code>name</code> property as
     * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
     * If no such property is specified then an empty array is returned.
     *
     * @param name property name.
     * @return property value as an array of trimmed <code>String</code>s,
     *         or empty array.
     */
    public String[] getTrimmedStrings(String name) {
        String valueString = get(name);
        return getTrimmedStringsUtil(valueString);
    }

    /**
     * Get the comma delimited values of the <code>name</code> property as
     * an array of <code>String</code>s, trimmed of the leading and trailing whitespace.
     * If no such property is specified then default value is returned.
     *
     * @param name property name.
     * @param defaultValue The default value
     * @return property value as an array of trimmed <code>String</code>s,
     *         or default value.
     */
    public String[] getTrimmedStrings(String name, String... defaultValue) {
        String valueString = get(name);
        if (null == valueString) {
            return defaultValue;
        } else {
            return getTrimmedStringsUtil(valueString);
        }
    }

    /**
     * Set the <code>value</code> of the <code>name</code> property. If
     * <code>name</code> is deprecated or there is a deprecated name associated to it,
     * it sets the value to both names.
     *
     * @param name property name.
     * @param value property value.
     */
    public void set(String name, String value) {
        optionMap.put(name, value);
    }

    /**
     * Set the array of string values for the <code>name</code> property as
     * as comma delimited values.
     *
     * @param name property name.
     * @param values The values
     */
    public void setStrings(String name, String... values) {
        set(name, arrayToString(values));
    }

    /**
     * Given an array of strings, return a comma-separated list of its elements.
     * @param strs Array of strings
     * @return Empty string if strs.length is 0, comma separated list of strings
     * otherwise
     */

    public static String arrayToString(String[] strs) {
        if (strs.length == 0) { return ""; }
        StringBuilder sbuf = new StringBuilder();
        sbuf.append(strs[0]);
        for (int idx = 1; idx < strs.length; idx++) {
            sbuf.append(",");
            sbuf.append(strs[idx]);
        }
        return sbuf.toString();
    }

    /**
     * Get the value of the <code>name</code> property as an <code>int</code>.
     *
     * If no such property exists, the provided default value is returned,
     * or if the specified value is not a valid <code>int</code>,
     * then an error is thrown.
     *
     * @param name property name.
     * @param defaultValue default value.
     * @throws NumberFormatException when the value is invalid
     * @return property value as an <code>int</code>,
     *         or <code>defaultValue</code>.
     */
    public int getInt(String name, int defaultValue) {
        String valueString = getTrimmed(name);
        if (valueString == null)
            return defaultValue;
        String hexString = getHexDigits(valueString);
        if (hexString != null) {
            return Integer.parseInt(hexString, 16);
        }
        return Integer.parseInt(valueString);
    }

    /**
     * Get the value of the <code>name</code> property as a set of comma-delimited
     * <code>int</code> values.
     *
     * If no such property exists, an empty array is returned.
     *
     * @param name property name
     * @return property value interpreted as an array of comma-delimited
     *         <code>int</code> values
     */
    public int[] getInts(String name) {
        String[] strings = getTrimmedStrings(name);
        int[] ints = new int[strings.length];
        for (int i = 0; i < strings.length; i++) {
            ints[i] = Integer.parseInt(strings[i]);
        }
        return ints;
    }

    /**
     * Set the value of the <code>name</code> property to an <code>int</code>.
     *
     * @param name property name.
     * @param value <code>int</code> value of the property.
     */
    public void setInt(String name, int value) {
        set(name, Integer.toString(value));
    }


    /**
     * Get the value of the <code>name</code> property as a <code>long</code>.
     * If no such property exists, the provided default value is returned,
     * or if the specified value is not a valid <code>long</code>,
     * then an error is thrown.
     *
     * @param name property name.
     * @param defaultValue default value.
     * @throws NumberFormatException when the value is invalid
     * @return property value as a <code>long</code>,
     *         or <code>defaultValue</code>.
     */
    public long getLong(String name, long defaultValue) {
        String valueString = getTrimmed(name);
        if (valueString == null)
            return defaultValue;
        String hexString = getHexDigits(valueString);
        if (hexString != null) {
            return Long.parseLong(hexString, 16);
        }
        return Long.parseLong(valueString);
    }

    /**
     * Set the value of the <code>name</code> property to a <code>long</code>.
     *
     * @param name property name.
     * @param value <code>long</code> value of the property.
     */
    public void setLong(String name, long value) {
        set(name, Long.toString(value));
    }

    private String getHexDigits(String value) {
        boolean negative = false;
        String str = value;
        String hexString = null;
        if (value.startsWith("-")) {
            negative = true;
            str = value.substring(1);
        }
        if (str.startsWith("0x") || str.startsWith("0X")) {
            hexString = str.substring(2);
            if (negative) {
                hexString = "-" + hexString;
            }
            return hexString;
        }
        return null;
    }

    /**
     * Get the value of the <code>name</code> property as a <code>float</code>.
     * If no such property exists, the provided default value is returned,
     * or if the specified value is not a valid <code>float</code>,
     * then an error is thrown.
     *
     * @param name property name.
     * @param defaultValue default value.
     * @throws NumberFormatException when the value is invalid
     * @return property value as a <code>float</code>,
     *         or <code>defaultValue</code>.
     */
    public float getFloat(String name, float defaultValue) {
        String valueString = getTrimmed(name);
        if (valueString == null)
            return defaultValue;
        return Float.parseFloat(valueString);
    }

    /**
     * Set the value of the <code>name</code> property to a <code>float</code>.
     *
     * @param name property name.
     * @param value property value.
     */
    public void setFloat(String name, float value) {
        set(name,Float.toString(value));
    }

    /**
     * Get the value of the <code>name</code> property as a <code>double</code>.
     * If no such property exists, the provided default value is returned,
     * or if the specified value is not a valid <code>double</code>,
     * then an error is thrown.
     *
     * @param name property name.
     * @param defaultValue default value.
     * @throws NumberFormatException when the value is invalid
     * @return property value as a <code>double</code>,
     *         or <code>defaultValue</code>.
     */
    public double getDouble(String name, double defaultValue) {
        String valueString = getTrimmed(name);
        if (valueString == null)
            return defaultValue;
        return Double.parseDouble(valueString);
    }

    /**
     * Set the value of the <code>name</code> property to a <code>double</code>.
     *
     * @param name property name.
     * @param value property value.
     */
    public void setDouble(String name, double value) {
        set(name,Double.toString(value));
    }

    /**
     * Get the value of the <code>name</code> property as a <code>boolean</code>.
     * If no such property is specified, or if the specified value is not a valid
     * <code>boolean</code>, then <code>defaultValue</code> is returned.
     *
     * @param name property name.
     * @param defaultValue default value.
     * @return property value as a <code>boolean</code>,
     *         or <code>defaultValue</code>.
     */
    public boolean getBoolean(String name, boolean defaultValue) {
        String valueString = getTrimmed(name);
        if (null == valueString || valueString.isEmpty()) {
            return defaultValue;
        }

        valueString = valueString.toLowerCase();

        if ("true".equals(valueString))
            return true;
        else if ("false".equals(valueString))
            return false;
        else return defaultValue;
    }

    /**
     * Set the value of the <code>name</code> property to a <code>boolean</code>.
     *
     * @param name property name.
     * @param value <code>boolean</code> value of the property.
     */
    public void setBoolean(String name, boolean value) {
        set(name, Boolean.toString(value));
    }

    /**
     * Set the value of the <code>name</code> property to the given type. This
     * is equivalent to <code>set(&lt;name&gt;, value.toString())</code>.
     * @param name property name
     * @param value new value
     */
    public <T extends Enum<T>> void setEnum(String name, T value) {
        set(name, value.toString());
    }

    /**
     * Return value matching this enumerated type.
     * @param name Property name
     * @param defaultValue Value returned if no mapping exists
     * @throws IllegalArgumentException If mapping is illegal for the type
     * provided
     */
    public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
        final String val = get(name);
        return null == val
                ? defaultValue
                : Enum.valueOf(defaultValue.getDeclaringClass(), val);
    }


    /**
     * A class that represents a set of positive integer ranges. It parses
     * strings of the form: "2-3,5,7-" where ranges are separated by comma and
     * the lower/upper bounds are separated by dash. Either the lower or upper
     * bound may be omitted meaning all values up to or over. So the string
     * above means 2, 3, 5, and 7, 8, 9, ...
     */
    public static class IntegerRanges implements Iterable<Integer>{
        private static class Range {
            int start;
            int end;
        }

        private static class RangeNumberIterator implements Iterator<Integer> {
            Iterator<Range> internal;
            int at;
            int end;

            public RangeNumberIterator(List<Range> ranges) {
                if (ranges != null) {
                    internal = ranges.iterator();
                }
                at = -1;
                end = -2;
            }

            @Override
            public boolean hasNext() {
                if (at <= end) {
                    return true;
                } else if (internal != null){
                    return internal.hasNext();
                }
                return false;
            }

            @Override
            public Integer next() {
                if (at <= end) {
                    at++;
                    return at - 1;
                } else if (internal != null){
                    Range found = internal.next();
                    if (found != null) {
                        at = found.start;
                        end = found.end;
                        at++;
                        return at - 1;
                    }
                }
                return null;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };

        List<Range> ranges = new ArrayList<Range>();

        public IntegerRanges() {
        }

        public IntegerRanges(String newValue) {
            StringTokenizer itr = new StringTokenizer(newValue, ",");
            while (itr.hasMoreTokens()) {
                String rng = itr.nextToken().trim();
                String[] parts = rng.split("-", 3);
                if (parts.length < 1 || parts.length > 2) {
                    throw new IllegalArgumentException("integer range badly formed: " +
                            rng);
                }
                Range r = new Range();
                r.start = convertToInt(parts[0], 0);
                if (parts.length == 2) {
                    r.end = convertToInt(parts[1], Integer.MAX_VALUE);
                } else {
                    r.end = r.start;
                }
                if (r.start > r.end) {
                    throw new IllegalArgumentException("IntegerRange from " + r.start +
                            " to " + r.end + " is invalid");
                }
                ranges.add(r);
            }
        }

        /**
         * Convert a string to an int treating empty strings as the default value.
         * @param value the string value
         * @param defaultValue the value for if the string is empty
         * @return the desired integer
         */
        private static int convertToInt(String value, int defaultValue) {
            String trim = value.trim();
            if (trim.length() == 0) {
                return defaultValue;
            }
            return Integer.parseInt(trim);
        }

        /**
         * Is the given value in the set of ranges
         * @param value the value to check
         * @return is the value in the ranges?
         */
        public boolean isIncluded(int value) {
            for(Range r: ranges) {
                if (r.start <= value && value <= r.end) {
                    return true;
                }
            }
            return false;
        }

        /**
         * @return true if there are no values in this range, else false.
         */
        public boolean isEmpty() {
            return ranges == null || ranges.isEmpty();
        }

        @Override
        public String toString() {
            StringBuilder result = new StringBuilder();
            boolean first = true;
            for(Range r: ranges) {
                if (first) {
                    first = false;
                } else {
                    result.append(',');
                }
                result.append(r.start);
                result.append('-');
                result.append(r.end);
            }
            return result.toString();
        }

        @Override
        public Iterator<Integer> iterator() {
            return new RangeNumberIterator(ranges);
        }

    }

    /**
     * Parse the given attribute as a set of integer ranges
     * @param name the attribute name
     * @param defaultValue the default value if it is not set
     * @return a new set of ranges from the configured value
     */
    public IntegerRanges getRange(String name, String defaultValue) {
        return new IntegerRanges(get(name, defaultValue));
    }

    /**
     * Set the value of the <code>name</code> property to the name of a
     * <code>theClass</code> implementing the given interface <code>xface</code>.
     *
     * An exception is thrown if <code>theClass</code> does not implement the
     * interface <code>xface</code>.
     *
     * @param name property name.
     * @param theClass property value.
     * @param xface the interface implemented by the named class.
     */
    public void setClass(String name, Class<?> theClass, Class<?> xface) {
        if (!xface.isAssignableFrom(theClass))
            throw new RuntimeException(theClass+" not "+xface.getName());
        set(name, theClass.getName());
    }

    /**
     * Get the value of the <code>name</code> property as a <code>Class</code>.
     * If no such property is specified, then <code>defaultValue</code> is
     * returned.
     *
     * @param name the class name.
     * @param defaultValue default value.
     * @return property value as a <code>Class</code>,
     *         or <code>defaultValue</code>.
     */
    public Class<?> getClass(String name, Class<?> defaultValue) {
        String valueString = getTrimmed(name);
        if (valueString == null)
            return defaultValue;
        try {
            return getClassByName(valueString);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Load a class by name.
     *
     * @param name the class name.
     * @return the class object.
     * @throws ClassNotFoundException if the class is not found.
     */
    public Class<?> getClassByName(String name) throws ClassNotFoundException {
        Class<?> ret = getClassByNameOrNull(name);
        if (ret == null) {
            throw new ClassNotFoundException("Class " + name + " not found");
        }
        return ret;
    }

    /**
     * Load a class by name, returning null rather than throwing an exception
     * if it couldn't be loaded. This is to avoid the overhead of creating
     * an exception.
     *
     * @param name the class name
     * @return the class object, or null if it could not be found.
     */
    public Class<?> getClassByNameOrNull(String name) {
        Map<String, WeakReference<Class<?>>> map;

        synchronized (CACHE_CLASSES) {
            map = CACHE_CLASSES.get(classLoader);
            if (map == null) {
                map = Collections.synchronizedMap(
                        new WeakHashMap<String, WeakReference<Class<?>>>());
                CACHE_CLASSES.put(classLoader, map);
            }
        }

        Class<?> clazz = null;
        WeakReference<Class<?>> ref = map.get(name);
        if (ref != null) {
            clazz = ref.get();
        }

        if (clazz == null) {
            try {
                clazz = Class.forName(name, true, classLoader);
            } catch (ClassNotFoundException e) {
                // Leave a marker that the class isn't found
                map.put(name, new WeakReference<Class<?>>(NEGATIVE_CACHE_SENTINEL));
                return null;
            }
            // two putters can race here, but they'll put the same class
            map.put(name, new WeakReference<Class<?>>(clazz));
            return clazz;
        } else if (clazz == NEGATIVE_CACHE_SENTINEL) {
            return null; // not found
        } else {
            // cache hit
            return clazz;
        }
    }
}
