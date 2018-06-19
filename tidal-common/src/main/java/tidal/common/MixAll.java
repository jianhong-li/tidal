package tidal.common;

import java.lang.reflect.Method;
import java.util.Properties;

public class MixAll {

	public static final String TIDAL_HOME_ENV = "TIDAL_HOME";
	public static final String TIDAL_HOME_PROPERTY = "tidal.home.dir";
	public static final String TIDAL_ADDR_ENV = "SERVER_ADDR";
	public static final String TIDAL_ADDR_PROPERTY = "tidal.server.addr";

	public static void properties2Object(final Properties p, final Object object) {
		Method[] methods = object.getClass().getMethods();
		for (Method method : methods) {
			String mn = method.getName();
			if (mn.startsWith("set")) {
				try {
					String tmp = mn.substring(4);
					String first = mn.substring(3, 4);

					String key = first.toLowerCase() + tmp;
					String property = p.getProperty(key);
					if (property != null) {
						Class<?>[] pt = method.getParameterTypes();
						if (pt != null && pt.length > 0) {
							String cn = pt[0].getSimpleName();
							Object arg = null;
							if (cn.equals("int") || cn.equals("Integer")) {
								arg = Integer.parseInt(property);
							} else if (cn.equals("long") || cn.equals("Long")) {
								arg = Long.parseLong(property);
							} else if (cn.equals("double") || cn.equals("Double")) {
								arg = Double.parseDouble(property);
							} else if (cn.equals("boolean") || cn.equals("Boolean")) {
								arg = Boolean.parseBoolean(property);
							} else if (cn.equals("float") || cn.equals("Float")) {
								arg = Float.parseFloat(property);
							} else if (cn.equals("String")) {
								arg = property;
							} else {
								continue;
							}
							method.invoke(object, arg);
						}
					}
				} catch (Throwable ignored) {
				}
			}
		}
	}

}
