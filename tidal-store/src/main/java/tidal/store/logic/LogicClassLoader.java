package tidal.store.logic;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import tidal.logic.LogicFactory;

public class LogicClassLoader {

	public static boolean isPresent(String className) {
		try {
			Class.forName(className);
			return true;
		} catch (Throwable ex) {
			// Class or one of its dependencies is not present...
			return false;
		}
	}

	public static boolean checkPloyMapping(int ploy, String className) {
		Class<LogicFactory> findClass = findClass(className);
		List<Class<?>> allInterfaces = new ArrayList<Class<?>>();
		getInterfaces(findClass, allInterfaces);

		boolean flag = false;
		for (Class<?> clz : allInterfaces) {
			if (clz.getName().equals("tidal.logic.IncrLogicFactory") && ploy == 0) {
				flag = true;
				break;
			} else if (clz.getName().equals("tidal.logic.DateTimeLogicFactory") && ploy == 1) {
				flag = true;
				break;
			} else if (clz.getName().equals("tidal.logic.DateTimeLogicFactory") && ploy == 2) {
				flag = true;
				break;
			} else if (clz.getName().equals("tidal.logic.DateTimeLogicFactory") && ploy == 3) {
				flag = true;
				break;
			}
		}
		return flag;
	}

	public static void getInterfaces(Class<?> cls, final List<Class<?>> allInterfaces) {
		Class<?>[] interfaces = cls.getInterfaces();
		do {
			for (Class<?> clz : interfaces) {
				allInterfaces.add(clz);
				getInterfaces(clz, allInterfaces);
			}
			break;
		} while (interfaces != null);
	}

	@SuppressWarnings("unchecked")
	public static Class<LogicFactory> findClass(String className) {
		Class<LogicFactory> cls;
		try {
			cls = (Class<LogicFactory>) Class.forName(className);
			return cls;
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
		return null;
	}

	public static LogicFactory newInstance(Class<LogicFactory> cls) {
		try {
			return cls.newInstance();
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return null;
	}

	/**
	 * for incr
	 * 
	 * @param cls
	 * @param lf
	 * @param mySid
	 * @param value
	 * @return
	 */
	public static String invoke(Class<LogicFactory> cls, LogicFactory lf, int mySid, int value) {

		Method method;
		try {

			method = cls.getMethod("build", int.class, int.class);
			return (String) method.invoke(lf, mySid, value);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;

	}

	/**
	 * for dateTime
	 * 
	 * @param cls
	 * @param lf
	 * @param mySid
	 * @param time
	 * @param value
	 * @return
	 */
	public static String invoke(Class<LogicFactory> cls, LogicFactory lf, int mySid, long time, int value) {

		Method method;
		try {

			method = cls.getMethod("build", int.class, long.class, int.class);
			return (String) method.invoke(lf, mySid, time, value);
		} catch (NoSuchMethodException e) {
			e.printStackTrace();
		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return null;

	}

}
