package tidal.server.util;

import java.lang.reflect.Field;

import tidal.common.annotation.ImportantField;

public class EnvironmentUtil {

	public static void printConfig(boolean isImportant, Object... object) {
		for (Object obj : object) {
			Class<? extends Object> cls = obj.getClass();
			Field[] fields = cls.getDeclaredFields();
			Field.setAccessible(fields, true);
			for (Field field : fields) {
				if (isImportant) {
					boolean fieldHasAnno = field.isAnnotationPresent(ImportantField.class);
					if (fieldHasAnno) {
						try {
							System.out.println(field.getName() + ":" + field.get(obj));
						} catch (Exception e) {
						}
					}
				} else {
					try {
						System.out.println(field.getName() + ":" + field.get(obj));
					} catch (Exception e) {
					}
				}

			}
		}
	}
}
