package tidal.store.config;

import java.io.File;

public class StorePathConfigHelper {

	public static String getLockFile(final String rootDir) {
		return rootDir + File.separator + "lock";
	}

	public static String getStoreFilePath(final String rootDir) {
		return rootDir + File.separator + "store";
	}
}
