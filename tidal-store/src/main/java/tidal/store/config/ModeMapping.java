package tidal.store.config;

public class ModeMapping {

	public static int getMap(boolean isCluster) {
		if (isCluster) {
			return 1;
		} else {
			return 0;
		}
	}
}
