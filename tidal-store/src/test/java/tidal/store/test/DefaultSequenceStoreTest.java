package tidal.store.test;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import tidal.common.protocol.body.ComponentData;
import tidal.store.DefaultTidalStore;
import tidal.store.config.StoreConfig;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class DefaultSequenceStoreTest {

	private static String testPath = new File("").getAbsolutePath();
	private static DefaultTidalStore sequenceStore;
	private static StoreConfig storeConfig;

	private String topic = "LogicDemo";
	private int ploy = 0;
	private int initValue = 21;
	private String className = "tidal.logic.impl.LogicDemo";

	public static void main(String[] args) {
		storeConfig = new StoreConfig();
		storeConfig.setStorePath(testPath);
		deleteDir(new File(storeConfig.getStoreRootDir()));
	}

	@BeforeClass
	public static void init() throws Exception {
		storeConfig = new StoreConfig();
		storeConfig.setStorePath(testPath);
		storeConfig.setCluster(false);
		sequenceStore = new DefaultTidalStore(storeConfig);
		sequenceStore.start();
	}

	/**
	 * write
	 * @throws Exception
	 */
	@Test
	public void test1() throws Exception {
		sequenceStore.getStoreService().putComponent(topic, ploy, initValue, className);
	}

	/**
	 * read
	 * @throws Exception
	 */
	@Test
	public void test2() throws Exception {
		List<ComponentData> queryAllTopic = sequenceStore.getStoreService().queryAllTopic();
		for (ComponentData componentData : queryAllTopic) {
			assertEquals(topic, componentData.getTopic());
			assertEquals(ploy, componentData.getPloy());
			assertEquals(initValue, componentData.getInitValue());
			assertEquals(className, componentData.getClassName());
		}
		deleteDir(new File(storeConfig.getStoreRootDir()));
	}

	@AfterClass
	public static void destory() {
		sequenceStore.shutdown();
	}

	private static boolean deleteDir(File dir) {
		if (dir.isDirectory()) {
			String[] children = dir.list();
			for (int i = 0; i < children.length; i++) {
				boolean success = deleteDir(new File(dir, children[i]));
				if (!success) {
					return false;
				}
			}
		}
		return dir.delete();
	}

}
