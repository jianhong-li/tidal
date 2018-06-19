package tidal.store.test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.Date;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import tidal.store.config.StoreConfig;

@Ignore
public class AssistTest {

	private String base_dir = testPath() + File.separator + ".tidal";
	private String storePathRootDir = base_dir + File.separator + "store" + File.separator + "tidal";
	private RandomAccessFile randomAccessFile;
	private FileChannel channel;
	private MappedByteBuffer map;

	private String key = "LogicDemo";
	private String value = "tidal.logic.impl.LogicDemo";

	private byte[] keyBytes;
	private byte[] valueBytes;

	@Before
	public void init() throws IOException {
		File file = new File(storePathRootDir);
		File fileTemp = new File(file.getParent());
		if (!fileTemp.exists()) {
			fileTemp.mkdirs();
		}
		if (!file.exists()) {
			file.createNewFile();
		}
		this.randomAccessFile = new RandomAccessFile(file, "rw");
		this.channel = randomAccessFile.getChannel();
		this.map = channel.map(MapMode.READ_WRITE, 0, 1024 * 1024 * 10);

		this.keyBytes = key.getBytes(StoreConfig.TIDAL_STORE_DEFAULT_CHARSET);
		this.valueBytes = value.getBytes(StoreConfig.TIDAL_STORE_DEFAULT_CHARSET);
	}

	/**
	 * write
	 * @throws Exception
	 */
	@Test
	public void test_a() throws Exception {
		long headerTime = new Date().getTime();
		map.putLong(headerTime);
		map.putLong(headerTime);
		map.putInt(0);
		map.putInt(1);
		map.putInt(2);
		map.putInt(32);

		long contentTime = new Date().getTime();
		map.putLong(contentTime);
		map.putLong(contentTime);
		map.putInt(0);
		map.putInt(11);
		map.putInt(11);
		map.putInt(keyBytes.length);
		map.putInt(valueBytes.length);

		for (int i = 0; i < keyBytes.length; i++) {
			map.put(keyBytes[i]);
		}

		for (int i = 0; i < valueBytes.length; i++) {
			map.put(valueBytes[i]);
		}
		map.force();
	}

	/**
	 * read
	 * @throws UnsupportedEncodingException
	 */
	@Test
	public void test_b() throws UnsupportedEncodingException {
		// store header
		System.out.println(map.getLong());
		System.out.println(map.getLong());
		System.out.println(map.getInt());
		System.out.println(map.getInt());
		System.out.println(map.getInt());
		System.out.println(map.getInt());
		// store component
		System.out.println(map.getLong());
		System.out.println(map.getLong());
		System.out.println(map.getInt());
		System.out.println(map.getInt());
		System.out.println(map.getInt());
		int keyLenght = map.getInt();
		int valueLenght = map.getInt();
		System.out.println(keyLenght);
		System.out.println(valueLenght);
		byte keys[] = new byte[keyLenght];
		for (int i = 0; i < keyLenght; i++) {
			keys[i] = map.get();
		}

		System.out.println(new String(keys, StoreConfig.TIDAL_STORE_DEFAULT_CHARSET));

		byte values[] = new byte[valueLenght];
		for (int i = 0; i < valueLenght; i++) {
			values[i] = map.get();
		}

		System.out.println(new String(values, StoreConfig.TIDAL_STORE_DEFAULT_CHARSET));

	}

	@After
	public void close() throws IOException {
		if (this.channel != null) {
			this.channel.close();
		}
		if (this.randomAccessFile != null) {
			this.randomAccessFile.close();
		}
	}

	private String testPath() {
		return new File("").getAbsolutePath();
	}

}
