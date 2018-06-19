package tidal.store;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.store.common.LoggerName;

public class MappedFile {

	protected static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	protected RandomAccessFile raf;

	protected FileChannel fileChannel;

	private File file;

	private MappedByteBuffer mappedByteBuffer;

	public MappedFile() {
	}

	public MappedFile(final String fileName, final int fileSize) throws IOException {
		this.file = new File(fileName);

		ensureDirOK(this.file.getParent());
		init(this.file, fileSize);
	}

	public MappedFile(final File file, final int fileSize) throws IOException {
		this.file = file;

		init(file, fileSize);
	}

	private void init(final File file, final int fileSize) throws IOException {

		boolean ok = false;

		try {
			this.raf = new RandomAccessFile(this.file, "rw");
			this.fileChannel = this.raf.getChannel();
			this.mappedByteBuffer = this.fileChannel.map(MapMode.READ_WRITE, 0, fileSize);
			ok = true;
		} catch (FileNotFoundException e) {
			log.error("create file channel " + this.file.getAbsoluteFile() + " Failed. ", e);
			throw e;
		} catch (IOException e) {
			log.error("map file " + this.file.getAbsoluteFile() + " Failed. ", e);
			throw e;
		} finally {
			if (!ok && this.fileChannel != null) {
				this.fileChannel.close();
			}
		}
	}
	
	public void flush() {
		this.mappedByteBuffer.force();
	}

	public static void ensureDirOK(final String dirName) {
		if (dirName != null) {
			File f = new File(dirName);
			if (!f.exists()) {
				boolean result = f.mkdirs();
				log.info(dirName + " mkdir " + (result ? "OK" : "Failed"));
			}
		}
	}

	public void destroy() {
		if (this.fileChannel != null) {
			try {
				this.fileChannel.close();
			} catch (IOException e) {
				log.error("close fileChannel Exception: ", e.getMessage());
			}
		}
		if (this.raf != null) {
			try {
				this.raf.close();
			} catch (IOException e) {
				log.error("close randomAccessFile Exception: ", e.getMessage());
			}
		}
	}

	public FileChannel getFileChannel() {
		return fileChannel;
	}

	public MappedByteBuffer getMappedByteBuffer() {
		return mappedByteBuffer;
	}
}
