package tidal.common;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.common.constant.LoggerName;

public abstract class ServiceThread implements Runnable {
	private static final Logger STLOG = LoggerFactory
			.getLogger(LoggerName.COMMON_LOGGER_NAME);
	private static final long JOIN_TIME = 90 * 1000;

	protected final Thread thread;
	protected final CountDownLatch2 waitPoint = new CountDownLatch2(1);
	protected volatile AtomicBoolean hasNotified = new AtomicBoolean(false);
	protected volatile boolean stopped = false;

	public ServiceThread() {
		this.thread = new Thread(this, this.getServiceName());
	}

	public abstract String getServiceName();

	public void start() {
		this.thread.start();
	}

	public void shutdown() {
		this.shutdown(false);
	}

	public void shutdown(final boolean interrupt) {
		this.stopped = true;
		STLOG.info("shutdown thread " + this.getServiceName() + " interrupt "
				+ interrupt);

		if (hasNotified.compareAndSet(false, true)) {
			waitPoint.countDown(); // notify
		}

		try {
			if (interrupt) {
				this.thread.interrupt();
			}

			long beginTime = System.currentTimeMillis();
			if (!this.thread.isDaemon()) {
				this.thread.join(this.getJointime());
			}
			long eclipseTime = System.currentTimeMillis() - beginTime;
			STLOG.info("join thread " + this.getServiceName()
					+ " eclipse time(ms) " + eclipseTime + " "
					+ this.getJointime());
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public long getJointime() {
		return JOIN_TIME;
	}

	public void stop() {
		this.stop(false);
	}

	public void stop(final boolean interrupt) {
		this.stopped = true;
		STLOG.info("stop thread " + this.getServiceName() + " interrupt "
				+ interrupt);

		if (hasNotified.compareAndSet(false, true)) {
			waitPoint.countDown(); // notify
		}

		if (interrupt) {
			this.thread.interrupt();
		}
	}

	public void makeStop() {
		this.stopped = true;
		STLOG.info("makestop thread " + this.getServiceName());
	}

	public void wakeup() {
		if (hasNotified.compareAndSet(false, true)) {
			waitPoint.countDown(); // notify
		}
	}

	protected void waitForRunning(long interval) {
		if (hasNotified.compareAndSet(true, false)) {
			this.onWaitEnd();
			return;
		}

		// entry to wait
		waitPoint.reset();

		try {
			waitPoint.await(interval, TimeUnit.MILLISECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			hasNotified.set(false);
			this.onWaitEnd();
		}
	}

	protected void onWaitEnd() {
	}

	public boolean isStopped() {
		return stopped;
	}
}
