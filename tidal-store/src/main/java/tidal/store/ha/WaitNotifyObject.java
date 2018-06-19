package tidal.store.ha;

import java.util.HashMap;

public class WaitNotifyObject {

	protected final HashMap<Long/* thread id */, Boolean/* notified */> waitingThreadTable = new HashMap<Long, Boolean>(
			16);

	protected volatile boolean hasNotified = false;

	public void wakeup() {
		synchronized (this) {
			if (!this.hasNotified) {
				this.hasNotified = true;
				this.notify();
			}
		}
	}

	protected void waitForRunning(long interval) {
		synchronized (this) {
			if (this.hasNotified) {
				this.hasNotified = false;
				this.onWaitEnd();
				return;
			}

			try {
				this.wait(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				this.hasNotified = false;
				this.onWaitEnd();
			}
		}
	}

	protected void onWaitEnd() {
	}

	public void wakeupAll() {
		synchronized (this) {
			boolean needNotify = false;

			for (Long key : this.waitingThreadTable.keySet()) {
				needNotify = needNotify || !waitingThreadTable.get(key);
				waitingThreadTable.put(key, true);
			}

			if (needNotify) {
				this.notifyAll();
			}
		}
	}

	public void allWaitForRunning(long interval) {
		long currentThreadId = Thread.currentThread().getId();
		synchronized (this) {
			Boolean notified = this.waitingThreadTable.get(currentThreadId);
			if (notified != null && notified) {
				this.waitingThreadTable.put(currentThreadId, false);
				this.onWaitEnd();
				return;
			}

			try {
				this.wait(interval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			} finally {
				this.waitingThreadTable.put(currentThreadId, false);
				this.onWaitEnd();
			}
		}
	}
}
