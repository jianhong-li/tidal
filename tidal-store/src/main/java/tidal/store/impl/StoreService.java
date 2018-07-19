package tidal.store.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.common.protocol.ResponseCode;
import tidal.common.protocol.body.ComponentData;
import tidal.store.DefaultTidalStore;
import tidal.store.common.LoggerName;
import tidal.store.config.StoreConfig;
import tidal.store.logic.LogicClassLoader;

public class StoreService {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private final PhysicalFile physicalFile;

	private Lock lock = new ReentrantLock();

	public StoreService(final DefaultTidalStore defaultTidalStore) throws IOException {
		this.physicalFile = new PhysicalFile(defaultTidalStore.getStoreConfig());
	}

	public List<ComponentData> queryAllTopic() {
		ConcurrentMap<String, Component> componentMap = physicalFile.getStoreComponent().getComponentMap();
		List<ComponentData> getAll = new ArrayList<ComponentData>(componentMap.size());
		for (String topic : componentMap.keySet()) {
			Component component = componentMap.get(topic);

			ComponentData componentData = new ComponentData();
			componentData.setTopic(component.getTopic());
			componentData.setPloy(component.getPloy());
			componentData.setInitValue(component.getInitValue());
			componentData.setClassName(component.getClassName());

			getAll.add(componentData);
		}
		return getAll;
	}

	public int putComponent(String topic, int ploy, int initValue, String className) {
		lock.lock();
		int position = physicalFile.getMappedByteBuffer().position();
		try {
			int topicLength = topic.getBytes(StoreConfig.TIDAL_STORE_DEFAULT_CHARSET).length;
			int valueLength = className.getBytes(StoreConfig.TIDAL_STORE_DEFAULT_CHARSET).length;

			if (physicalFile.isFull(Component.COMPONENT_HEADER_SIZE + topicLength + valueLength)) {
				return ResponseCode.FLUSH_DISK_FULL;
			}
			if (physicalFile.getStoreComponent().isExistTopic(topic)) {
				return ResponseCode.TOPIC_EXISTED;
			}
			if (!physicalFile.getStoreComponent().isExistPloyType(ploy)) {
				return ResponseCode.PLOY_NOT_SUPPORTED;
			}
			if (!physicalFile.getStoreComponent().checkInitValue(initValue)) {
				return ResponseCode.INIT_VALUE_INVALID;
			}
			if (!LogicClassLoader.isPresent(className)) {
				return ResponseCode.CLASS_NOT_FOUND;
			}
			if (!LogicClassLoader.checkPloyMapping(ploy, className)) {
				return ResponseCode.PLOY_CLASS_MAPPING_ERROR;
			}

			long currentTime = new Date().getTime();

			Component component = new Component(position);
			component.setCreateTime(currentTime);
			component.setUpdateTime(currentTime);
			component.setPloy(ploy);
			component.setInitValue(initValue);
			component.setCurrentValue(initValue);
			component.setTopicLength(topicLength);
			component.setClassNameLength(valueLength);
			component.setTopic(topic);
			component.setClassName(className);

			this.physicalFile.getStoreComponent().write(component);
			this.physicalFile.getStoreComponent().addMap(component);
			this.physicalFile.getLogicImpl().add(component);
			this.physicalFile.getStoreHeader().setLastComponentOffset(position);
			this.physicalFile.save();

			log.info(this.getServiceName() + " ++> add topic: " + topic + ", ploy: " + ploy + ", initValue: "
					+ initValue + ", className: " + className + " success.");
			return ResponseCode.SUCCESS;
		} catch (Exception e) {
			e.printStackTrace();
			return ResponseCode.SYSTEM_ERROR;
		} finally {
			lock.unlock();
		}

	}

	public String updateComponent(String topic) {
		if (!physicalFile.getStoreComponent().componentMapContainsKey(topic)) {
			log.warn("invalid topic: " + topic);
			return emptyString();
		}

		Component component = physicalFile.getStoreComponent().getComponentMap().get(topic);
		component.getLock().lock();
		try {

			if (physicalFile.getStoreComponent().isReset(component)) {
				this.physicalFile.getStoreComponent().reset(component);
			}
			component.setUpdateTime(new Date().getTime());
			component.incrCurrentValue();

			this.physicalFile.getStoreComponent().update(component);
			this.physicalFile.save();
			log.debug(this.getServiceName() + " ||^ update topic: " + topic + ", currentValue: "
					+ component.getCurrentValue());

			if (component.getPloy() == StoreComponent.INCREMENT_FIELD) {
				return physicalFile.getLogicImpl().invoke(physicalFile.getLogicImpl().getLogicObject(topic),
						physicalFile.getStoreHeader().getMySid(), component.getCurrentValue());
			} else {
				return physicalFile.getLogicImpl().invoke(physicalFile.getLogicImpl().getLogicObject(topic),
						physicalFile.getStoreHeader().getMySid(), component.getUpdateTime(),
						component.getCurrentValue());
			}
		} catch (Exception e) {
			log.error("update sequence error: ", e.getMessage());
		} finally {
			component.getLock().unlock();
		}
		return emptyString();
	}

	public void start() throws Exception {
		try {
			log.info(this.getServiceName() + " load tidal store file start. -->>");
			this.physicalFile.load();
			log.info(this.getServiceName() + " load tidal store file end. <<--");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public String emptyString() {
		return "";
	}

	public final long getBeginTimeInLock() {
		return 0L;
	}

	public void setLastComponentOffset(int lastComponentOffset) {
		this.physicalFile.getStoreHeader().setLastComponentOffset(lastComponentOffset);
	}

	public void setMySid(int mySid) {
		this.physicalFile.getStoreHeader().setMySid(mySid);
	}

	public int getMySid() {
		return physicalFile.getStoreHeader().getMySid();
	}

	public void setBrotherSid(int brotherSid) {
		this.physicalFile.getStoreHeader().setBrotherSid(brotherSid);
	}

	public int getBrotherSid() {
		return physicalFile.getStoreHeader().getBrotherSid();
	}

	public int getLastComponentOffset() {
		return physicalFile.getStoreHeader().getLastComponentOffset();
	}

	public void setLastOffset(int remotingLength) {
		physicalFile.getStoreComponent().setLastOffset(remotingLength);
	}

	public int getLastOffset() {
		return physicalFile.getStoreComponent().getLastOffset();
	}

	public MappedByteBuffer getMappedByteBuffer() {
		return physicalFile.getMappedByteBuffer();
	}

	public ByteBuffer getHaSliceByteBuffer() {
		return physicalFile.getHaSliceByteBuffer();
	}

	public void syncOffset() {
		physicalFile.load();
	}

	public void shutdown() {
		this.physicalFile.destroy();
	}

	public String getServiceName() {
		return StoreService.class.getSimpleName();
	}

}
