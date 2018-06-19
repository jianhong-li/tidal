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
import tidal.store.logic.LogicImpl;

public class StoreService {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private final PhysicalFile physicalFile;
	private StoreHeader storeHeader;
	private StoreComponent storeComponent;
	private LogicImpl logicImpl;

	private Lock lock = new ReentrantLock();

	public StoreService(final DefaultTidalStore defaultTidalStore) throws IOException {
		this.physicalFile = new PhysicalFile(defaultTidalStore.getStoreConfig());
	}

	public List<ComponentData> queryAllTopic() {
		ConcurrentMap<String, Component> componentMap = storeComponent.getComponentMap();
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
			if (storeComponent.isExistTopic(topic)) {
				return ResponseCode.TOPIC_EXISTED;
			}
			if (!storeComponent.isExistPloyType(ploy)) {
				return ResponseCode.PLOY_NOT_SUPPORTED;
			}
			if (!storeComponent.checkInitValue(initValue)) {
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

			this.storeComponent.write(component);
			this.storeComponent.addMap(component);
			this.logicImpl.add(component);
			this.storeHeader.setLastComponentOffset(position);
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
		if (!storeComponent.componentMapContainsKey(topic)) {
			log.warn("invalid topic: " + topic);
			return emptyString();
		}

		Component component = storeComponent.getComponentMap().get(topic);
		component.getLock().lock();
		try {

			if (storeComponent.isReset(component)) {
				this.storeComponent.reset(component);
			}
			component.setUpdateTime(new Date().getTime());
			component.incrCurrentValue();

			this.storeComponent.update(component);
			this.physicalFile.save();
			log.debug(this.getServiceName() + " ||^ update topic: " + topic + ", currentValue: "
					+ component.getCurrentValue());

			if (component.getPloy() == StoreComponent.INCREMENT_FIELD) {
				return logicImpl.invoke(logicImpl.getLogicObject(topic), storeHeader.getMySid(),
						component.getCurrentValue());
			} else {
				return logicImpl.invoke(logicImpl.getLogicObject(topic), storeHeader.getMySid(),
						component.getUpdateTime(), component.getCurrentValue());
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
			physicalFile.load();
			log.info(this.getServiceName() + " load tidal store file end. <<--");

			this.storeHeader = physicalFile.getStoreHeader();
			this.storeComponent = physicalFile.getStoreComponent();
			this.logicImpl = physicalFile.getLogicImpl();

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
		this.storeHeader.setLastComponentOffset(lastComponentOffset);
	}

	public void setMySid(int mySid) {
		this.storeHeader.setMySid(mySid);
	}

	public int getMySid() {
		return storeHeader.getMySid();
	}

	public void setBrotherSid(int brotherSid) {
		this.storeHeader.setBrotherSid(brotherSid);
	}

	public int getBrotherSid() {
		return storeHeader.getBrotherSid();
	}

	public int getLastComponentOffset() {
		return storeHeader.getLastComponentOffset();
	}

	public void setLastOffset(int remotingLength) {
		storeComponent.setLastOffset(remotingLength);
	}

	public int getLastOffset() {
		return storeComponent.getLastOffset();
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
