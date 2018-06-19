package tidal.store.logic;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.logic.LogicFactory;
import tidal.store.common.LoggerName;
import tidal.store.impl.Component;

public class LogicImpl {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private final ConcurrentMap<String, LogicObject<LogicFactory>> logicObjectMap = new ConcurrentHashMap<String, LogicObject<LogicFactory>>();

	public void add(Component component) {
		LogicObject<LogicFactory> lo = new LogicObject<LogicFactory>();

		Class<LogicFactory> findClass = LogicClassLoader.findClass(component.getClassName());
		lo.setT(LogicClassLoader.newInstance(findClass));
		lo.setTopic(component.getTopic());
		lo.setCls(findClass);

		logicObjectMap.put(component.getTopic(), lo);

		log.info("new LogicFactory instance success. topic: " + component.getTopic());
	}

	public String invoke(LogicObject<LogicFactory> lo, int mySid, int value) {
		return LogicClassLoader.invoke(lo.getCls(), lo.getT(), mySid, value);
	}

	public String invoke(LogicObject<LogicFactory> lo, int mySid, long time, int value) {
		return LogicClassLoader.invoke(lo.getCls(), lo.getT(), mySid, time, value);
	}

	public LogicObject<LogicFactory> getLogicObject(String reqId) {
		return logicObjectMap.get(reqId);
	}

	public void clean() {
		logicObjectMap.clear();
	}

}
