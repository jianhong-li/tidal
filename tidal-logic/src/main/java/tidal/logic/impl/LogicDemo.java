package tidal.logic.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.logic.IncrLogicFactory;
import tidal.logic.common.LoggerName;

public class LogicDemo implements IncrLogicFactory {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private final String solt = "52357";

	public String build(int mySid, int value) {
		StringBuilder b = new StringBuilder();
		b.append(mySid);
		b.append(value);
		b.append(solt);
		String result = b.toString();
		log.info("exec: " + result);
		return result;
	}

}
