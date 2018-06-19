package tidal.logic.impl;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tidal.logic.DateTimeLogicFactory;
import tidal.logic.common.LoggerName;

public class LogicDemo2 implements DateTimeLogicFactory {

	private static final Logger log = LoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

	private static final DateTimeFormatter format = DateTimeFormatter.ofPattern("yyyyMMdd");

	private final ThreadLocalRandom r = ThreadLocalRandom.current();

	public static String time2Date(long time) {
		return Instant.ofEpochMilli(time).atZone(ZoneId.of(ZoneId.systemDefault().getId())).toLocalDateTime()
				.format(format);
	}

	public String build(int mySid, long time, int value) {
		StringBuilder sb = new StringBuilder();
		sb.append(mySid);
		sb.append(time2Date(time));
		sb.append(value);
		sb.append(shuffle(String.valueOf(r.nextInt(1000))));
		log.info("exec: " + sb.toString());
		return sb.toString();
	}

	private String shuffle(String value) {
		if (value.length() == 1) {
			return "00" + value;
		} else if (value.length() == 2) {
			return "0" + value;
		} else {
			return value;
		}
	}

	public String build(int value) {
		return null;
	}

}
