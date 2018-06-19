package tidal.logic.test;

import static org.junit.Assert.*;
import org.junit.Test;

import tidal.logic.DateTimeLogicFactory;
import tidal.logic.IncrLogicFactory;
import tidal.logic.impl.LogicDemo;
import tidal.logic.impl.LogicDemo2;

public class DemoTest {

	@Test
	public void testDemo() {
		IncrLogicFactory incrLogicFactory = new LogicDemo();
		String seq = incrLogicFactory.build(0, 12);
		assertEquals("01252357", seq);
	}

	@Test
	public void testDemo2() {
		DateTimeLogicFactory dateTimeLogicFactory = new LogicDemo2();
		String seq = dateTimeLogicFactory.build(0, System.currentTimeMillis(), 12);
		assertNotNull(seq);
	}

}
