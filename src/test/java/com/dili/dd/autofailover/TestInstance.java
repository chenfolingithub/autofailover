package com.dili.dd.autofailover;

public class TestInstance implements Instance {

	boolean isStart;

	@Override
	public void start() {
		isStart = true;
	}

	@Override
	public void stop() {
		isStart = false;
	}

	@Override
	public boolean isStart() {
		return isStart;
	}

}
