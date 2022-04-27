package com.quinn.pillbox.feature;

/**
 * 依據節點的事件觸發, <br>
 * 同一節點同一時間只具備一種身份, <br>
 * 所以只會呼叫其中一個 method
 * @author pigmilk
 * @date Apr 27, 2022 1:12:08 PM
 */
public interface OnElectionCallback {
	
	void onElectedToBeLeader();

	void onWorker();
}
