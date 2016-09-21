package com.bumblebee.app;

import org.apache.cassandra.db.DecoratedKey;
/**
 * to store info of key along with the data size
 * 
 * @author barala
 *
 */
public class KeyAttributes {
	private DecoratedKey decoratedKey;
	private long dataSize;
	
	public KeyAttributes(DecoratedKey decoratedKey,long dataSize) {
		this.decoratedKey = decoratedKey;
		this.dataSize = dataSize;
	}

	public DecoratedKey getDecoratedKey() {
		return decoratedKey;
	}

	public void setDecoratedKey(DecoratedKey decoratedKey) {
		this.decoratedKey = decoratedKey;
	}

	public long getDataSize() {
		return dataSize;
	}

	public void setDataSize(long dataSize) {
		this.dataSize = dataSize;
	}
}
