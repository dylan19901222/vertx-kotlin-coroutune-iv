package kotlin.frequencycap;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

import io.vertx.core.shareddata.Shareable;

public class UserAdLogHolder<K, V> implements Shareable, Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3262993345897784073L;

	private ConcurrentHashMap<K, V> map;

	public UserAdLogHolder() {
		if (this.map == null) {
			this.map = new ConcurrentHashMap<>();
		}
	}

	public UserAdLogHolder(ConcurrentHashMap<K, V> map) {
		this.map = map;
	}

	@Override
	public String toString() {
		return "UserAdLogHolder [map=" + map + "]";
	}

	public synchronized void put(K key, V value) {
		this.map.put(key, value);
	}

	public synchronized V get(K key) {
		return this.map.get(key);
	}

	public synchronized boolean containsKey(K key) {
		return this.map.containsKey(key);
	}

	public synchronized void setMap(ConcurrentHashMap<K, V> map) {
		this.map = map;
	}

	public synchronized void remove(K key) {
		this.map.remove(key);
	}

	public synchronized Integer size() {
		return this.map.size();
	}

}
