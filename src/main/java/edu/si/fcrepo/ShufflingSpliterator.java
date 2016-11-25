package edu.si.fcrepo;

import java.util.Iterator;
import java.util.Spliterator;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * A {@link Spliterator} fed by an {@link Iterator} that uses a lock to shuffle between splits.
 * 
 * @author ajs6f
 *
 * @param <T> the type of element returned by this
 */
public class ShufflingSpliterator<T> implements Spliterator<T> {

	private final Iterator<T> src;

	private final Lock lock = new ReentrantLock();

	private final long originalEstSize;

	/**
	 * The estimated sizes of all splits (including this) as per {@link #estimateSize()}, which, because we use a lock to
	 * shuffle amongst splits, are always equal.
	 */
	private long estSize;

	/**
	 * The number of spliterators already split off via {@link #trySplit()} plus 1 for this itself.
	 */
	private volatile int splits = 1;

	private final int characteristics;

	protected ShufflingSpliterator(Iterator<T> src) {
		this(src, 0);
	}

	protected ShufflingSpliterator(Iterator<T> src, int characteristics) {
		this(src, Long.MAX_VALUE, characteristics);
	}

	protected ShufflingSpliterator(Iterator<T> src, long est, int characteristics) {
		this.src = src;
		this.estSize = this.originalEstSize = est;
		this.characteristics = characteristics | CONCURRENT;
	}

	@Override
	public boolean tryAdvance(Consumer<? super T> action) {
		lock.lock();
		try {
			boolean hasNext = src.hasNext();
			if (hasNext) action.accept(src.next());
			return hasNext;
		} finally {
			lock.unlock();
		}
	}

	@Override
	public Spliterator<T> trySplit() {
		lock.lock();
		try {
			splits++;
			//TODO what if there is a split from the parent? then this calculation will be wrong
			if (originalEstSize != Long.MAX_VALUE) estSize = originalEstSize / splits;
			return new ShufflingSpliterator<>(src, estSize, characteristics());
		} finally {
			lock.unlock();
		}
	}

	@Override
	public long estimateSize() {
		return estSize;
	}

	public void forEachRemaining(Consumer<? super T> action) {
		lock.lock();
		try {
			src.forEachRemaining(action);
		} finally {
			lock.unlock();
		}
	}

	@Override
	public int characteristics() {
		return characteristics;
	}
}
