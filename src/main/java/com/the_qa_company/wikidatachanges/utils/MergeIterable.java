package com.the_qa_company.wikidatachanges.utils;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;

import java.util.Iterator;

@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class MergeIterable<T> implements Iterable<T> {
	public static <T> Iterable<T> merge(Iterable<T> it1, Iterable<T> it2) {
		return new MergeIterable<>(it1, it2);
	}
	public static <T> Iterable<T> merge(Iterable<T> it1, Iterable<T> it2, Iterable<T> it3) {
		return new MergeIterable<>(it1, merge(it2, it3));
	}
	public static <T> Iterable<T> merge(Iterable<T> it1, Iterable<T> it2, Iterable<T> it3, Iterable<T> it4) {
		return new MergeIterable<>(it1, merge(it2, it3, it4));
	}
	
	private final Iterable<T> it1;
	private final Iterable<T> it2;
	@Override
	public Iterator<T> iterator() {
		return new MergeIterator(it1.iterator(), it2.iterator());
	}
	
	@RequiredArgsConstructor
	private class MergeIterator implements Iterator<T> {
		private final Iterator<T> it1;
		private final Iterator<T> it2;

		@Override
		public boolean hasNext() {
			return it1.hasNext() || it2.hasNext();
		}

		@Override
		public T next() {
			if (it1.hasNext()) {
				return it1.next();
			} else {
				return it2.next();
			}
		}
	}
}
