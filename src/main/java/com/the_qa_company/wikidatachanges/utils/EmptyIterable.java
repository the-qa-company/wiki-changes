package com.the_qa_company.wikidatachanges.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Iterator;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EmptyIterable<T> implements Iterable<T> {
	public static <T> Iterable<T> empty() {
		return new EmptyIterable<>();
	}

	@Override
	public Iterator<T> iterator() {
		return new EmptyIterator();
	}

	private class EmptyIterator implements Iterator<T> {
		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public T next() {
			return null;
		}
	}
}
