package com.the_qa_company.wikidatachanges;

import java.util.Iterator;

public record ChangesIterable<T>(Iterable<T> iterable, long size) implements Iterable<T> {

	@Override
	public Iterator<T> iterator() {
		return iterable().iterator();
	}
}
