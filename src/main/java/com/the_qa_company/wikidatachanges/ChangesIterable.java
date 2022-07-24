package com.the_qa_company.wikidatachanges;

import java.util.Iterator;
import java.util.List;

public record ChangesIterable<T>(List<T> iterable, long size) implements Iterable<T> {

	@Override
	public Iterator<T> iterator() {
		return iterable().iterator();
	}
}
