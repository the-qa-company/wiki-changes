package com.the_qa_company.wikidatachanges.api;

import lombok.Data;

@Data
public class Change {
	private String type;
	private long ns;
	private String title;
}
