package com.the_qa_company.wikidatachanges.api;

import lombok.Data;

import java.util.List;

@Data
public class Query {
	private List<Change> recentchanges;
}
