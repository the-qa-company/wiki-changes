package com.the_qa_company.wikidatachanges;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class FetcherOptions {
	private String url;
}
