package com.the_qa_company.wikidatachanges.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class ApiResult {
	private String batchcomplete;
	@JsonProperty("continue")
	private Continue continueOpt;
	private Query query;
}
