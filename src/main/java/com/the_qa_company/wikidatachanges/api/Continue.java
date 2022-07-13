package com.the_qa_company.wikidatachanges.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.Date;
import java.util.GregorianCalendar;

@Data
public class Continue {
	private String rccontinue;
	@JsonProperty("continue")
	private String continueOpt;

	public Date getRcContinueDate() {
		// 2022 07 05 08 35 02

		int year = Integer.parseInt(rccontinue, 0, 4, 10);
		int month = Integer.parseInt(rccontinue, 4, 6, 10) - 1;
		int day = Integer.parseInt(rccontinue, 6, 8, 10);

		int hour = Integer.parseInt(rccontinue, 8, 10, 10);
		int minute = Integer.parseInt(rccontinue, 10, 12, 10);
		int second = Integer.parseInt(rccontinue, 12, 14, 10);

		assert month >= 0 && month < 12;

		return new GregorianCalendar(year, month, day, hour, minute, second).getTime();
	}

}
