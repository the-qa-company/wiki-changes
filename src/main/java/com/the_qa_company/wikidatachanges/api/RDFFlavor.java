package com.the_qa_company.wikidatachanges.api;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * RDFFlavor to get RDF outputs
 *
 * @author Antoine Willerval
 */
@AllArgsConstructor
@Getter
public enum RDFFlavor {
	/**
	 * Excludes descriptions of entities referred to in the data
	 */
	DUMP("dump", true, "Excludes descriptions of entities referred to in the data."),
	/**
	 * Provides only truthy statements, along with sitelinks and version information.
	 */
	SIMPLE("simple", true, "Provides only truthy statements, along with sitelinks and version information."),
	/**
	 * An argument of "full" returns all data.
	 */
	FULL("full", false, "An argument of \"full\" returns all data.");

	/**
	 * @return the default flavor
	 */
	public static RDFFlavor getDefaultFlavor() {
		return FULL;
	}

	private final String title;
	private final boolean shouldSpecify;
	private final String description;
}
