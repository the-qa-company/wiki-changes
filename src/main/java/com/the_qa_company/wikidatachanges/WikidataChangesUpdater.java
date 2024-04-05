package com.the_qa_company.wikidatachanges;

import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.wikidatachanges.api.Change;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class WikidataChangesUpdater {
	private static final String CONFIG_FILE = "changes.cfg";
	public static void main(String[] args) throws ParseException, IOException, InterruptedException {
		final String DEFAULT_LOCAL_SPARQL_SERVER = "https://www.wikidata.org/wiki/Special:EntityData/";
		final String DEFAULT_REMOTE_SERVER_UPDATER = "https://www.wikidata.org/w/api.php";
		final String DEFAULT_REMOTE_SERVER = "http://query.wikidata.org/sparql";
		Option sparqlOpt = new Option("s", "sparql", true, "local sparql server, default: " + DEFAULT_LOCAL_SPARQL_SERVER);
		Option updaterOpt = new Option("u", "updater", true, "remote wiki update server, default: " + DEFAULT_REMOTE_SERVER_UPDATER);
		Option serverOpt = new Option("S", "server", true, "remote wiki server, default: " + DEFAULT_REMOTE_SERVER);
		Option dateOpt = new Option("d", "date", true, "Last date to lookup");
		Option minLengthOpt = new Option("l", "minlength", true, "Min length of the update");
		Option maxLengthOpt = new Option("L", "maxlength", true, "Max length of the update");
		Option deltaFetchOpt = new Option("D", "delta", true, "Time delta between the fetch after lookup (ms), default: 10000");

		Option todayOpt = new Option("T", "today", false, "Print date");
		Option colorOpt = new Option("c", "color", false, "Color output");
		Option helpOpt = new Option("h", "help", false, "Print help");

		Options opt = new Options()
				.addOption(sparqlOpt)
				.addOption(serverOpt)
				.addOption(minLengthOpt)
				.addOption(maxLengthOpt)
				.addOption(dateOpt)
				.addOption(deltaFetchOpt)
				.addOption(todayOpt)
				.addOption(updaterOpt)
				.addOption(colorOpt)
				.addOption(helpOpt);

		CommandLineParser parser = new DefaultParser();
		CommandLine cl = parser.parse(opt, args);

		if (cl.hasOption(helpOpt)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("wiki-changes-updater", opt, true);
			return;
		}

		ColorTool tool = new ColorTool(cl.hasOption(colorOpt));

		if (cl.hasOption(todayOpt)) {
			tool.log(Instant.now().toString());
			return;
		}

		String localSparql = cl.getOptionValue(sparqlOpt, DEFAULT_LOCAL_SPARQL_SERVER);
		String remoteServer = cl.getOptionValue(serverOpt, DEFAULT_REMOTE_SERVER);
		String remoteServerUpdater = cl.getOptionValue(updaterOpt, DEFAULT_REMOTE_SERVER_UPDATER);
		long deltaFetch;
		long minLength;
		long maxLength;
		String deltaFetchVal = cl.getOptionValue(deltaFetchOpt, "10000");
		String minLengthVal = cl.getOptionValue(deltaFetchOpt, "100");
		String maxLengthVal = cl.getOptionValue(deltaFetchOpt, "1000");

		try {
			deltaFetch = Long.parseLong(deltaFetchVal);
		} catch (NumberFormatException e) {
			tool.error("Invalid delta fetch value: " + deltaFetchVal);
			return;
		}
		try {
			minLength = Long.parseLong(minLengthVal);
		} catch (NumberFormatException e) {
			tool.error("Invalid min length value: " + minLengthVal);
			return;
		}
		try {
			maxLength = Long.parseLong(maxLengthVal);
		} catch (NumberFormatException e) {
			tool.error("Invalid max length value: " + maxLengthVal);
			return;
		}

		tool.log("Starting updater...");
		tool.log("Local ... " + localSparql);
		tool.log("Updater . " + remoteServerUpdater);
		tool.log("Remote .. " + remoteServer);

		HDTOptions config = HDTOptions.of();

		try {
			config = HDTOptions.readFromFile(CONFIG_FILE);
		} catch (FileNotFoundException ignored) {
		}

		String lastdate = cl.getOptionValue(dateOpt, config.get("lastdate", ""));

		if (lastdate == null || lastdate.isEmpty()) {
			tool.error("Missing date to lookup, you need to use --" + dateOpt.getLongOpt() + " [date] to set it");
			return;
		}

		config.set("lastdate", lastdate);

		Instant inst = Instant.parse(lastdate);

		// lookup to the last date

		FetcherOptions opts = FetcherOptions.builder()
				.url(remoteServerUpdater)
				.build();

		WikidataChangesFetcher fetcher = new WikidataChangesFetcher(opts);


		Set<Change> urls = new HashSet<>();
		ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

		AtomicLong downloads = new AtomicLong();
		Map<String, String> urlHeader = Map.of(
				"user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
				"accept", "text/turtle",
				"accept-encoding", "gzip, deflate",
				"accept-language", "en-US,en;q=0.9",
				"upgrade-insecure-requests", "1",
				"scheme", "https"
		);

		while (true) {
			tool.log("Lookup to date '" + inst + "'");
			ChangesIterable<Change> changes = fetcher.getChanges(Date.from(inst), 500, true);

			urls.clear();
			downloads.set(0);

			Repository sparqlRepository = new SPARQLRepository(localSparql);
			sparqlRepository.init();

			Object sparqlSync = new Object() {
			};

			long i = 0;
			for (Change change : changes) {
				if (!change.getTitle().isEmpty() && change.getNs() == 0) {
					long d = ++i;
					urls.add(change);
					WikidataChangesFetcher.printPercentage(d, changes.size(), "fetch: " + urls.size(), true);
				}

			}
			tool.log("urls: " + urls.size());

			List<? extends Future<Collection<Statement>>> futures = urls.stream()
					.map(change -> pool.submit(() -> {
						String url = "https://www.wikidata.org/wiki/Special:EntityData/" + change.getTitle() + ".ttl?flavor=dump";

						byte[] file = fetcher.downloadPage(new URL(url), urlHeader, 10, 500, true);

						Collection<Statement> lst = new HashSet<>();
						RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
						try (GraphQueryResult res = QueryResults.parseGraphBackground(new ByteArrayInputStream(file),  null, rdfParser, null)) {
							while (res.hasNext()) {
								Statement st = res.next();
								if (!st.getPredicate().toString().equals("http://www.w3.org/2004/02/skos/core#prefLabel")) {
									if (((((!st.getPredicate().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") || !st.getObject().toString().equals("http://wikiba.se/ontology#Statement")) && (!st.getPredicate().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") || !st.getObject().toString().equals("http://wikiba.se/ontology#Item"))) && (!st.getPredicate().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") || !st.getObject().toString().equals("http://wikiba.se/ontology#Reference"))) && (!st.getPredicate().toString().equals("http://schema.org/softwareVersion") && !st.getPredicate().toString().equals("http://wikiba.se/ontology#statements") && !st.getPredicate().toString().equals("http://wikiba.se/ontology#sitelinks") && !st.getPredicate().toString().equals("http://wikiba.se/ontology#identifiers") && !st.getPredicate().toString().equals("http://creativecommons.org/ns#license") && !st.getPredicate().toString().equals("http://schema.org/version"))) && !st.getPredicate().toString().equals("http://schema.org/name")) {
										lst.add(st);
									}
								}
							}
						}




						return lst;
					}))
					.toList();

			try {
				Set<Statement> delta = new HashSet<>();
				for (Future<Collection<Statement>> f : futures) {
					delta.addAll(f.get());
				}

				tool.log("Prepare update with " + delta.size() + " element(s)");

			} catch (ExecutionException e) {
				pool.shutdownNow();
				sparqlRepository.shutDown();
				long d = downloads.get();
				int percentage = (int) (100L * d / urls.size());
				throw new IOException("Wasn't able to download all the files: " + d + "/" + urls.size() + " " + percentage + "%", e.getCause());
			}

			sparqlRepository.shutDown();

			Thread.sleep(deltaFetch);
		}

	}
}
