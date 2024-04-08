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
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.base.CoreDatatype;
import org.eclipse.rdf4j.query.GraphQueryResult;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.QueryResults;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.sparql.SPARQLRepository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.sparqlbuilder.core.query.DeleteDataQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.InsertDataQuery;
import org.eclipse.rdf4j.sparqlbuilder.core.query.Queries;
import org.eclipse.rdf4j.sparqlbuilder.graphpattern.GraphPatterns;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

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
		int minLength;
		int maxLength;
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
			minLength = Integer.parseInt(minLengthVal);
		} catch (NumberFormatException e) {
			tool.error("Invalid min length value: " + minLengthVal);
			return;
		}
		try {
			maxLength = Integer.parseInt(maxLengthVal);
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

		Set<Statement> deltaAdd = new HashSet<>();
		Set<Statement> deltaRemove = new HashSet<>();
		while (true) {
			Instant now = Instant.now();
			Thread.sleep(deltaFetch);
			tool.log("Lookup to date '" + inst + "'");
			ChangesIterable<Change> changes = fetcher.getChanges(Date.from(inst), 500, true);

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

			inst = now;
			if (urls.size() < minLength) {
				continue;
			}

			tool.log("begin update with " + urls.size() + " urls");

			record UpdateData(Collection<Statement> toAdd, Collection<Statement> toRemove) {
			}

			List<? extends Future<UpdateData>> futures = urls.stream()
					.map(change -> pool.submit(() -> {
						final String qid = change.getTitle();
						String url = "https://www.wikidata.org/wiki/Special:EntityData/" + qid + ".ttl?flavor=dump";

						byte[] file = fetcher.downloadPage(new URL(url), urlHeader, 10, 500, true);

						Collection<Statement> lstNew = new HashSet<>();
						RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);
						try (GraphQueryResult res = QueryResults.parseGraphBackground(new ByteArrayInputStream(file),  null, rdfParser, null)) {
							while (res.hasNext()) {
								Statement st = res.next();
								if (!st.getPredicate().toString().equals("http://www.w3.org/2004/02/skos/core#prefLabel")) {
									if (((((!st.getPredicate().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") || !st.getObject().toString().equals("http://wikiba.se/ontology#Statement")) && (!st.getPredicate().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") || !st.getObject().toString().equals("http://wikiba.se/ontology#Item"))) && (!st.getPredicate().toString().equals("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") || !st.getObject().toString().equals("http://wikiba.se/ontology#Reference"))) && (!st.getPredicate().toString().equals("http://schema.org/softwareVersion") && !st.getPredicate().toString().equals("http://wikiba.se/ontology#statements") && !st.getPredicate().toString().equals("http://wikiba.se/ontology#sitelinks") && !st.getPredicate().toString().equals("http://wikiba.se/ontology#identifiers") && !st.getPredicate().toString().equals("http://creativecommons.org/ns#license") && !st.getPredicate().toString().equals("http://schema.org/version"))) && !st.getPredicate().toString().equals("http://schema.org/name")) {
										lstNew.add(st);
									}
								}
							}
						}
						Collection<Statement> lstCurr = new HashSet<>();

						try (RepositoryConnection connection = sparqlRepository.getConnection()) {
							// Execute the SPARQL query and get the result directly into the repository
							try (GraphQueryResult evaluate = connection.prepareGraphQuery(QueryLanguage.SPARQL, "CONSTRUCT {?s ?p ?o . ?o ?p2 ?o2 .  ?o2 ?pTime ?oTime . ?o2 ?pQuantity ?oQuantity . ?o2 ?pCoor ?oCoor . ?o prov:wasDerivedFrom ?r . ?r ?p3 ?o3 . ?o3 ?p4 ?o4 .  } WHERE { VALUES ?s { wd:"+qid+" } ?s ?p ?o . OPTIONAL {?o wikibase:rank ?rank . ?o ?p2 ?o2 OPTIONAL {?o2 a wikibase:TimeValue . OPTIONAL {?o2 ?pTime ?oTime } } OPTIONAL {?o2 a wikibase:QuantityValue . OPTIONAL {?o2 ?pQuantity ?oQuantity } } OPTIONAL {?o2 a wikibase:GlobecoordinateValue . OPTIONAL {?o2 ?pCoor ?oCoor } } OPTIONAL { ?o prov:wasDerivedFrom ?r . OPTIONAL { ?r ?p3 ?o3 OPTIONAL {?o3 a wikibase:TimeValue . OPTIONAL {?o3 ?p4 ?o4 } }} } } }").evaluate()) {
								evaluate.forEach(lstCurr::add);
							}

							try (GraphQueryResult res2 = connection.prepareGraphQuery(QueryLanguage.SPARQL, "CONSTRUCT {?wikipedia <http://schema.org/about> wd:" + qid + " . ?wikipedia ?pWikipedia ?oWikipedia . ?oWikipedia <http://wikiba.se/ontology#wikiGroup> ?o2Wikipedia } WHERE { ?wikipedia <http://schema.org/about> wd:" + qid + " . OPTIONAL { ?wikipedia ?pWikipedia ?oWikipedia OPTIONAL {?oWikipedia <http://wikiba.se/ontology#wikiGroup> ?o2Wikipedia } } }").evaluate()) {
								res2.forEach(lstCurr::add);
							}
						}


						computeDelta(lstCurr, lstNew);

						return new UpdateData(lstNew, lstCurr);
					}))
					.toList();

			try {
				for (Future<UpdateData> f : futures) {
					UpdateData updateData = f.get();
					deltaAdd.addAll(updateData.toAdd);
					deltaRemove.addAll(updateData.toRemove);
				}
				urls.clear();

				tool.log("Prepare update with +" + deltaAdd.size() + "/-" + deltaRemove.size() + " element(s)");

				Iterator<Statement> itdel = deltaRemove.iterator();

				while (itdel.hasNext()) {
					int added = 0;

					DeleteDataQuery delete = Queries.DELETE_DATA();

					do {
						Statement stmt = itdel.next();
						delete.deleteData(GraphPatterns.tp(stmt.getSubject(), stmt.getPredicate(), stmt.getObject()));
						added++;
					} while (itdel.hasNext() && added < maxLength);

					String query = delete.getQueryString();

					// send update
					try (RepositoryConnection conn = sparqlRepository.getConnection()) {
						conn.begin();

						conn.prepareUpdate(query).execute();

						conn.commit();
					}
				}
				Iterator<Statement> itadd = deltaAdd.iterator();

				while (itadd.hasNext()) {
					int added = 0;

					InsertDataQuery insert = Queries.INSERT_DATA();

					do {
						Statement stmt = itadd.next();
						insert.insertData(GraphPatterns.tp(stmt.getSubject(), stmt.getPredicate(), stmt.getObject()));
						added++;
					} while (itadd.hasNext() && added < maxLength);

					String query = insert.getQueryString();

					// send update
					try (RepositoryConnection conn = sparqlRepository.getConnection()) {
						conn.begin();

						conn.prepareUpdate(query).execute();

						conn.commit();
					}
				}

				deltaAdd.clear();
				deltaRemove.clear();

				config.set("lastdate", now.toString());
				config.write(Path.of(CONFIG_FILE));
			} catch (ExecutionException e) {
				pool.shutdownNow();
				sparqlRepository.shutDown();
				throw new IOException("Wasn't able to download all the files", e.getCause());
			}

			sparqlRepository.shutDown();
		}

	}

	/**
	 * Compute the delta of 2 statement datasets and put the results inside 2 datasets for add/remove
	 * @param currentStmts current stmts in the system
	 * @param newStmts stmts from the update
	 */
	private static void computeDelta(Collection<Statement> currentStmts, Collection<Statement> newStmts) {

		Iterator<Statement> newMembersIt = newStmts.iterator();
		while (newMembersIt.hasNext()) {
			Statement newMember = newMembersIt.next();

			if (newMember.getSubject() instanceof BNode || newMember.getObject() instanceof BNode) {
				// remove/add
				continue;
			}

			if (!currentStmts.removeIf(testEq(newMember))) {
				newMembersIt.remove();
			}
		}

	}
	public static Predicate<Statement> testEq(Statement other) {
		return stmt -> {
			if (!Objects.equals(stmt.getSubject(), other.getSubject())) return false;
			if (!Objects.equals(stmt.getPredicate(), other.getPredicate())) return false;
			if (Objects.equals(stmt.getObject(), other.getObject())) return true;

			if (stmt.getObject() == null || other.getObject() == null) return false;

			return valueEquivalent(stmt.getObject(), other.getObject());
		};
	}

	public static boolean valueEquivalent(Value v1, Value v2) {
		if (!v1.isLiteral() || !v2.isLiteral()) {
			return false;
		}

		Literal l1 = (Literal) v1;
		Literal l2 = (Literal) v2;

		CoreDatatype dt1 = l1.getCoreDatatype();
		CoreDatatype dt2 = l2.getCoreDatatype();
		if (!dt1.isXSDDatatype() || !dt2.isXSDDatatype()
		    || !dt1.asXSDDatatype().orElseThrow().isNumericDatatype()
		    || !dt1.asXSDDatatype().orElseThrow().isNumericDatatype()
		) {
			return false;
		}

		double d1 = l1.doubleValue();
		double d2 = l2.doubleValue();
		return Math.abs(d1 - d2) < 0.000000001;
	}
}
