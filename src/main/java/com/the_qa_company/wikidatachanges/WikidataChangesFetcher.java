package com.the_qa_company.wikidatachanges;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.the_qa_company.wikidatachanges.api.ApiResult;
import com.the_qa_company.wikidatachanges.api.Change;
import com.the_qa_company.wikidatachanges.utils.EmptyIterable;
import com.the_qa_company.wikidatachanges.utils.MergeIterable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

@RequiredArgsConstructor
public class WikidataChangesFetcher {
	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException, ParseException {
		Option cacheOpt = new Option("-c", "--cache", true, "cache location");
		Option elementsOpt = new Option("-e", "--element", true, "element to ask to the wiki API");
		Option wikiapiOpt = new Option("-w", "--wikiapi", true, "Wiki api location");
		Option dateOpt = new Option("-d", "--date", true, "Wiki api location");
		dateOpt.setRequired(true);
		Options opt = new Options()
				.addOption(cacheOpt)
				.addOption(elementsOpt)
				.addOption(wikiapiOpt)
				.addOption(dateOpt);

		CommandLineParser parser = new DefaultParser();
		CommandLine cl = parser.parse(opt, args);

		Path outputDirectory = Path.of(cl.getOptionValue(cacheOpt, "cache"));
		int elementPerRead = Integer.parseInt(cl.getOptionValue(elementsOpt, "500"));
		String wikiapi = cl.getOptionValue(wikiapiOpt, "https://www.wikidata.org/w/api.php");
		Date date = Date.from(Instant.parse(cl.getOptionValue(dateOpt)));

		if (elementPerRead <= 0) {
			throw new IllegalArgumentException("elementPerRead can't be negative or zero! " + elementPerRead);
		}

		System.out.println("Reading from date: " + date);

		WikidataChangesFetcher fetcher = new WikidataChangesFetcher(FetcherOptions
				.builder()
				.url(wikiapi)
				.build());

		Set<Change> urls = new HashSet<>();

		System.out.print("fetching changes...\r");

		Iterable<Change> changes = fetcher.getChanges(date, elementPerRead, true);

		System.out.println();

		System.out.print("fetching changes: 0\r");
		for (Change change : changes) {
			if (!change.getTitle().isEmpty() && change.getNs() == 0) {
				urls.add(change);
				System.out.print("fetching changes: " + urls.size() + "\r");
			}
		}

		System.out.println();

		ExecutorService pool = Executors.newFixedThreadPool(100);

		Object logSync = new Object() {
		};

		System.out.println("Downloading ttl files...");
		Path sites = outputDirectory.resolve("sites");
		Files.createDirectories(sites);

		AtomicLong downloads = new AtomicLong();
		List<? extends Future<Path>> futures = urls.stream()
				.map(change -> pool.submit(() -> {
					Path path = sites.resolve(change.getTitle() + ".ttl");
					fetcher.downloadPageToFile(new URL("https://www.wikidata.org/wiki/Special:EntityData/" + change.getTitle() + ".ttl"), path);
					long d = downloads.incrementAndGet();
					int percentage = (int) (100L * d / urls.size());

					synchronized (logSync) {
						System.out.print(
								"[" +
										"#".repeat(percentage * 20 / 100) + " ".repeat(20 - percentage * 20 / 100)
										+ "] (" + d + " / " + urls.size() + " - " + percentage + "%)       \r"
						);
					}
					return path;
				}))
				.toList();

		try {
			for (Future<Path> f : futures) {
				f.get();
			}
		} catch (ExecutionException e) {
			pool.shutdownNow();
			throw e;
		}

		System.out.println();

		pool.shutdown();

		System.out.println("done.");
	}

	private final ObjectMapper mapper = new ObjectMapper();
	@Getter
	private final FetcherOptions options;

	private void downloadPageToFile(URL uri, Path output) throws IOException {
		try {
			IOUtils.copy(uri, output.toFile());
		} catch (java.io.FileNotFoundException e) {
			// ignore
		}
	}

	/**
	 * call the wikidata changes api
	 *
	 * @param elementPerRead number of elements to query to the wiki api
	 * @return result
	 * @throws IOException api call fail
	 */
	public ApiResult changesApiCall(long elementPerRead) throws IOException {
		return changesApiCall(null, elementPerRead);
	}

	/**
	 * call the wikidata changes api
	 *
	 * @param rcchange the rcchange id for restart
	 * @param elementPerRead number of elements to query to the wiki api
	 * @return result
	 * @throws IOException api call fail
	 */
	public ApiResult changesApiCall(String rcchange, long elementPerRead) throws IOException {
		String urlLink = options.getUrl() + "?format=json&rcprop=title&list=recentchanges&action=query&rclimit=" + elementPerRead;
		if (rcchange != null) {
			urlLink += "&rccontinue=" + rcchange;
		}
		URL url = new URL(urlLink);

		return mapper.readValue(url, ApiResult.class);
	}

	public Iterable<Change> getChanges(Date end, long elementPerRead, boolean log) throws IOException {
		Iterable<Change> it = EmptyIterable.empty();
		long count = 0;

		String lastRc = null;
		while (true) {
			// fetch changes
			ApiResult apiResult = changesApiCall(lastRc, elementPerRead);
			// add result to the iterable
			List<Change> recentchanges = apiResult.getQuery().getRecentchanges();
			count += recentchanges.size();
			it = MergeIterable.merge(it, recentchanges);

			lastRc = apiResult.getContinueOpt().getRccontinue();
			Date date = apiResult.getContinueOpt().getRcContinueDate();

			// end date
			if (log) {
				System.out.print("rollback to " + date + " " + count + " elements.   \r");
			}
			if (date.before(end)) {
				break;
			}
		}

		return it;
	}
}
