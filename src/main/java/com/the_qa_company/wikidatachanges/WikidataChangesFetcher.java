package com.the_qa_company.wikidatachanges;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.the_qa_company.wikidatachanges.api.ApiResult;
import com.the_qa_company.wikidatachanges.api.Change;
import com.the_qa_company.wikidatachanges.utils.BitmapAccess;
import com.the_qa_company.wikidatachanges.utils.EmptyIterable;
import com.the_qa_company.wikidatachanges.utils.MergeIterable;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.file.PathUtils;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Instant;
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
import java.util.zip.GZIPInputStream;

@RequiredArgsConstructor
public class WikidataChangesFetcher {
	public static void main(String[] args) throws IOException, InterruptedException, ParseException {
		Option cacheOpt = new Option("c", "cache", true, "cache location");
		Option elementsOpt = new Option("e", "element", true, "element to ask to the wiki API");
		Option wikiapiOpt = new Option("w", "wikiapi", true, "Wiki api location");
		Option todayOpt = new Option("T", "today", false, "Print date");
		Option dateOpt = new Option("d", "date", true, "Wiki api location (required)");
		Option noCacheRecomputeOpt = new Option("C", "nonewcache", false, "Don't recreate the cache");
		Option clearCacheOpt = new Option("D", "deletecache", false, "Clear the cache after the HDT build");
		Option maxTryOpt = new Option("m", "maxtry", true, "Number of try with http request, 0 for infinity (default: 5)");
		Option sleepBetweenTryOpt = new Option("s", "sleeptry", true, "Millis to sleep between try , 0 for no sleep (default: 5000)");
		Option noHdtRecomputeOpt = new Option("H", "nonewhdt", false, "Don't recompute the HDT");
		Option hdtLoadOpt = new Option("l", "hdtload", false, "Load the HDT into memory, fast up the process");
		Option hdtSourceOpt = new Option("s", "hdtsource", true, "Hdt source location (required to compute bitmaps and merge hdt)");
		Option helpOpt = new Option("h", "help", false, "Print help");

		Options opt = new Options()
				.addOption(cacheOpt)
				.addOption(elementsOpt)
				.addOption(wikiapiOpt)
				.addOption(todayOpt)
				.addOption(dateOpt)
				.addOption(clearCacheOpt)
				.addOption(noCacheRecomputeOpt)
				.addOption(maxTryOpt)
				.addOption(sleepBetweenTryOpt)
				.addOption(noHdtRecomputeOpt)
				.addOption(hdtLoadOpt)
				.addOption(hdtSourceOpt)
				.addOption(helpOpt);

		CommandLineParser parser = new DefaultParser();
		CommandLine cl = parser.parse(opt, args);

		if (cl.hasOption(helpOpt)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("wikichanges", opt, true);
			return;
		}
		if (cl.hasOption(todayOpt)) {
			System.out.println(Instant.now().toString());
			return;
		}
		if (!cl.hasOption(dateOpt)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("wikichanges", "Date option missing", opt, "", true);
			return;
		}
		Path outputDirectory = Path.of(cl.getOptionValue(cacheOpt, "cache"));
		Path sites = outputDirectory.resolve("sites");
		int elementPerRead = Integer.parseInt(cl.getOptionValue(elementsOpt, "500"));
		String wikiapi = cl.getOptionValue(wikiapiOpt, "https://www.wikidata.org/w/api.php");
		Date date = Date.from(Instant.parse(cl.getOptionValue(dateOpt)));
		boolean clearCache = cl.hasOption(clearCacheOpt);
		boolean noHdtRecompute = cl.hasOption(noHdtRecomputeOpt);
		boolean noCacheRecompute = cl.hasOption(noCacheRecomputeOpt);
		int maxTry = Integer.parseInt(cl.getOptionValue(maxTryOpt, "5"));
		long sleepBetweenTry = Long.parseLong(cl.getOptionValue(sleepBetweenTryOpt, "5000"));
		boolean hdtLoad = cl.hasOption(hdtLoadOpt);
		Path hdtSource;
		if (cl.hasOption(hdtSourceOpt)) {
			hdtSource = Path.of(cl.getOptionValue(hdtSourceOpt));
		} else {
			hdtSource = null;
		}

		if (elementPerRead <= 0) {
			throw new IllegalArgumentException("elementPerRead can't be negative or zero! " + elementPerRead);
		}
		if (maxTry < 0) {
			throw new IllegalArgumentException("maxTry can't be negative! " + maxTry);
		}
		if (sleepBetweenTry < 0) {
			throw new IllegalArgumentException("sleepBetweenTry can't be negative! " + sleepBetweenTry);
		}

		System.out.println("Reading from date: " + date);

		WikidataChangesFetcher fetcher = new WikidataChangesFetcher(FetcherOptions
				.builder()
				.url(wikiapi)
				.build());

		if (noCacheRecompute) {
			if (Files.exists(sites)) {
				System.out.println("Using cache " + sites);
			} else {
				throw new IOException("Using --" + noCacheRecomputeOpt.getLongOpt() + " -" + noCacheRecomputeOpt.getOpt() + " without the cache " + sites);
			}
		} else {
			Set<Change> urls = new HashSet<>();

			System.out.print("fetching changes...\r");

			ChangesIterable<Change> changes = fetcher.getChanges(date, elementPerRead, true);

			System.out.println();

			System.out.print("\r");
			long i = 0;
			for (Change change : changes) {
				long d = ++i;
				if (!change.getTitle().isEmpty() && change.getNs() == 0) {
					urls.add(change);
					printPercentage(d, changes.size(), "fetch: " + urls.size(), true);
				}
			}

			System.out.println();

			System.out.println("Downloading ttl files...");
			if (Files.exists(sites)) {
				PathUtils.deleteDirectory(sites);
			}
			Files.createDirectories(sites);

			ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

			Object logSync = new Object() {
			};

			AtomicLong downloads = new AtomicLong();
			List<? extends Future<Path>> futures = urls.stream()
					.map(change -> pool.submit(() -> {
						Path path = sites.resolve(change.getTitle() + ".ttl");
						if (!fetcher.downloadPageToFile(
								new URL("https://www.wikidata.org/wiki/Special:EntityData/" + change.getTitle() + ".ttl?flavor=simple"),
								path,
								Map.of(
										"user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
										"accept", "text/turtle",
										"accept-encoding", "gzip, deflate",
										"accept-language", "en-US,en;q=0.9",
										"upgrade-insecure-requests", "1",
										"scheme", "https"
								),
								maxTry,
								sleepBetweenTry
						)) {
							// write delete triple
							Files.writeString(path, "");
						}
						long d = downloads.incrementAndGet();
						synchronized (logSync) {
							printPercentage(d, urls.size(), "downloading", true);
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
				long d = downloads.get();
				int percentage = (int) (100L * d / urls.size());
				throw new IOException("Wasn't able to download all the files: " + d + "/" + urls.size() + " " + percentage + "%", e.getCause());
			}

			System.out.println();

			pool.shutdown();
		}

		Set<String> subjects = fetcher.fetchSubjectOfCache("http://www.wikidata.org/entity/", sites);

		System.out.println();
		System.out.println("Read " + subjects.size() + " subject(s).");

		System.out.println("Creating HDT from cache");

		Path hdtLocation = outputDirectory.resolve("sites.hdt");

		if (noHdtRecompute) {
			if (Files.exists(hdtLocation)) {
				System.out.println("Using hdt " + hdtLocation);
			} else {
				throw new IOException("Using --" + noHdtRecomputeOpt.getLongOpt() + " -" + noHdtRecomputeOpt.getOpt() + " without the hdt " + hdtLocation);
			}
		} else {
			fetcher.createHDTOfCache(sites, "http://www.wikidata.org/entity/", hdtLocation, clearCache);
			System.out.println("cache converted into: " + hdtLocation);
		}


		Path bitmapLocation = outputDirectory.resolve("bitmap.bin");

		if (hdtSource != null) {
			BitmapAccess bitmap = null;
			try {
				try (HDT sourceHDT = loadOrMap(hdtSource, hdtLoad)) {
					long count = sourceHDT.getTriples().getNumberOfElements();
					// if bitmap.size > 500MB, using disk bitmap
					if (count > 4_000_000_000L) {
						bitmap = BitmapAccess.disk(count, bitmapLocation);
					} else {
						bitmap = BitmapAccess.memory(count, bitmapLocation);
					}
					System.out.println("Compute bitmap...");
					fetcher.computeBitmap(sourceHDT, subjects, bitmap);
				}
				bitmap.save();
				System.out.println("bitmap saved to " + bitmapLocation);


				System.out.println("create diff");
				Path diffLocation = outputDirectory.resolve("diff.hdt");
				Path diffWork = diffLocation.resolveSibling("diff_work");
				Files.createDirectories(diffWork);
				try (HDT hdtDiff = HDTManager.diffHDTBit(
						diffWork.toAbsolutePath().toString(),
						hdtLocation.toAbsolutePath().toString(),
						bitmap,
						new HDTSpecification(),
						null
				)) {
					hdtDiff.saveToHDT(diffLocation.toAbsolutePath().toString(), null);
					System.out.println("diff created to " + diffLocation);
				} finally {
					PathUtils.deleteDirectory(diffWork);
				}
				System.out.println("create cat from diff/cache");
				Path resultHDTLocation = outputDirectory.resolve("result.hdt");
				Path catWork = diffLocation.resolveSibling("cat_work");
				Files.createDirectories(catWork);
				try (HDT hdtCat = HDTManager.catHDT(
						catWork.toAbsolutePath().toString(),
						diffLocation.toAbsolutePath().toString(),
						hdtLocation.toAbsolutePath().toString(),
						new HDTSpecification(),
						null
				)) {
					hdtCat.saveToHDT(resultHDTLocation.toAbsolutePath().toString(), null);
					System.out.println("cat created to " + resultHDTLocation);

					System.out.println("done.");
				} finally {
					PathUtils.deleteDirectory(catWork);
				}
				Files.delete(diffLocation);
				Files.delete(hdtLocation);
			} finally {
				if (bitmap != null) {
					bitmap.close();
				}
			}
		} else {
			System.out.println("HDT Source not specified, no bitmap/merge hdt built, use --" + hdtSourceOpt.getLongOpt() + " (hdt) to add a source");
		}
	}

	private static HDT loadOrMap(Path file, boolean load) throws IOException {
		if (load) {
			return HDTManager.loadHDT(file.toAbsolutePath().toString());
		} else {
			return HDTManager.mapHDT(file.toAbsolutePath().toString());
		}
	}

	private static void printPercentage(long value, long maxValue, String message, boolean showValues) {
		int percentage = (int) (100L * value / maxValue);
		if (showValues) {
			System.out.print(
					"[" +
							"\u25A0".repeat(percentage * 20 / 100) + " ".repeat(20 - percentage * 20 / 100)
							+ "] " + message
							+ " (" + value + " / " + maxValue + " - " + percentage + "%)       \r"
			);
		} else {
			System.out.print(
					"[" +
							"\u25A0".repeat(percentage * 20 / 100) + " ".repeat(20 - percentage * 20 / 100)
							+ "] " + message
							+ " (" + percentage + "%)       \r"
			);
		}
	}

	private final ObjectMapper mapper = new ObjectMapper();
	@Getter
	private final FetcherOptions options;

	private boolean downloadPageToFile(URL url, Path output, Map<String, String> headers, int maxTry, long sleepBetweenTry) throws IOException, InterruptedException {
		IOException lastException = null;
		if (maxTry <= 0) {
			maxTry = Integer.MAX_VALUE;
		}
		for (int i = 0; i < maxTry; i++) {
			try {
				if (!(url.openConnection() instanceof HttpURLConnection conn)) {
					throw new IllegalArgumentException("the url " + url + " isn't an http url");
				}
				headers.forEach(conn::setRequestProperty);

				try (InputStream is = new GZIPInputStream(conn.getInputStream());
					 OutputStream os = Files.newOutputStream(output)) {
					IOUtils.copy(is, os);
					// we have the file, we can leave
					return true;
				}
			} catch (java.io.FileNotFoundException e) {
				// no file, we can leave
				return false;
			} catch (IOException e) {
				if (lastException == null) {
					lastException = e;
				} else {
					lastException.addSuppressed(e);
				}
				if (sleepBetweenTry > 0) {
					try {
						Thread.sleep(sleepBetweenTry);
					} catch (InterruptedException ie) {
						ie.addSuppressed(lastException);
						throw ie;
					}
				}
			}
		}
		// too many try
		throw lastException;
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
	 * @param rcchange       the rcchange id for restart
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

	public ChangesIterable<Change> getChanges(Date end, long elementPerRead, boolean log) throws IOException {
		Iterable<Change> it = EmptyIterable.empty();
		long count = 0;

		long now = Date.from(Instant.now()).getTime();
		long deltaTime = (now - end.getTime());

		if (deltaTime <= 0) {
			throw new IllegalArgumentException("future time");
		}

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
				long current = Math.max(0, Math.min(deltaTime, deltaTime - (date.getTime() - end.getTime())));
				printPercentage(current, deltaTime, "rollback to " + date + " " + count + " elements.", false);
			}
			if (date.before(end)) {
				break;
			}
		}

		return new ChangesIterable<>(it, count);
	}

	/**
	 * create an HDT from a directory containing RDF file
	 *
	 * @param cachePath the directory
	 * @return hdt
	 * @throws IOException io error
	 */
	public HDT createHDTOfCache(Path cachePath, String baseURI) throws IOException {
		HDTOptions opts = new HDTSpecification();
		try {
			return HDTManager.generateHDT(
					cachePath.toAbsolutePath().toString(),
					baseURI,
					RDFNotation.DIR,
					opts,
					null
			);
		} catch (ParserException e) {
			throw new IOException("Can't parse HDT", e);
		}
	}

	/**
	 * create an HDT from a directory and save it into a file
	 *
	 * @param cachePath   the directory
	 * @param hdtPath     hdt path to save it
	 * @param deleteCache if we need to delete the directory
	 * @throws IOException io error
	 */
	public void createHDTOfCache(Path cachePath, String baseURI, Path hdtPath, boolean deleteCache) throws IOException {
		try (HDT hdt = createHDTOfCache(cachePath, baseURI);
			 OutputStream os = new BufferedOutputStream(Files.newOutputStream(hdtPath))) {
			hdt.saveToHDT(os, null);
		} finally {
			if (deleteCache) {
				PathUtils.deleteDirectory(cachePath);
			}
		}
	}

	public void computeBitmap(HDT source, Set<String> subjects, BitmapAccess bitmap) {
		try {
			for (String subject : subjects) {
				IteratorTripleString it = source.search(subject, "", "");
				while (it.hasNext()) {
					it.next();
					bitmap.set(it.getLastTriplePosition(), true);
				}
			}
		} catch (NotFoundException e) {
			// shouldn't happen with HDTs?
		}
	}

	public Set<String> fetchSubjectOfCache(String baseURI, Path cache) throws IOException {
		Set<String> subjects = new HashSet<>();
		Files.walkFileTree(cache, new FileVisitor<>() {
			@Override
			public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
				String filename = file.getFileName().toString();

				int dotIndex = filename.indexOf(".");
				if (dotIndex != -1) {
					String iri = baseURI + filename.substring(0, dotIndex);
					subjects.add(iri);
					System.out.print("found subject " + iri + "         \r");
				}

				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult visitFileFailed(Path file, IOException exc) {
				return FileVisitResult.CONTINUE;
			}

			@Override
			public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
				return FileVisitResult.CONTINUE;
			}
		});
		return subjects;
	}
}
