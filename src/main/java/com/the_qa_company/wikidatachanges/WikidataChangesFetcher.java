package com.the_qa_company.wikidatachanges;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.the_qa_company.wikidatachanges.api.ApiResult;
import com.the_qa_company.wikidatachanges.api.Change;
import com.the_qa_company.wikidatachanges.utils.HDTUtils;
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
import org.rdfhdt.hdt.compact.bitmap.Bitmap64;
import org.rdfhdt.hdt.compact.bitmap.Bitmap64Disk;
import org.rdfhdt.hdt.compact.bitmap.ModifiableBitmap;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.dictionary.DictionarySection;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.enums.TripleComponentRole;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleID;
import org.rdfhdt.hdt.triples.TripleID;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
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
		Option sleepBetweenTryOpt = new Option("S", "sleeptry", true, "Millis to sleep between try , 0 for no sleep (default: 5000)");
		Option noHdtRecomputeOpt = new Option("H", "nonewhdt", false, "Don't recompute the HDT");
		Option hdtLoadOpt = new Option("l", "hdtload", false, "Load the HDT into memory, fast up the process");
		Option hdtSourceOpt = new Option("s", "hdtsource", true, "Hdt source location (required to compute bitmaps and merge hdt)");
		Option mapBitMapOpt = new Option("B", "mapbitmap", false, "map the bitmap into disk, reduce the memory using and the speed");
		Option noDiffRecomputeOpt = new Option("u", "nodiff", false, "No diff recompute");
		Option deleteDiffEndOpt = new Option("U", "deletediff", false, "Delete diff at the end");
		Option deleteSitesEndOpt = new Option("v", "deletesites", false, "Delete cache HDT at the end");
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
				.addOption(mapBitMapOpt)
				.addOption(sleepBetweenTryOpt)
				.addOption(noHdtRecomputeOpt)
				.addOption(hdtLoadOpt)
				.addOption(hdtSourceOpt)
				.addOption(noDiffRecomputeOpt)
				.addOption(deleteDiffEndOpt)
				.addOption(deleteSitesEndOpt)
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
		if (!Files.exists(outputDirectory)) {
			Files.createDirectories(outputDirectory);
		}
		int elementPerRead = Integer.parseInt(cl.getOptionValue(elementsOpt, "500"));
		String wikiapi = cl.getOptionValue(wikiapiOpt, "https://www.wikidata.org/w/api.php");
		Date date = Date.from(Instant.parse(cl.getOptionValue(dateOpt)));
		boolean clearCache = cl.hasOption(clearCacheOpt);
		boolean noHdtRecompute = cl.hasOption(noHdtRecomputeOpt);
		boolean noCacheRecompute = cl.hasOption(noCacheRecomputeOpt);
		int maxTry = Integer.parseInt(cl.getOptionValue(maxTryOpt, "5"));
		long sleepBetweenTry = Long.parseLong(cl.getOptionValue(sleepBetweenTryOpt, "5000"));
		boolean hdtLoad = cl.hasOption(hdtLoadOpt);
		boolean mapBitmap = cl.hasOption(mapBitMapOpt);
		boolean noDiffRecompute = cl.hasOption(noDiffRecomputeOpt);
		boolean deleteDiffEnd = cl.hasOption(deleteDiffEndOpt);
		boolean deleteSitesEnd = cl.hasOption(deleteSitesEndOpt);

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

		Path deletedSubjects = outputDirectory.resolve("deletedSubjects");

		if (!noCacheRecompute) {
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
			Object writeSync = new Object() {
			};

			try (BufferedWriter writer = Files.newBufferedWriter(deletedSubjects)) {
				AtomicLong downloads = new AtomicLong();
				Map<String, String> urlHeader = Map.of(
						"user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
						"accept", "text/turtle",
						"accept-encoding", "gzip, deflate",
						"accept-language", "en-US,en;q=0.9",
						"upgrade-insecure-requests", "1",
						"scheme", "https"
				);
				List<? extends Future<Path>> futures = urls.stream()
						.map(change -> pool.submit(() -> {
							Path path = sites.resolve(change.getTitle() + ".ttl");
							if (!fetcher.downloadPageToFile(
									new URL("https://www.wikidata.org/wiki/Special:EntityData/" + change.getTitle() + ".ttl?flavor=simple"),
									path,
									urlHeader,
									maxTry,
									sleepBetweenTry
							)) {
								// write delete triple
								synchronized (writeSync) {
									writer.write("""
											https://www.wikidata.org/wiki/Special:EntityData/%1$s
											http://www.wikidata.org/entity/%1$s
											""".formatted(change.getTitle()));
								}
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
		}

		System.out.println("Creating HDT from cache");

		Path hdtLocation = outputDirectory.resolve("sites.hdt");

		if (!noHdtRecompute) {
			if (Files.exists(sites)) {
				System.out.println("Using cache " + sites);
			} else {
				throw new IOException("can't find cache " + sites);
			}

			fetcher.createHDTOfCache(sites, "http://www.wikidata.org/entity/", hdtLocation, clearCache);
			System.out.println("cache converted into: " + hdtLocation);
		}


		Path bitmapLocation = outputDirectory.resolve("bitmap.bin");

		if (hdtSource != null) {
			ModifiableBitmap bitmap = null;
			if (Files.exists(hdtLocation)) {
				System.out.println("Using hdt " + hdtLocation);
			} else {
				throw new IOException("Can't find hdt " + hdtLocation);
			}

			Path diffLocation = outputDirectory.resolve("diff.hdt");

			if (!noDiffRecompute) {
				if (!Files.exists(deletedSubjects)) {
					throw new IllegalArgumentException("deleted subject file doesn't exist! " + deletedSubjects);
				}
				try {
					Set<String> deletedSubjectsSet = new HashSet<>();
					try (BufferedReader deletedSubjectsReader = Files.newBufferedReader(deletedSubjects)) {
						System.out.println("Reading deleted subjects");

						String line;

						while ((line = deletedSubjectsReader.readLine()) != null) {
							if (line.isEmpty() || line.charAt(0) == '#') {
								continue;
							}

							deletedSubjectsSet.add(line);
							System.out.println("found " + deletedSubjectsSet.size() + " deleted subjects\r");
						}
						System.out.println();
					}
					try (HDT sourceHDT = loadOrMap(hdtSource, hdtLoad);
						 HDT sitesHDT = loadOrMap(hdtLocation, hdtLoad)) {
						long count = sourceHDT.getTriples().getNumberOfElements() + 2;
						System.out.println("build bitmap of size: " + count);
						if (mapBitmap) {
							bitmap = new Bitmap64Disk(bitmapLocation.toAbsolutePath().toString(), count);
						} else {
							bitmap = new Bitmap64(count);
						}
						fetcher.computeBitmap(sourceHDT, sitesHDT, deletedSubjectsSet, bitmap);
					}


					System.out.println("create diff");
					Path diffWork = diffLocation.resolveSibling("diff_work");
					Files.createDirectories(diffWork);
					try (HDT hdtDiff = HDTManager.diffHDTBit(
							diffWork.toAbsolutePath() + "/",
							hdtSource.toAbsolutePath().toString(),
							bitmap,
							new HDTSpecification(),
							HDTUtils::listener
					)) {
						System.out.println();
						hdtDiff.saveToHDT(diffLocation.toAbsolutePath().toString(), null);
						System.out.println("diff created to " + diffLocation);
					} finally {
						PathUtils.deleteDirectory(diffWork);
					}
				} finally {
					if (bitmap instanceof Closeable) {
						((Closeable) bitmap).close();
					}
				}
			} else {
				if (Files.exists(diffLocation)) {
					System.out.println("Using diff hdt " + diffLocation);
				} else {
					throw new IOException("can't find diff hdt " + diffLocation);
				}
			}
			System.out.println("create cat from diff/cache");
			Path resultHDTLocation = outputDirectory.resolve("result.hdt");
			Path catWork = diffLocation.resolveSibling("cat_work");
			Files.createDirectories(catWork);
			try (HDT hdtCat = HDTManager.catHDT(
					catWork.toAbsolutePath() + "/",
					diffLocation.toAbsolutePath().toString(),
					hdtLocation.toAbsolutePath().toString(),
					new HDTSpecification(),
					HDTUtils::listener
			)) {
				System.out.println();
				hdtCat.saveToHDT(resultHDTLocation.toAbsolutePath().toString(), null);
				System.out.println("cat created to " + resultHDTLocation);

				System.out.println("done.");
			} finally {
				PathUtils.deleteDirectory(catWork);
			}
			if (deleteDiffEnd) {
				Files.delete(diffLocation);
			}
			if (deleteSitesEnd) {
				Files.delete(hdtLocation);
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
		List<Change> arr = new ArrayList<>();
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
			arr.addAll(recentchanges);

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

		return new ChangesIterable<>(arr, count);
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
		HDTOptions opts = new HDTSpecification();
		HDTUtils.compressToHdt(RDFNotation.DIR, baseURI, cachePath.toAbsolutePath().toString(), hdtPath, opts);
		if (deleteCache) {
			PathUtils.deleteDirectory(cachePath);
		}
	}

	public void computeBitmap(HDT source, HDT sites, Set<String> deletedSubjects, ModifiableBitmap bitmap) {
		int n = 0;
		DictionarySection subjectsSection = sites.getDictionary().getSubjects();
		Dictionary sourceDict = source.getDictionary();
		long subjects = subjectsSection.getNumberOfElements() + deletedSubjects.size();
		long deleteStatements = 0;

		for (Iterator<? extends CharSequence> its = subjectsSection.getSortedEntries(); its.hasNext(); ) {
			CharSequence subject = its.next();
			long sid = sourceDict.stringToId(subject, TripleComponentRole.SUBJECT);
			IteratorTripleID it = source.getTriples().search(new TripleID(sid, 0, 0));
			while (it.hasNext()) {
				it.next();
				bitmap.set(it.getLastTriplePosition(), true);
				deleteStatements++;
			}
			printPercentage(++n, subjects, "compute delete bitmap " + deleteStatements + " statement(s) to delete", true);
		}
		for (CharSequence subject : deletedSubjects) {
			long sid = sourceDict.stringToId(subject, TripleComponentRole.SUBJECT);
			IteratorTripleID it = source.getTriples().search(new TripleID(sid, 0, 0));
			while (it.hasNext()) {
				it.next();
				bitmap.set(it.getLastTriplePosition(), true);
				deleteStatements++;
			}
			printPercentage(++n, subjects, "compute delete bitmap " + deleteStatements + " statement(s) to delete", true);
		}
		printPercentage(subjects, subjects, "compute delete bitmap " + deleteStatements + " statement(s) to delete", true);
		System.out.println();
	}

}
