package com.the_qa_company.wikidatachanges;

import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCOutputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.wikidatachanges.api.Change;
import com.the_qa_company.wikidatachanges.api.RDFFlavor;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static com.the_qa_company.wikidatachanges.WikidataChangesFetcher.printPercentage;

public class WikidataChangesDelta {
	public static void main(String[] args) throws ParseException, IOException, InterruptedException {

		Option elementsOpt = new Option("e", "element", true, "element to ask to the wiki API");
		Option todayOpt = new Option("T", "today", false, "Print date");
		Option dateOpt = new Option("d", "date", true, "Wiki api location (required)");
		Option wikiapiOpt = new Option("w", "wikiapi", true, "Wiki api location");
		Option flavorOpt = new Option("f", "flavor", true, "The flavor to retrieve the RDF outputs, important if you are using a truthy hdt");
		Option flavorListOpt = new Option("F", "flavorlist", false, "The flavor list for the --" + flavorOpt.getLongOpt() + " option");
		Option maxTryOpt = new Option("m", "maxtry", true, "Number of try with http request, 0 for infinity (default: 5)");
		Option sleepBetweenTryOpt = new Option("S", "sleeptry", true, "Millis to sleep between try , 0 for no sleep (default: 5000)");
		Option helpOpt = new Option("h", "help", false, "Print help");

		Options opt = new Options()
				.addOption(flavorOpt)
				.addOption(flavorListOpt)
				.addOption(elementsOpt)
				.addOption(todayOpt)
				.addOption(dateOpt)
				.addOption(wikiapiOpt)
				.addOption(maxTryOpt)
				.addOption(sleepBetweenTryOpt)
				.addOption(helpOpt);

		CommandLineParser parser = new DefaultParser();
		CommandLine cl = parser.parse(opt, args);

		if (cl.hasOption(helpOpt)) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("wikichanges", opt, true);
			return;
		}

		Instant now = Instant.now();
		if (cl.hasOption(todayOpt)) {
			System.out.println(now.toString());
			return;
		}

		if (cl.hasOption(flavorListOpt)) {
			System.out.println("Flavors:");
			for (RDFFlavor f : RDFFlavor.values()) {
				System.out.println(f.name().toLowerCase(Locale.ROOT) + " - " + f.getDescription() + (f.isShouldSpecify() ? "" : " (default)"));
			}
			return;
		}

		int elementPerRead = Integer.parseInt(cl.getOptionValue(elementsOpt, "500"));
		RDFFlavor flavor = RDFFlavor.valueOf(cl.getOptionValue(flavorOpt, RDFFlavor.getDefaultFlavor().name()).toUpperCase(Locale.ROOT));
		String wikiapi = cl.getOptionValue(wikiapiOpt, "https://www.wikidata.org/w/api.php");
		Date date = Optional
				.ofNullable(cl.getOptionValue(dateOpt))
				.map(d -> Date.from(Instant.parse(d)))
				.orElse(null);
		int maxTry = Integer.parseInt(cl.getOptionValue(maxTryOpt, "5"));
		long sleepBetweenTry = Long.parseLong(cl.getOptionValue(sleepBetweenTryOpt, "5000"));

		FetcherOptions fopt = FetcherOptions.builder()
				.url(wikiapi)
				.build();

		WikidataChangesFetcher delta = new WikidataChangesFetcher(fopt);

		Path deltaDir = Path.of("deltafiles");
		Files.createDirectories(deltaDir);

		if (sleepBetweenTry < 0) {
			throw new IllegalArgumentException("sleepBetweenTry can't be negative! " + sleepBetweenTry);
		}

		String flavorUrlOpt;

		if (flavor.isShouldSpecify()) {
			flavorUrlOpt = "?flavor=" + flavor.getTitle();
		} else {
			flavorUrlOpt = "";
		}

		if (date == null) {
			System.err.println("No date specified, please use --" + dateOpt.getLongOpt() + " [date] to set it!");
			return;
		}

		ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());


		ChangesIterable<Change> changes = delta.getChanges(date, elementPerRead, true);

		System.out.println();

		System.out.print("\r");

		Set<Change> urls = new HashSet<>();

		Instant dateInstant = date.toInstant();
		System.out.printf("Fetching changes from %s to %s...\n",
				now, dateInstant);

		long i = 0;
		for (Change change : changes) {
			long d = ++i;
			if (!change.getTitle().isEmpty() && change.getNs() == 0) {
				urls.add(change);
				if (i % 1000 == 0) {
					printPercentage(d, changes.size(), "fetch: " + urls.size(), true);
				}
			}
		}

		// write lock
		Object syncLock = new Object() {
		};

		Path deltaNt = deltaDir.resolve("delta.df");

		try (CRCOutputStream osnt = new CRCOutputStream(new BufferedOutputStream(Files.newOutputStream(deltaNt)), new CRC8())) {

			AtomicLong downloads = new AtomicLong();
			Map<String, String> urlHeader = Map.of(
					"user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36",
					"accept", "text/turtle",
					"accept-encoding", "gzip, deflate",
					"accept-language", "en-US,en;q=0.9",
					"upgrade-insecure-requests", "1",
					"scheme", "https"
			);

			byte[] empty = new byte[0];
			ProgressListener pl = ProgressListener.ignore();

			// cookie (8B)
			osnt.write("$DltF0\n\r".getBytes(StandardCharsets.US_ASCII));
			// urls (8B)
			IOUtil.writeLong(osnt, urls.size());
			// start (8B)
			IOUtil.writeLong(osnt, now.getEpochSecond() * 1_000_000 + now.getNano() / 1000);
			// end (8B)
			IOUtil.writeLong(osnt, dateInstant.getEpochSecond() * 1_000_000 + dateInstant.getNano() / 1000);
			// flavor (1B)
			osnt.write(flavor.getId());
			// padding (3B)
			osnt.write(0);
			osnt.write(0);
			osnt.write(0);

			// write header CRC and prepare for data part
			// CRC (1B)
			osnt.writeCRC();
			osnt.setCRC(new CRC32());

			List<? extends Future<Void>> futures = urls.stream()
					.map(change -> pool.submit(() -> {
						String titleFile = change.getTitle() + ".ttl";
						String url = "https://www.wikidata.org/wiki/Special:EntityData/" + change.getTitle() + ".ttl" + flavorUrlOpt;

						try {
							byte[] page = delta.downloadPage(
									new URL(url),
									urlHeader,
									maxTry,
									sleepBetweenTry,
									false
							);
							long d = downloads.incrementAndGet();
							synchronized (syncLock) {
								IOUtil.writeSizedBuffer(osnt, titleFile.getBytes(StandardCharsets.UTF_8), pl);
								IOUtil.writeSizedBuffer(osnt, Objects.requireNonNullElse(page, empty), pl);
								printPercentage(d, urls.size(), "downloading", true);
							}
						} catch (IOException | InterruptedException e) {
							throw new RuntimeException(e);
						}
						return (Void) null;
					}))
					.toList();

			try {
				for (Future<Void> f : futures) {
					f.get();
				}
			} catch (ExecutionException e) {
				pool.shutdownNow();
				long d = downloads.get();
				int percentage = (int) (100L * d / urls.size());
				throw new IOException("Wasn't able to download all the files: " + d + "/" + urls.size() + " " + percentage + "%", e.getCause());
			}

			osnt.writeCRC();

			System.out.println();

			pool.shutdown();
		}

		System.out.println("Fetched " + urls.size() + " files");
		System.out.println("Dump: " + deltaNt);

	}
}
