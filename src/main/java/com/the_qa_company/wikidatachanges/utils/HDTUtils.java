package com.the_qa_company.wikidatachanges.utils;

import lombok.experimental.UtilityClass;
import com.the_qa_company.qendpoint.core.enums.RDFNotation;
import com.the_qa_company.qendpoint.core.exceptions.ParserException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.rdf.RDFParserCallback;
import com.the_qa_company.qendpoint.core.rdf.RDFParserFactory;
import com.the_qa_company.qendpoint.core.triples.TripleString;
import com.the_qa_company.qendpoint.core.util.StopWatch;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

@UtilityClass
public class HDTUtils {

	/**
	 * @return a theoretical maximum amount of memory the JVM will attempt to
	 *         use
	 */
	public static long getMaxChunkSize() {
		Runtime runtime = Runtime.getRuntime();
		long presFreeMemory = (long) ((runtime.maxMemory() - (runtime.totalMemory() - runtime.freeMemory())) * 0.125
				* 0.85);
		System.out.println("Maximal available memory " + presFreeMemory);
		return presFreeMemory;
	}
	public static void compressToHdt(RDFNotation notation, String baseURI, String filename, Path hdtLocation,
									 HDTOptions specs) throws IOException {
		long chunkSize = getMaxChunkSize();

		Path hdtParentFile = hdtLocation.getParent().toAbsolutePath();
		Files.createDirectories(hdtParentFile);

		StopWatch timeWatch = new StopWatch();

		Path tempFile = hdtParentFile.resolve(filename);
		// the compression will not fit in memory, cat the files in chunks and
		// use hdtCat

		// get a triple iterator for this stream
		RDFParserCallback parser = RDFParserFactory.getParserCallback(notation);
		Iterator<TripleString> tripleIterator = PipedIterator.createOfCallback(
				pipe -> parser.doParse(filename, baseURI, notation, true, (triple, pos) -> pipe.addElement(triple))
		);
		// split this triple iterator to filed triple iterator
		FileTripleIterator it = new FileTripleIterator(tripleIterator, chunkSize);

		int file = 0;
		Path lastFile = null;
		while (it.hasNewFile()) {
			System.out.println("Compressing #" + file);
			Path hdtOutput  = hdtParentFile.resolve(tempFile.getFileName() + "." + String.format("%03d", file) + ".hdt");

			generateHDT(it, baseURI, specs, hdtOutput);

			System.gc();
			System.out.println("Competed into " + hdtOutput);
			if (file > 0) {
				// not the first file, so we have at least 2 files
				System.out.println("Cat " + hdtOutput);
				Path nextIndex = hdtParentFile.resolve("index_cat_tmp_" + file + ".hdt");
				try (HDT tmp = HDTManager.catHDT(
						nextIndex.toAbsolutePath().toString(),
						lastFile.toAbsolutePath().toString(),
						hdtOutput.toAbsolutePath().toString(),
						specs,
						HDTUtils::listener)) {
					System.out.println();

					System.out.println(
							"saving hdt with " + tmp.getTriples().getNumberOfElements() + " triple(s) into " + nextIndex);
					tmp.saveToHDT(
							nextIndex.toAbsolutePath().toString(),
							HDTUtils::listener
					);
					System.out.println();
				}
				System.gc();

				Files.delete(hdtOutput);
				if (file > 1) {
					// at least the 2nd
					Files.delete(hdtParentFile.resolve("index_cat_tmp_" + (file - 1) + ".hdt"));
					Files.delete(hdtParentFile.resolve("index_cat_tmp_" + (file - 1) + ".hdtdictionary"));
					Files.delete(hdtParentFile.resolve("index_cat_tmp_" + (file - 1) + ".hdttriples"));
				} else {
					Files.delete(lastFile);
				}
				lastFile = nextIndex;
			} else {
				lastFile = hdtOutput;
			}
			file++;
		}
		assert lastFile != null : "Last file can't be null";
		Files.move(lastFile, hdtLocation);
		if (file != 1) {
			Files.delete(hdtParentFile.resolve("index_cat_tmp_" + (file - 1) + ".hdtdictionary"));
			Files.delete(hdtParentFile.resolve("index_cat_tmp_" + (file - 1) + ".hdttriples"));
		}
		System.out.println("NT file loaded in " + timeWatch.stopAndShow());
	}

	private static void generateHDT(Iterator<TripleString> it, String baseURI, HDTOptions spec, Path hdtOutput)
			throws IOException {
			// directly use the TripleString stream to generate the HDT
		try (HDT hdtDump = HDTManager.generateHDT(it, baseURI, spec, HDTUtils::listener);
			 OutputStream out = new BufferedOutputStream(Files.newOutputStream(hdtOutput))) {
			hdtDump.saveToHDT(out, HDTUtils::listener);
		} catch (ParserException e) {
			throw new IOException("Can't generate HDT", e);
		} finally {
			System.out.println();
		}
	}

	private static String last = "";
	public static void listener(float progress, String message) {
		String print = message + "(" + (int) (progress) + "%)";
		System.out.print(print + " ".repeat(Math.max(0, last.length() - print.length())) + "\r");
		last = print;
	}
}
