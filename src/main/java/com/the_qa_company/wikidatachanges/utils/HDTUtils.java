package com.the_qa_company.wikidatachanges.utils;

import lombok.experimental.UtilityClass;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTOptions;
import org.rdfhdt.hdt.rdf.RDFParserCallback;
import org.rdfhdt.hdt.rdf.RDFParserFactory;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdt.util.StopWatch;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
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
	public static void compressToHdt(RDFNotation notation, String baseURI, String filename, String hdtLocation,
									 HDTOptions specs) throws IOException {
		long chunkSize = getMaxChunkSize();

		File hdtParentFile = new File(hdtLocation).getParentFile();
		String hdtParent = hdtParentFile.getAbsolutePath();
		Files.createDirectories(hdtParentFile.toPath());

		StopWatch timeWatch = new StopWatch();

		File tempFile = new File(hdtParentFile, filename);
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
		String lastFile = null;
		while (it.hasNewFile()) {
			System.out.println("Compressing #" + file);
			String hdtOutput = new File(tempFile.getParent(),
					tempFile.getName() + "." + String.format("%03d", file) + ".hdt").getAbsolutePath();

			generateHDT(it, baseURI, specs, hdtOutput);

			System.gc();
			System.out.println("Competed into " + hdtOutput);
			if (file > 0) {
				// not the first file, so we have at least 2 files
				System.out.println("Cat " + hdtOutput);
				String nextIndex = hdtParent + "/index_cat_tmp_" + file + ".hdt";
				HDT tmp = HDTManager.catHDT(nextIndex, lastFile, hdtOutput, specs, null);

				System.out.println(
						"saving hdt with " + tmp.getTriples().getNumberOfElements() + " triple(s) into " + nextIndex);
				tmp.saveToHDT(nextIndex, null);
				tmp.close();
				System.gc();

				Files.delete(Paths.get(hdtOutput));
				if (file > 1) {
					// at least the 2nd
					Files.delete(Paths.get(hdtParent, "/index_cat_tmp_" + (file - 1) + ".hdt"));
					Files.delete(Paths.get(hdtParent, "/index_cat_tmp_" + (file - 1) + ".hdtdictionary"));
					Files.delete(Paths.get(hdtParent, "/index_cat_tmp_" + (file - 1) + ".hdttriples"));
				} else {
					Files.delete(Paths.get(lastFile));
				}
				lastFile = nextIndex;
			} else {
				lastFile = hdtOutput;
			}
			file++;
		}
		assert lastFile != null : "Last file can't be null";
		Files.move(Paths.get(lastFile), Paths.get(hdtLocation));
		if (file != 1) {
			Files.delete(Paths.get(hdtParent, "/index_cat_tmp_" + (file - 1) + ".hdtdictionary"));
			Files.delete(Paths.get(hdtParent, "/index_cat_tmp_" + (file - 1) + ".hdttriples"));
		}
		System.out.println("NT file loaded in " + timeWatch.stopAndShow());
	}

	private void generateHDT(Iterator<TripleString> it, String baseURI, HDTOptions spec, String hdtOutput)
			throws IOException {
			// directly use the TripleString stream to generate the HDT
		try (HDT hdtDump = HDTManager.generateHDT(it, baseURI, spec, null)) {
			hdtDump.saveToHDT(hdtOutput, null);
		} catch (ParserException e) {
			throw new IOException("Can't generate HDT", e);
		}
	}

}
