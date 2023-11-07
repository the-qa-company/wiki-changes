package com.the_qa_company.wikidatachanges.datastruct;

import com.the_qa_company.qendpoint.core.exceptions.CRCException;
import com.the_qa_company.qendpoint.core.listener.ProgressListener;
import com.the_qa_company.qendpoint.core.util.crc.CRC32;
import com.the_qa_company.qendpoint.core.util.crc.CRC8;
import com.the_qa_company.qendpoint.core.util.crc.CRCInputStream;
import com.the_qa_company.qendpoint.core.util.io.IOUtil;
import com.the_qa_company.wikidatachanges.WikidataChangesFetcher;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

public class DeltaFileUtils {
	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.err.println("[check [file]]");
			return;
		}

		switch (args[0]) {
			case "check" -> {
				checkFile(Path.of(args[1]));
			}
			case "z" -> {

			}
			default -> {
				System.err.println("Bad cfg");
				return;
			}
		}
	}
	public static void checkFile(Path path) throws IOException {
		try (CRCInputStream is = new CRCInputStream(new BufferedInputStream(Files.newInputStream(path)), new CRC8())) {
			// read header
			if (!Arrays.equals("$DltF0\n\r".getBytes(StandardCharsets.US_ASCII), is.readNBytes(8))) {
				throw new IOException("Bad cookie");
			}

			long urls = IOUtil.readLong(is);
			long start = IOUtil.readLong(is);
			long end = IOUtil.readLong(is);
			int flavor = is.read();

			is.skipNBytes(3);

			if (is.readCRCAndCheck()) {
				throw new CRCException("Bad crc");
			}

			is.setCRC(new CRC32());

			ProgressListener pl = ProgressListener.ignore();
			try {
				for (long i = 0; i < urls; i++) {
					// name
					IOUtil.readSizedBuffer(is, pl);
					// buffer
					IOUtil.readSizedBuffer(is, pl);

					WikidataChangesFetcher.printPercentage(i, urls, "reading files", true);
				}
			} catch (Throwable t) {
				System.out.println();
				throw t;
			}
			if (!is.readCRCAndCheck()) {
				throw new CRCException("Bad 32 crc");
			}
		}
	}
}
