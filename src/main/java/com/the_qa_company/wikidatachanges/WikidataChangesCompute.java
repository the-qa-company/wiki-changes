package com.the_qa_company.wikidatachanges;

import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap;
import com.the_qa_company.qendpoint.core.compact.bitmap.Bitmap64Big;
import com.the_qa_company.qendpoint.core.enums.TripleComponentRole;
import com.the_qa_company.qendpoint.core.exceptions.NotFoundException;
import com.the_qa_company.qendpoint.core.hdt.HDT;
import com.the_qa_company.qendpoint.core.hdt.HDTManager;
import com.the_qa_company.qendpoint.core.iterator.utils.ExceptionIterator;
import com.the_qa_company.qendpoint.core.iterator.utils.NotificationExceptionIterator;
import com.the_qa_company.qendpoint.core.options.HDTOptions;
import com.the_qa_company.qendpoint.core.options.HDTOptionsKeys;
import com.the_qa_company.qendpoint.core.triples.IteratorTripleID;
import com.the_qa_company.qendpoint.core.triples.TripleID;
import com.the_qa_company.qendpoint.core.util.StopWatch;
import com.the_qa_company.qendpoint.core.util.listener.ColorTool;
import com.the_qa_company.qendpoint.core.util.listener.MultiThreadListenerConsole;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WikidataChangesCompute {
	public static void main(String[] args) throws IOException {
		MultiThreadListenerConsole console = new MultiThreadListenerConsole(true);
		ColorTool tool = new ColorTool(true);
		tool.setConsole(console);
		StopWatch swa = new StopWatch();

		if (args.length == 0) {
			tool.error("[bitmap|diff|infobm]");
			return;
		}

		HDTOptions spec = HDTOptions.of(
				"profiler", true,
				"profiler.output", "prof.opt",
				"parser.deltafile.nocrc", true,
				"parser.deltafile.noExceptionOnlyStop", true,
				HDTOptionsKeys.HDTCAT_LOCATION, "hdtcat",
				HDTOptionsKeys.HDTCAT_FUTURE_LOCATION, "outcat.hdt",
				HDTOptionsKeys.HDTCAT_DELETE_LOCATION, true
		);

		switch (args[0]) {
			case "bitmap" -> {
				if (args.length < 4) {
					System.err.println("bitmap [in-hdt] [delta-hdt] [out-bitmap]");
					return;
				}
				Path inHDTFile = Path.of(args[1]);
				Path deltaHDTFile = Path.of(args[2]);
				Path outBitmap = Path.of(args[3]);

				tool.log("Mapping hdts...");
				StopWatch sw = new StopWatch();
				sw.reset();
				try (
						HDT inHDT = HDTManager.mapHDT(inHDTFile, console);
						HDT deltaHDT = HDTManager.mapHDT(deltaHDTFile, console);
						Bitmap64Big bitmap = Bitmap64Big.map(outBitmap, inHDT.getTriples().getNumberOfElements())
				) {
					long itCount = deltaHDT.getDictionary().getNsubjects();
					Iterator<? extends CharSequence> it = deltaHDT.getDictionary().stringIterator(TripleComponentRole.SUBJECT, true);
					NotificationExceptionIterator<? extends CharSequence, RuntimeException> it2 = new NotificationExceptionIterator<>(ExceptionIterator.of(it), itCount, itCount / 1000, "compute deletes", console);

					tool.log("done in " + sw.stopAndShow());
					sw.reset();
					tool.log("Computing delete bitmap...");
					long match = 0;
					TripleID tid = new TripleID();
					while (it2.hasNext()) {
						CharSequence next = it2.next();

						long id = inHDT.getDictionary().stringToId(next, TripleComponentRole.SUBJECT);

						if (id <= 0) {
							continue;
						}

						tid.setSubject(id);

						IteratorTripleID search = inHDT.getTriples().search(tid);

						while (search.hasNext()) {
							search.next(); // ignore
							bitmap.set(search.getLastTriplePosition(), true);
							match++;
						}
					}

					tool.log(console.colorReset() + "Find " + match + " triple(s) to delete into bitmap " + outBitmap);
					tool.log("done in " + sw.stopAndShow());
				}
			}
			case "infobm" -> {
				if (args.length < 3) {
					System.err.println("infobm [in-hdt] [out-bitmap]");
					return;
				}
				Path inHDTFile = Path.of(args[1]);
				Path inBitmap = Path.of(args[2]);

				tool.log("loading hdt/bm...");
				try (
						HDT hdt = HDTManager.mapHDT(inHDTFile);
						Bitmap64Big bm = Bitmap64Big.map(inBitmap, hdt.getTriples().getNumberOfElements())
				) {
					long c = 0;
					for (long i = 0; i < hdt.getTriples().getNumberOfElements(); i++) {
						if (bm.access(i)) c++;
					}
					tool.log("find: " + c);
				}

			}
			case "diff" -> {
				if (args.length < 5) {
					System.err.println("diff [in-hdt] [delta-hdt] [out-bitmap] [out-res]");
					return;
				}
				Path inHDTFile = Path.of(args[1]);
				Path deltaHDTFile = Path.of(args[2]);
				Path outBitmap = Path.of(args[3]);
				Path outHDT = Path.of(args[4]);

				tool.log("Mapping hdts...");
				StopWatch sw = new StopWatch();
				sw.reset();
				long mainTriples;
				try (HDT inHDT = HDTManager.mapHDT(inHDTFile, console)) {
					mainTriples = inHDT.getTriples().getNumberOfElements();
				}

				tool.log("done in " + sw.stopAndShow());
				sw.reset();
				tool.log("Computing diff bitmap...");

				try (
					Bitmap64Big bitmap = Bitmap64Big.map(outBitmap, mainTriples);
					HDT diff = HDTManager.diffBitCatHDTPath(
						List.of(inHDTFile, deltaHDTFile), Arrays.<Bitmap>asList(bitmap, null), spec, console
				)) {
					diff.saveToHDT(outHDT);
					tool.log("done in " + sw.stopAndShow());
					tool.log("old size: " + mainTriples);
					long newSize = diff.getTriples().getNumberOfElements();
					tool.log("new size: " + newSize + " (+" + (mainTriples * 100 / newSize) + "%)");
				}

			}
			case "div" -> {
				if (args.length < 3) {
					System.err.println("div [in-hdt] [count]");
					return;
				}
				Path inHDTFile = Path.of(args[1]);
				int div = Integer.parseInt(args[2]);

				if (div <= 1) {
					tool.error("Count should be at least 2");
					return;
				}

				long triples;

				tool.log("reading " + inHDTFile);
				try (HDT hdt = HDTManager.mapHDT(inHDTFile)) {
					triples = hdt.getTriples().getNumberOfElements();
				}
				tool.log("find value " + triples);


				if (triples < div) {
					tool.error("Can't divide more than the triple count " + triples + " < " + div);
					return;
				}

				long divSplit = triples / div;
				for (int i = 0; i < div; i++) {
					try (Bitmap64Big delete = Bitmap64Big.memory(triples)) {
						for (long idx = i * divSplit; idx < (i + 1) * divSplit; idx++) {
							delete.set(idx, true);
						}

						Path splitName = inHDTFile.resolveSibling("split-" + (i + 1) + "-" + inHDTFile.getFileName());
						tool.log("Generating split #" + (i + 1) + ": " + splitName);

						try (HDT hdtSplit = HDTManager.diffBitCatHDTPath(List.of(inHDTFile), List.of(delete), spec, console)) {
							hdtSplit.saveToHDT(splitName);
						}
					}
				}
				tool.log(div + " split generated");
			}
			case "mergediff" -> {
				if (args.length < 5) {
					System.err.println("div [in-hdt] [bitmap] [delta-hdt] [count]");
					return;
				}

				Path inHDTFile = Path.of(args[1]);
				Path bitmapFile = Path.of(args[2]);
				Path deltaHDTFile = Path.of(args[3]);
				int div = Integer.parseInt(args[4]);

				if (div <= 1) {
					tool.error("Count should be at least 2");
					return;
				}

				tool.log("get hdt count");

				long triples;
				try (HDT hdt = HDTManager.mapHDT(inHDTFile)) {
					triples = hdt.getTriples().getNumberOfElements();
				}
				tool.log("count: " + triples);

				StopWatch sw = new StopWatch();
				try (Bitmap64Big bitmap = Bitmap64Big.map(bitmapFile, triples)) {
					for (int i = 0; i < div; i++) {

						tool.log("Test with #" + (i + 1) + " split");
						sw.reset();

						List<Path> ds = new ArrayList<>();
						List<Bitmap> deletes = new ArrayList<>();

						ds.add(inHDTFile);
						deletes.add(bitmap);

						for (int j = 0; j < div; j++) {
							Path splitName = deltaHDTFile.resolveSibling("split-" + (i + 1) + "-" + deltaHDTFile.getFileName());
							ds.add(splitName);
							deletes.add(null);
						}

						HDTOptions spec2 = spec.pushTop();
						// set profiler output
						spec2.set(HDTOptionsKeys.PROFILER_OUTPUT_KEY, "prof" + (i + 1) + ".opt");
						// optimize output
						spec2.set(HDTOptionsKeys.HDTCAT_FUTURE_LOCATION, "changes.hdt");

						try (HDT hdtSplit = HDTManager.diffBitCatHDTPath(ds, deletes, spec2, console)) {
							hdtSplit.saveToHDT("changes.hdt");
							tool.log("done in " + sw.stopAndShow());
							long ntriples = hdtSplit.getTriples().getNumberOfElements();
							tool.log("size: " + ntriples + "(+" + (triples * 100 / ntriples) + "%");
						}
					}
				}

			}
			default -> tool.error("Bad arg: " + args[0]);
		}
		tool.log("Executed in " + swa.stopAndShow());
	}
}
