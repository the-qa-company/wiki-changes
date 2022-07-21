package com.the_qa_company.wikidatachanges.utils;

import org.rdfhdt.hdt.compact.bitmap.Bitmap;
import org.rdfhdt.hdt.compact.bitmap.Bitmap64;
import org.rdfhdt.hdt.compact.bitmap.Bitmap64Disk;
import org.rdfhdt.hdt.listener.ProgressListener;

import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public interface BitmapAccess extends Closeable, Bitmap {
	static BitmapAccess memory(long size, Path path) {
		Bitmap64 bitmap = new Bitmap64(size);
		return new BitmapAccess() {
			@Override
			public boolean access(long position) {
				return bitmap.access(position);
			}

			@Override
			public void save() throws IOException {
				try (OutputStream os = new BufferedOutputStream(Files.newOutputStream(path))) {
					bitmap.save(os, null);
				}
			}

			@Override
			public void close() {
				// ignore, by default
			}

			@Override
			public void set(long position, boolean value) {
				bitmap.set(position, value);
			}
		};
	}

	static BitmapAccess disk(long size, Path path) {
		Bitmap64Disk bitmap = new Bitmap64Disk(path.toAbsolutePath().toString(), size);
		return new BitmapAccess() {
			@Override
			public boolean access(long position) {
				return bitmap.access(position);
			}

			@Override
			public void save() {
				// ignore, by default
			}

			@Override
			public void close() throws IOException {
				bitmap.close();
			}

			@Override
			public void set(long position, boolean value) {
				bitmap.set(position, value);
			}
		};
	}

	void save() throws IOException;

	@Override
	boolean access(long position);

	void set(long position, boolean value);

	@Override
	default long rank1(long position) {
		throw new RuntimeException("not implemented");
	}

	@Override
	default long rank0(long position) {
		throw new RuntimeException("not implemented");
	}

	@Override
	default long selectPrev1(long start) {
		throw new RuntimeException("not implemented");
	}

	@Override
	default long selectNext1(long start) {
		throw new RuntimeException("not implemented");
	}

	@Override
	default long select0(long n) {
		throw new RuntimeException("not implemented");
	}

	@Override
	default long select1(long n) {
		throw new RuntimeException("not implemented");
	}

	@Override
	default long getNumBits() {
		throw new RuntimeException("not implemented");
	}

	@Override
	default long countOnes() {
		throw new RuntimeException("not implemented");
	}

	@Override
	default long countZeros() {
		throw new RuntimeException("not implemented");
	}

	@Override
	default long getSizeBytes() {
		throw new RuntimeException("not implemented");
	}

	@Override
	default void save(OutputStream output, ProgressListener listener) {
		throw new RuntimeException("not implemented");
	}

	@Override
	default void load(InputStream input, ProgressListener listener) {
		throw new RuntimeException("not implemented");
	}

	@Override
	default String getType() {
		throw new RuntimeException("not implemented");
	}
}
