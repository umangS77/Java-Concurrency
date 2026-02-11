package Concurrency.FileCopy;

import java.io.RandomAccessFile;
import java.util.concurrent.locks.ReentrantLock;

public class ParallelFileCopyService {

    private static final int CHUNK_SIZE = 8 * 1024 * 1024; // 8MB

    public static void copy(String srcPath,
                            String dstPath,
                            int threadCount) throws Exception {

        RandomAccessFile in = new RandomAccessFile(srcPath, "r");
        RandomAccessFile out = new RandomAccessFile(dstPath, "rw");

        long fileSize = in.length();
        out.setLength(fileSize); // pre-allocate destination

        long totalChunks = (fileSize + CHUNK_SIZE - 1) / CHUNK_SIZE;

        ReentrantLock readLock = new ReentrantLock();
        ReentrantLock writeLock = new ReentrantLock();

        Thread[] workers = new Thread[threadCount];

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;

            workers[t] = new Thread(() -> {

                try {
                    byte[] buffer = new byte[CHUNK_SIZE];

                    // each thread processes its own chunk stripe
                    for (long chunk = threadId;
                         chunk < totalChunks;
                         chunk += threadCount) {

                        long offset = chunk * CHUNK_SIZE;
                        int len = (int)Math.min(
                                CHUNK_SIZE,
                                fileSize - offset);

                        // ---------- READ ----------
                        readLock.lock();
                        int bytesRead;
                        try {
                            in.seek(offset);
                            bytesRead = in.read(buffer, 0, len);
                        } finally {
                            readLock.unlock();
                        }

                        if (bytesRead <= 0) continue;

                        // ---------- WRITE ----------
                        writeLock.lock();
                        try {
                            out.seek(offset);
                            out.write(buffer, 0, bytesRead);
                        } finally {
                            writeLock.unlock();
                        }
                    }

                } catch (Exception e) {
                    e.printStackTrace();
                }

            }, "copy-worker-" + t);

            workers[t].start();
        }

        // ---------- wait for all ----------
        for (Thread w : workers) {
            w.join();
        }

        in.close();
        out.close();

        System.out.println("Copy completed.");
    }

    // ------------------ demo ------------------

    public static void main(String[] args) throws Exception {
        copy("source.dat", "dest.dat", 4);
    }
}

