package org.saliya.memmapcomm;

import net.openhft.lang.io.ByteBufferBytes;
import net.openhft.lang.io.Bytes;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.stream.IntStream;

public class Program {
    public static void main(String[] args) throws InterruptedException {
        String tmpDir = args[0];
        String pointsFile = args[1];
        int numPoints = Integer.parseInt(args[2]);
        int worldProcRank = Integer.parseInt(args[3]);
        int worldProcCount = Integer.parseInt(args[4]);


        double[] points = readPoints(pointsFile, numPoints);
        // Alright, everyone knows what points the writers will write by now as they already have full set of points array.

        String pointsFileName = "points.bin";
        String lockFileName = "lock.bin";
        try (FileChannel fc = FileChannel.open(Paths.get(tmpDir,
                                                         pointsFileName),
                                               StandardOpenOption.CREATE,
                                               StandardOpenOption.WRITE,
                                               StandardOpenOption.READ);
            FileChannel lc = FileChannel.open(Paths.get(tmpDir, lockFileName),
                                              StandardOpenOption.CREATE,
                                              StandardOpenOption.WRITE,
                                              StandardOpenOption.READ)) {

            // Fancy load balancing of points
            // The idea is to equally divide points (q per rank) among ranks,
            // but if any leftover points (r) are given to the first r ranks - one each,
            // so first r ranks will end up getting q+1 points and the rest
            // will get q points
            int q = numPoints / worldProcCount;
            int r = numPoints % worldProcCount;
            int mySize = worldProcRank < r ? q+1 : q;
            int myOffset = worldProcRank < r ? worldProcRank*(q+1) : worldProcRank*q + r;
            int myOffsetInBytes = myOffset*Double.BYTES;
            int myExtentInBytes = mySize*Double.BYTES;
            int fullExtentInBytes = numPoints*Double.BYTES;

            // Reader is mapped for the full extent of the file
            Bytes
                readerBytes = ByteBufferBytes.wrap(fc.map(
                FileChannel.MapMode.READ_WRITE, 0, fullExtentInBytes));
            // Writer is a slice from reader mapping only the portion that it'll write data
            Bytes writerBytes = readerBytes.slice(myOffsetInBytes, myExtentInBytes);

            // Lock of 8 bytes. First 4 bytes for the lock
            // and next 4 bytes (integer) is for the count.
            // A lock is needed only to know when all the writers
            // are done writing, so each reader can start reading.
            // Note. writers write to non conflicting mappings, so they don't need a lock
            Bytes lockBytes = ByteBufferBytes.wrap(fc.map(
                FileChannel.MapMode.READ_WRITE, 0, 8));

            for (int i = 0; i < mySize; ++i){
                writerBytes.position(i * Double.BYTES);
                writerBytes.writeDouble(points[i + myOffset]);
            }

            lockBytes.busyLockInt(0);
            // Write I am done
            lockBytes.addAndGetInt(4, 1);
            lockBytes.unlockInt(0);
            // Now wait
            while(true){
                lockBytes.busyLockInt(0);
                int count = lockBytes.readInt(4);
                lockBytes.unlockInt(0);
                if (count == worldProcCount) break;
                Thread.sleep(5);
            }

            double[] readValues = new double[numPoints];
            for (int i = 0; i < numPoints; ++i){
                readValues[i] = readerBytes.readDouble(i*Double.BYTES);
            }

            for (int i = 0; i < numPoints; ++i){
                if (points[i] != readValues[i]){
                    System.out.println("Inconsistent rank " + worldProcRank + " " + i + " expected " + points[i] + " found " + readValues[i]);
                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static double[] readPoints(String pointsFile, int numPoints) {
        double[] points = new double[numPoints];
        try(BufferedReader reader = Files.newBufferedReader(Paths.get(pointsFile))) {
            int count = 0;
            String line = null;
            while ((line = reader.readLine()) != null){
                if (count == numPoints) break;
                points[count++] = Double.parseDouble(line.trim());
            }
            if (count != numPoints){
                throw new RuntimeException("Read " + count + "points, but expected " + numPoints + ". File " + pointsFile);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return points;
    }
}
