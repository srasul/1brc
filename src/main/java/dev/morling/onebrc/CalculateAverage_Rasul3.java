package dev.morling.onebrc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;


import dev.morling.onebrc.rasul.WeatherEntry;


public class CalculateAverage_Rasul3 {

    private static final String FILE = "./data/measurements.txt";

    private class MyCallable implements Callable<Map<String, WeatherEntry>> {
        MappedByteBuffer mappedByteBuffer;
        int size;

        MyCallable(MappedByteBuffer mappedByteBuffer, int size) {
            this.mappedByteBuffer = mappedByteBuffer;
            this.size = size;

        }

        @Override
        public Map<String, WeatherEntry> call() throws Exception {
            return readBufferContents(size, mappedByteBuffer);
        }
    }

    private static Map<String, WeatherEntry> readBufferContents(int size, MappedByteBuffer mbb)
            throws IOException {
        Map<String, WeatherEntry> allVals = new HashMap<>();
        String city = null;
        String temp = "";
        LinkedList<Character> chars = new LinkedList<>();
        for (int i = 0; i < size; i++) {
            char c = (char) mbb.get();
            if(c == ';') {

                char[] charArray = new char[chars.size()];
                for(int j = 0; j < chars.size(); j++) {
                    charArray[j] = chars.get(j);
                }
                city = new String(charArray);
                chars.clear();
            } else if(c == '\n' ) {
                char[] charArray = new char[chars.size()];
                for(int j = 0; j < chars.size(); j++) {
                    charArray[j] = chars.get(j);
                }
                temp = new String(charArray);
                chars.clear();

                if(!allVals.containsKey(city)) {
                    allVals.put(city, new WeatherEntry());
                }
                if(temp.length() > 0)
                    allVals.get(city).addVal(Integer.parseInt(temp));
            }else {
                if(c != '.') chars.add(c);
            }
        }

        return allVals;
    }

    public static void main(String[] args) throws IOException {
        new CalculateAverage_Rasul3()._main(args);
    }

    public void _main(String[] args) throws IOException {
        // input file is about 13GB and has
        // 13_795_336_148 chars

        long start = System.currentTimeMillis();

        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        FileChannel channel = file.getChannel();

        long totalSize = channel.size();

        // Read file into mapped buffer
        System.out.println("file channel size: "+ totalSize);

        int CHUNKS = 12; // min theoretically is 6, but it will be more than that

        long chunkSize = totalSize /CHUNKS;

        System.out.println("chunkSize < Integer.MAX_VALUE: "+ (chunkSize < Integer.MAX_VALUE));
        assert chunkSize < Integer.MAX_VALUE;

        System.out.println(13_795_336_148L/Integer.MAX_VALUE);

        List<Long> positions = new LinkedList<>();
        positions.add(0L);
        for(int i = 0; i < (CHUNKS-1); i++) {
            System.out.println( "chunk: " + i + " will try to read starting at: " + (chunkSize * (i)) + " to " +  (chunkSize * (i+1)) );
//            long end = (chunkSize * (i+1));
//            char atEnd = (char)mbb.get((int)end);

            MappedByteBuffer mbb =
                    channel.map(MapMode.READ_ONLY,
                                (chunkSize * (i)),          // position
                                (chunkSize * (i+1) - (chunkSize * (i)))); // size

            // from the end of mbb backwards, find the next '\n' char
            int cnt = 0;
            for(int j = mbb.capacity()-1; j >= 0; j--) {
                char charAt = (char) mbb.get(j);
                if(charAt == '\n') {
                    positions.add((chunkSize * (i+1) -cnt));
                    System.out.println("at " + j + " char is: " + charAt);
                    break;
                }else {
                    cnt++;
                }
            }

        }

        positions.add(totalSize);

        System.out.println(positions);

        ExecutorService executorService = Executors.newCachedThreadPool();

        List<Future<Map<String, WeatherEntry>>> futures = new ArrayList<>();
        for(int i = 0; i < (CHUNKS); i++) {
            System.out.println(positions.get(i));
            System.out.println(positions.get(i+1));
//            System.out.println("");

            RandomAccessFile file1 = new RandomAccessFile(FILE, "r");
            FileChannel channel1 = file1.getChannel();

            System.out.println("chunk  " + i);
            System.out.println("file channel size: "+ channel1.size());

            int size = (int) (positions.get(i + 1) - positions.get(i));
            MappedByteBuffer mappedByteBuffer =
                    channel1.map(MapMode.READ_ONLY,
                                 positions.get(i),          // position
                                 size);



            MyCallable myCallable = new MyCallable(mappedByteBuffer, size);

            futures.add(executorService.submit(myCallable));
            System.out.println("");

        }

        for(int i = 0; i < futures.size(); i++ ){
            try {
                System.out.println(futures.get(i).get());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
            System.out.println("\nfinished chunk: " + i + "\n\n");
        }

//        futures.forEach(mapFuture -> {
//            try {
//
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            } catch (ExecutionException e) {
//                throw new RuntimeException(e);
//            }
//        });

        System.out.println("Total time: " + (System.currentTimeMillis() - start));

        channel.close();
        file.close();
    }
}
