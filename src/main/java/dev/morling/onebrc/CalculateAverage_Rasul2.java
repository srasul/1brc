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
import dev.morling.onebrc.rasul.WeatherEntryDouble;

public class CalculateAverage_Rasul2 {

    private class MyCallable implements Callable<Map<String, WeatherEntryDouble>> {
        MappedByteBuffer mappedByteBuffer;
        int size;

        MyCallable(MappedByteBuffer mappedByteBuffer, int size) {
            this.mappedByteBuffer = mappedByteBuffer;
            this.size = size;

        }

        @Override
        public Map<String, WeatherEntryDouble> call() throws Exception {
            return readBufferContents(size, mappedByteBuffer);
        }
    }

    private static final String FILE = "./data/measurements.txt";

    public static void main(String[] args) throws IOException {
        new CalculateAverage_Rasul2()._main(args);
    }

    public void _main(String[] args) throws IOException {
        Long before = System.currentTimeMillis();

        RandomAccessFile file = new RandomAccessFile(FILE, "r");
        FileChannel channel = file.getChannel();

        // Read file into mapped buffer
        System.out.println("file channel size: "+ channel.size());


        MappedByteBuffer mbb =
                channel.map(MapMode.READ_ONLY,
                            0,          // position
                            channel.size());

        int chunks = 8;

        System.out.println("total "+ channel.size());

        long chunkSize = channel.size() /chunks;

        System.out.println("chunk size: "+ chunkSize);

        List<Long> positions = new LinkedList<>();
        positions.add(0L);
        for(int i = 0; i < chunks; i++) {

            System.out.println( "will try to read starting at: " + (chunkSize * (i)) + " to " +  (chunkSize * (i+1)) );
            long end = (chunkSize * (i+1));
            char atEnd = (char)mbb.get((int)end);

            int toAdd = 1;
            if(atEnd != '\n') {
                while ((char)mbb.get((int)end + toAdd) != '\n') {
                    toAdd++;

                }
                System.out.println("will read additional " + toAdd);

                positions.add((chunkSize * (i+1) + toAdd));

                System.out.println("char at this additional pos: " + (char)mbb.get((int) (chunkSize * (i + 1) + toAdd)));

            }else {
                positions.add((chunkSize * (i+1)));
            }

//            System.out.println("char at end: "+ atEnd);
        }

//        positions.add(channel.size() -1);

        System.out.println(positions);

        ExecutorService executorService = Executors.newCachedThreadPool();
        List<Future<Map<String, WeatherEntryDouble>>> futures = new ArrayList<>();
        for(int i = 0; i < (chunks); i++) {
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

        futures.forEach(mapFuture -> {
            try {
                mapFuture.get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        });






//        Map<String, WeatherEntry> allVals = new HashMap<>();
//
//        readBufferContents(channel, mbb, allVals);
//
        Long after = System.currentTimeMillis();

//        System.out.println(allVals.size());

        System.out.println("took: " + (after - before));

//        System.out.println(allVals);

        channel.close();
        file.close();
    }

    private static Map<String, WeatherEntryDouble> readBufferContents(int size, MappedByteBuffer mbb)
            throws IOException {
        Map<String, WeatherEntryDouble> allVals = new HashMap<>();
        String city = null;
        String temp;
        LinkedList<Character> chars = new LinkedList<>();
//        long fileSize = channel.size();
        for (int i = 0; i < size; i++) {
            char c = (char) mbb.get();
            if(c == ';') {
                char[] charArray = new char[chars.size()];
                for(int j = 0; j < chars.size(); j++) {
                    charArray[j] = chars.get(j);
                }
                city = new String(charArray);
                chars.clear();

//                System.out.println(city);
            } else if(c == '\n' ) {
                char[] charArray = new char[chars.size()];
                for(int j = 0; j < chars.size(); j++) {
                    charArray[j] = chars.get(j);
                }
                temp = new String(charArray);
                chars.clear();

                if(!allVals.containsKey(city)) {
                    allVals.put(city, new WeatherEntryDouble());
                }
                if(temp.length() > 0)
                    allVals.get(city).addVal(Double.parseDouble(temp));

//                System.out.println(temp);
            }else {
                chars.add(c);
            }
        }

        return allVals;
    }
}
