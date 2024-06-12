package dev.morling.onebrc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import dev.morling.onebrc.rasul.WeatherEntry;
import dev.morling.onebrc.rasul.WeatherEntryDouble;

public class CalculateAverage_Rasul1 {
    private static final String FILE = "./data/measurements.txt";
    public static final int LOG_INTERVAL = 10000000;

    public static void main(String[] args) throws IOException {

        Long before = System.currentTimeMillis();

        Map<String, WeatherEntryDouble> allVals = new HashMap<>();



        final AtomicLong cnt = new AtomicLong(0);
        final AtomicReference<Instant> timestamp = new AtomicReference<>(Instant.now());
        Files.lines(Path.of(FILE)).map(s -> s.split(";")).forEach(strings -> {

            if(!allVals.containsKey(strings[0])) {
                allVals.put(strings[0], new WeatherEntryDouble());
            }

            allVals.get(strings[0]).addVal(Double.parseDouble(strings[1]));


            if(cnt.get() % LOG_INTERVAL == 0) {
                System.out.println(cnt);

                Duration duration = Duration.between(timestamp.get(), Instant.now());

                System.out.println(LOG_INTERVAL + " lines took : " + duration.toMillis() + " ms");

                timestamp.set(Instant.now());
            }
            cnt.incrementAndGet();
        });

//        allVals.values().forEach(weatherEntry -> {
//            try {
//                weatherEntry.isDone();
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        });

        Long after = System.currentTimeMillis();

        System.out.println(allVals.size());

        System.out.println("took: " + (after - before));

        System.out.println(allVals);

    }
}
