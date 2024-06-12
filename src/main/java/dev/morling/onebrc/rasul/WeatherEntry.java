package dev.morling.onebrc.rasul;

public class WeatherEntry {
    int min;
    int max;
    int sum;
    int count;
//    private ExecutorService executor;
//    double mean;

    public WeatherEntry() {
//        this.executor = executor;
    }

    public void addVal(int val) {
        if (val < min) {
            min = val;
        }
        if (val > max) {
            max = val;
        }
        sum += val;
        count++;


//        mean = sum / count;
    }

//    public void isDone() throws InterruptedException {
//        executor.shutdown();
//        executor.awaitTermination(4, java.util.concurrent.TimeUnit.MINUTES);
//    }

    @Override
    public String toString() {
        double mean = sum;
        mean = mean/(count*10);
        return min + "/" + mean + "/" + max;
    }
}
