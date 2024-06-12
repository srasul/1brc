package dev.morling.onebrc.rasul;

public class WeatherEntryDouble {
    double min;
    double max;
    double sum;
    int count;
//    private ExecutorService executor;
//    double mean;

    public WeatherEntryDouble() {
//        this.executor = executor;
    }

    public void addVal(double val) {
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
        return min + "/" + (sum/count) + "/" + max;
    }
}
