package pl.edu.agh.student.offlinedstream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Iterator;
import java.util.Locale;

public class OffDstreamReducer extends Reducer<CoordsTimestampTuple, IntWritable, Coordinates, Text> {

    public static final double LAMBDA = 0.8;
    public static final DecimalFormatSymbols SYMBOLS = new DecimalFormatSymbols(Locale.ENGLISH);

    public void reduce(CoordsTimestampTuple tuple, Iterable<IntWritable> timestamps, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int numberOfRecords = conf.getInt("number.of.records", 0);

        int lastTimestamp = 0;
        double gridDensity = 1.0;

        Iterator<IntWritable> it = timestamps.iterator();

        if (it.hasNext()) {
            lastTimestamp = it.next().get();
        }

        while (it.hasNext()) {
            int timestamp = it.next().get();
            gridDensity = updatedGridDensity(gridDensity, lastTimestamp, timestamp);
            lastTimestamp = timestamp;
        }

        if (lastTimestamp < numberOfRecords) {
            gridDensity = updatedGridDensity(gridDensity, lastTimestamp, numberOfRecords);
        }

        String outputValue = new DecimalFormat("#0.00000", SYMBOLS).format(gridDensity);
        context.write(tuple.getCoords(), new Text(outputValue));
    }

    private double updatedGridDensity(double currentDensity, int lastTimestamp, int currentTimestamp) {
        int timeDelta = currentTimestamp - lastTimestamp;
        return Math.pow(LAMBDA, timeDelta) * currentDensity + 1;
    }

}
