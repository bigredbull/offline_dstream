package pl.edu.agh.student.offlinedstream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class OffDstreamMapper extends Mapper<LongWritable, Text, CoordsTimestampTuple, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int gridRange = conf.getInt("grid.range", 1);

        IntWritable timestamp = new IntWritable();
        StringTokenizer tokenizer = new StringTokenizer(value.toString());
        List<Integer> coordsData = new ArrayList<>();

        if (tokenizer.hasMoreTokens()) {
            timestamp.set(Integer.parseInt(tokenizer.nextToken()));
        }

        while (tokenizer.hasMoreTokens()) {
            int recordCoord = Integer.parseInt(tokenizer.nextToken());
            int gridCoord = calculateGridCoord(recordCoord, gridRange);
            coordsData.add(gridCoord);
        }

        Coordinates coords = new Coordinates(coordsData);
        CoordsTimestampTuple tuple = new CoordsTimestampTuple(coords, timestamp);

        context.write(tuple, timestamp);
    }

    private int calculateGridCoord(int recordCoord, int gridRange) {
        return (int) Math.floor(recordCoord / gridRange);
    }

}
