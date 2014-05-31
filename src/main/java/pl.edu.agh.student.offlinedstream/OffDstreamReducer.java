package pl.edu.agh.student.offlinedstream;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OffDstreamReducer extends Reducer<Coordinates, Grid, IntWritable, Text> {

    public void reduce(Coordinates coords, Iterable<Grid> grids, Context context) throws IOException, InterruptedException {
        context.write(new IntWritable(1), new Text("Hello!"));
    }
}