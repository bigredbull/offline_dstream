package pl.edu.agh.student.offlinedstream;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OffDstreamMapper extends Mapper<Object, Text, Coordinates, Grid> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new Coordinates(new ArrayList<Integer>()), new Grid());
    }
}