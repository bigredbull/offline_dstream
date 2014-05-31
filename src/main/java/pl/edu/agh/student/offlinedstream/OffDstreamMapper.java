package pl.edu.agh.student.offlinedstream;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OffDstreamMapper extends Mapper<Object, Text, pl.edu.agh.student.offlinedstream.Coordinates, pl.edu.agh.student.offlinedstream.Grid> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        context.write(new pl.edu.agh.student.offlinedstream.Coordinates(new ArrayList<Integer>()), new pl.edu.agh.student.offlinedstream.Grid());
    }
}