package pl.edu.agh.student.offlinedstream;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OffDstreamMapper extends Mapper<Object, Text, Coordinates, Grid> {

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        ArrayList<Integer> tab = new ArrayList<>();
        for(int i = 0; i < 5; ++i)
            tab.add(i * 2);
        context.write(new Coordinates(tab), new Grid(3.14, 15)); // lista sie powieksza dla kazdej z linijek z inputu :O
    }
}
