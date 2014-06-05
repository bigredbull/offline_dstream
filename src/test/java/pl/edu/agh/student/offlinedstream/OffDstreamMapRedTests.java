package pl.edu.agh.student.offlinedstream;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OffDstreamMapRedTests {

    MapDriver<LongWritable, Text, CoordsTimestampTuple, IntWritable> mapDriver;
    ReduceDriver<CoordsTimestampTuple, IntWritable, Coordinates, Text> reduceDriver;

    @Before
    public void setUp() {
        OffDstreamMapper mapper = new OffDstreamMapper();
        OffDstreamReducer reducer = new OffDstreamReducer();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);

        Configuration mapperConf = mapDriver.getConfiguration();
        mapperConf.setInt("grid.range", 300);
        Configuration reducerConf = reduceDriver.getConfiguration();
        reducerConf.setInt("number.of.records", 31);
    }

    @Test
    public void testMapperFor2DSpace() throws IOException {
        // Input
        LongWritable inKey = new LongWritable();
        Text inValue = new Text("11 150 350");
        mapDriver.withInput(inKey, inValue);

        // Output
        Coordinates coords = new Coordinates(ImmutableList.of(0, 1));
        IntWritable timestamp = new IntWritable(11);
        CoordsTimestampTuple outKey = new CoordsTimestampTuple(coords, timestamp);
        IntWritable outValue = new IntWritable(11);
        mapDriver.withOutput(outKey, outValue);

        mapDriver.runTest();
    }

    @Test
    public void testMapperFor3DSpace() throws IOException {
        // Input
        LongWritable inKey = new LongWritable();
        Text inValue = new Text("4 310 50 650");
        mapDriver.withInput(inKey, inValue);

        // Output
        Coordinates coords = new Coordinates(ImmutableList.of(1, 0, 2));
        IntWritable timestamp = new IntWritable(4);
        CoordsTimestampTuple outKey = new CoordsTimestampTuple(coords, timestamp);
        IntWritable outValue = new IntWritable(4);
        mapDriver.withOutput(outKey, outValue);

        mapDriver.runTest();
    }

    @Test(expected = NumberFormatException.class)
    public void testMapperForInvalidInput() throws IOException {
        // Input
        LongWritable inKey = new LongWritable();
        Text inValue = new Text("invalid input");
        mapDriver.withInput(inKey, inValue);

        mapDriver.runTest();
    }

    @Test
    public void testReducerForSingleTimestamp() throws IOException {
        // Input
        Coordinates coords = new Coordinates(ImmutableList.of(4, 4));
        IntWritable timestamp = new IntWritable(31);
        CoordsTimestampTuple inKey = new CoordsTimestampTuple(coords, timestamp);

        List<IntWritable> inValues = new ArrayList<>();
        inValues.add(new IntWritable(31));

        reduceDriver.withInput(inKey, inValues);

        // Output
        Coordinates outKey = new Coordinates(ImmutableList.of(4, 4));
        Text outValue = new Text("1.00000");
        reduceDriver.withOutput(outKey, outValue);

        reduceDriver.runTest();
    }

    @Test
    public void testReducerForMultipleTimestamps() throws IOException {
        // Input
        Coordinates coords = new Coordinates(ImmutableList.of(2, 3));
        IntWritable timestamp = new IntWritable(4);
        CoordsTimestampTuple inKey = new CoordsTimestampTuple(coords, timestamp);

        List<IntWritable> inValues = new ArrayList<>();
        inValues.add(new IntWritable(4));
        inValues.add(new IntWritable(15));
        inValues.add(new IntWritable(31));

        reduceDriver.withInput(inKey, inValues);

        // Output
        Coordinates outKey = new Coordinates(ImmutableList.of(2, 3));
        Text outValue = new Text("1.03057");
        reduceDriver.withOutput(outKey, outValue);

        reduceDriver.runTest();
    }

}
