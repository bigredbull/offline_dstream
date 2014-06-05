package pl.edu.agh.student.offlinedstream;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OffDstreamPartitioner extends Partitioner<CoordsTimestampTuple, IntWritable> {

    @Override
    public int getPartition(CoordsTimestampTuple key, IntWritable value, int numPartitions) {
        return key.getCoords().hashCode() % numPartitions;
    }

}
