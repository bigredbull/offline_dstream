package pl.edu.agh.student.offlinedstream;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupComparator extends WritableComparator {

    protected GroupComparator() {
        super(CoordsTimestampTuple.class, true);
    }

    @Override
    public int compare(WritableComparable key1, WritableComparable key2) {
        CoordsTimestampTuple tuple1 = (CoordsTimestampTuple) key1;
        CoordsTimestampTuple tuple2 = (CoordsTimestampTuple) key2;

        return tuple1.getCoords().compareTo(tuple2.getCoords());
    }

}
