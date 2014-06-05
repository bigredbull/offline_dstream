package pl.edu.agh.student.offlinedstream;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {

    protected KeyComparator() {
        super(CoordsTimestampTuple.class, true);
    }

    @Override
    public int compare(WritableComparable key1, WritableComparable key2) {
        CoordsTimestampTuple tuple1 = (CoordsTimestampTuple) key1;
        CoordsTimestampTuple tuple2 = (CoordsTimestampTuple) key2;

        return tuple1.compareTo(tuple2);
    }

}
