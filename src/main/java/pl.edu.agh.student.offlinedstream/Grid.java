package pl.edu.agh.student.offlinedstream;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

public class Grid implements Writable, Serializable {
    public double density;
    public int refreshmentTime;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeDouble(density);
        dataOutput.writeInt(refreshmentTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        density = dataInput.readDouble();
        refreshmentTime = dataInput.readInt();
    }
}
