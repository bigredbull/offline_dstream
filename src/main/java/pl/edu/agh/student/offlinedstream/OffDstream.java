package pl.edu.agh.student.offlinedstream;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OffDstream {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Path inputPath = new Path(args[0]);
        Path tmpPath = new Path("temp");
        Path outputDir = new Path(args[1]);

        // Create configuration
//        Configuration conf = new Configuration(true);

        ControlledJob cJob1 = new ControlledJob(new Configuration());
        cJob1.setJobName("First job!");
        Job job1 = cJob1.getJob();

//        job1.setJarByClass(OffDstreamMapper.class);
        job1.setNumReduceTasks(1);

        job1.setOutputKeyClass(Coordinates.class); //output for mapper
        job1.setOutputValueClass(Grid.class); //output for mapper

        job1.setMapperClass(OffDstreamMapper.class);
        job1.setReducerClass(GridReducer.class);

        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, tmpPath);

        ControlledJob cJob2 = new ControlledJob(new Configuration());
        cJob2.setJobName("Second job!");
        Job job2 = cJob2.getJob();
        cJob2.addDependingJob(cJob1);

//        job2.setJarByClass(FilterMapper.class);
        job2.setNumReduceTasks(1);

        job2.setOutputKeyClass(Text.class); //output for mapper
        job2.setOutputValueClass(Text.class); //output for mapper

        job2.setMapperClass(FilterMapper.class);
        job2.setReducerClass(ClusterReducer.class);

        FileInputFormat.setInputPaths(job2, tmpPath);
        FileOutputFormat.setOutputPath(job2, outputDir);

        JobControl control = new JobControl("Pierwszy chaining");

        // Delete output if exists
        FileSystem hdfs = FileSystem.get(new Configuration());
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);
        if (hdfs.exists(tmpPath))
            hdfs.delete(tmpPath, true);

        control.addJob(cJob1);
        control.addJob(cJob2);

        control.run();
//
//        Thread jobRunnerThread = new Thread(new JobRunner(control));
//        jobRunnerThread.start();
//
//        while (!control.allFinished()) {
//            System.out.println("Still running...");
//            Thread.sleep(1000);
//        }
//        System.out.println("Done");
//        control.stop();

    }

}