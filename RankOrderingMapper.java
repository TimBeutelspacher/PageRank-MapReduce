import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankOrderingMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Key = verlinkende Page
        // Value = PR + verlinkte Page
        

        String key1 = value.toString().split("\t")[0];
        String prOfPstr = value.toString().split("\t")[1];
        Float prOfP = Float.parseFloat(prOfPstr);

        context.write(new FloatWritable(prOfP), new Text(key1));
    }
}
