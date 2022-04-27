import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RankCalculatorMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String key1 = value.toString().split("\t")[0];
        String prOfP = value.toString().split("\t")[1];
        String linksInP = value.toString().split("\t")[2];

        for (String page : linksInP.split(",")){
            context.write(new Text(page), new Text(key1 +"\t" + prOfP + "\t" + linksInP.split(",").length));
        }
    }
}
