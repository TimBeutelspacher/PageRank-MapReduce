import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class RankCalculatorReduce extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text page, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        // Key = verlinkte Page
        // Value = verlinkende Page

        Double result = 0.0;
        Double pr = 0.0;
        String linksInPage = "";

        for(Text value : values) {

            String lp = value.toString().split("\t")[0];
            String PRofLPstr = value.toString().split("\t")[1];
            String numLinksOfLPstr = value.toString().split("\t")[2];

            Double PRofLP = Double.parseDouble(PRofLPstr);
            Double numLinksOfLP = Double.parseDouble(numLinksOfLPstr);

            Double PRofP = PRofLP/numLinksOfLP;
            result += PRofP;

            if(linksInPage == ""){
                linksInPage = linksInPage + lp;
            }else{
                linksInPage = linksInPage + "," + lp;
            }
        }

        pr = (1-0.85)+0.85*result;
        context.write(page, new Text(pr + "\t" + linksInPage));
    }
}
