import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class WikiPageLinksReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        // key= Verlinkende Seite
        // values = verlinkte Seite
        
        String result = "";

        for (Text value : values) {

            if(!value.toString().isEmpty()){

                if(result == ""){
                    result = result + value.toString();
                }
                else{
                    result = result + "," + value.toString();
                }
            }
        }


        context.write(key, new Text("1.0\t" + result));
    }
}
