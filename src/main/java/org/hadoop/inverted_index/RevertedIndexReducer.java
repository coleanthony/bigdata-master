package org.hadoop.inverted_index;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RevertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text K, Iterable<Text> V, Context context)
            throws IOException, InterruptedException {
        String str = "";
        for(Text t:V){
            str = "(" + t.toString()+")" + str;
        }
        context.write(K, new Text(str));
    }
}

