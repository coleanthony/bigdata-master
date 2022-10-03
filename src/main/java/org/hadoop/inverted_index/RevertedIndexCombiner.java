package org.hadoop.inverted_index;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RevertedIndexCombiner extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text K, Iterable<Text> V, Context context)
            throws IOException, InterruptedException {
        int total = 0;
        for(Text v:V)
            total = total + Integer.parseInt(v.toString());
        String data = K.toString();
        int index = data.indexOf(":");
        String word = data.substring(0, index);
        String fileName = data.substring(index+1);
        context.write(new Text(word), new Text(fileName+":"+total));
    }
}

