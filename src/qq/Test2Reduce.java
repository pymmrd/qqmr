package qq;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Test2Reduce extends Reducer<Text, Text, Text, Text>{

	@Override
	protected void reduce(Text key, Iterable<Text> i,  Context arg2)
			throws IOException, InterruptedException {
		 Set<String> set = new HashSet<String>();
		 for(Text t:i) {
			 set.add(t.toString());
		 }
		 if(set.size() > 1) {
			 for(String name:set) {
				 for (String other:set) {
					 if(!name.equals(other)){
						 arg2.write(new Text(name), new Text(other));
					 }
				 }
			 }
		 }
	}
	
	

}
