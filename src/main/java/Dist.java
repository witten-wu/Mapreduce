import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Dist {
	
	private static Map<String,Integer> Dist = new LinkedHashMap<String,Integer>();  
	private static BufferedReader DistBR;
	private static BufferedWriter buffW;
	public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text word2 = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        String s=word.toString();
        // filter special symbols
        String regEx = "[`~☆★!@#$%^&*()+=|{}':;,\\[\\]》·.<>/?~！@#￥%……（）——+|{}【】‘；：”“’。\"，\\-、？]";
        String s1 = Pattern.compile(regEx).matcher(s).replaceAll("").trim();
        String s2 = s1.toLowerCase();
        // map words which has "ex"
        if(s2.length() < 2) {
			continue;
		}
		if(s2.substring(0, 2).equals("ex")) {
			word2=new Text(s2);
            context.write(word2, one);
		}
      }
    }
  }

	public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
	String filename = "";
	FileSystem Fsy = FileSystem.get(conf);
	String[] DistVector = new String[10];
    // Process ten files respectively
    for (int i=0;i<10;i++) {
    	Map<String,Integer> DistTmp = new LinkedHashMap<String,Integer>(); 
    	Job job = Job.getInstance(conf, "Dist");
        job.setJarByClass(Dist.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);	
        // input file name
        if (i < 9) {
        	filename = "file0"+String.valueOf(i+1);}
        else {
        	filename = "file10";}
        FileInputFormat.addInputPath(job, new Path(args[0]+ "/" + filename + ".txt"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+ "/" + filename));
        job.waitForCompletion(true);
        
		// Generate Dist
		DistBR = new BufferedReader(new InputStreamReader(Fsy.open(new Path(args[1]+ "/" + filename+"/part-r-00000"))));
		String str = null;
		while ((str = DistBR.readLine()) != null) {
        	String[] cst = str.split("\\s+");
        	DistTmp.put(cst[0], Integer.parseInt(cst[1]));
        	
        	// Add to HashMap which store the total
        	if(Dist.containsKey(cst[0])){
        		Dist.put(cst[0],Dist.get(cst[0])+Integer.parseInt(cst[1]));
    		}
        	else{
    			Dist.put(cst[0],Integer.parseInt(cst[1]));
    		}
        }
		
		DistBR.close();
		
		// transform to list for sorting
		List<Map.Entry<String, Integer>> list = new ArrayList<Map.Entry<String, Integer>>(DistTmp.entrySet());
		Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
			public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
				return o2.getValue().compareTo(o1.getValue());
	        }
	    });
		String Outputtext = filename + ".txt" + " ";
		for (int m = 0; m < list.size(); m++) {
		    Outputtext += list.get(m).getKey() + "," + list.get(m).getValue() + ",";
		}
		
		DistVector[i]=Outputtext;
	}
    
    // Generate Total
    // Sort
	List<Map.Entry<String, Integer>> listTmp = new ArrayList<Map.Entry<String, Integer>>(Dist.entrySet());
	Collections.sort(listTmp, new Comparator<Map.Entry<String, Integer>>() {
		public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
			return o2.getValue().compareTo(o1.getValue());
        }
    });
	String Outputtext2 = "Total ";
	for (int h = 0; h < listTmp.size(); h++) {
	    Outputtext2 += listTmp.get(h).getKey() + "," + listTmp.get(h).getValue() + ",";
	}
    
    // write result into file outputDist.txt
 	OutputStream Ost = Fsy.create(new Path(args[1] + "/outputDist.txt"),true);
 	buffW = new BufferedWriter(new OutputStreamWriter(Ost));
 	for(int k=0;k<DistVector.length;k++){
 		buffW.write(DistVector[k]+'\n'+'\n');
 		System.out.println(DistVector[k]);
 		System.out.println("\n");
 	}
 	buffW.write(Outputtext2);
 	System.out.println(Outputtext2);
 	buffW.close();
 	Ost.close();
    Fsy.close();
  }
}
