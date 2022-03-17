import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.Arrays;
import java.util.LinkedHashMap;
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

public class Bow {
	
	private final static String[] top100Word = { "the", "be", "to", "of", "and", "a", "in", "that", "have", "i",
			 "it", "for", "not", "on", "with", "he", "as", "you", "do", "at",
			 "this", "but", "his", "by", "from", "they", "we", "say", "her", "she",
			 "or", "an", "will", "my", "one", "all", "would", "there", "their", "what",
			 "so", "up", "out", "if", "about", "who", "get", "which", "go", "me",
			 "when", "make", "can", "like", "time", "no", "just", "him", "know", "take",
			 "people", "into", "year", "your", "good", "some", "could", "them", "see", "other",
			 "than", "then", "now", "look", "only", "come", "its", "over", "think", "also",
			 "back", "after", "use", "two", "how", "our", "work", "first", "well", "way",
			 "even", "new", "want", "because", "any", "these", "give", "day", "most", "us" };
	private static LinkedHashMap<String,String> Bow = new LinkedHashMap<String,String>();  
	private static BufferedReader BowBR;
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
        // map words which in the top100 list
        if(Arrays.asList(top100Word).contains(s2)){
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
	String[] BowVector = new String[10];
    // Process ten files respectively
    for (int i=0;i<10;i++) {
    	for(int j=0;j<100;j++) {
    		Bow.put(top100Word[j],"0");
    	}
    	Job job = Job.getInstance(conf, "Bow");
        job.setJarByClass(Bow.class);
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
        
		// Generate BoW
		BowBR = new BufferedReader(new InputStreamReader(Fsy.open(new Path(args[1]+ "/" + filename+"/part-r-00000"))));
		String str = null;
		while ((str = BowBR.readLine()) != null) {
        	String[] cst = str.split("\\s+");
        	Bow.put(cst[0], cst[1]);
        }
		BowBR.close();
		String Outputtext = filename + ".txt" + " ";
		for (String key: Bow.keySet()) {
			Outputtext += Bow.get(key) + ",";
        }
		BowVector[i]=Outputtext;
	}
    
    // write result into file outputBow.txt
 	OutputStream Ost = Fsy.create(new Path(args[1] + "/outputBow.txt"),true);
 	buffW = new BufferedWriter(new OutputStreamWriter(Ost));
 	for(int k=0;k<BowVector.length;k++){
 		buffW.write(BowVector[k]+'\n'+'\n');
 		System.out.println(BowVector[k]);
 		System.out.println("\n");
 	}
 	buffW.close();
 	Ost.close();
    Fsy.close();
  }
}
