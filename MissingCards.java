import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class MissingCards {
    public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(" ");
            if (split.length == 2) {
                Text cardType = new Text(split[0]);
                IntWritable cardNumber = new IntWritable(Integer.parseInt(split[1]));
                context.write(cardType, cardNumber);
            }
        }
    }

    public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text cardType, Iterable<IntWritable> cardNumbers, Context context) throws IOException, InterruptedException {
            ArrayList<Integer> allCards = new ArrayList<>();
            ArrayList<Integer> presentCards = new ArrayList<>();

            // Initialize all cards from 1 to 13
            for (int i = 1; i <= 13; ++i) {
                allCards.add(i);
            }

            // Iterate through card numbers to identify present cards
            for (IntWritable cardNumber : cardNumbers) {
                presentCards.add(cardNumber.get());
            }

            // Remove present cards from the list of all cards
            allCards.removeAll(presentCards);

            // Emit missing cards
            for (Integer missingCard : allCards) {
                context.write(cardType, new IntWritable(missingCard));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        Job job = new Job(config, "MissingCards");

        job.setJarByClass(MissingCards.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
