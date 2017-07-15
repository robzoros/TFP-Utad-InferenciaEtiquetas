import bolt.*;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.*;
import org.apache.storm.hdfs.bolt.rotation.*;
import org.apache.storm.hdfs.bolt.sync.*;
import org.apache.storm.topology.TopologyBuilder;
import spout.TwitterSpout;
import twitter4j.FilterQuery;
import util.Constantes;


public class TwitterTopologia {


    public static void main(String[] args) throws Exception {
        String consumerKey ;
        String consumerSecret;
        String accessToken;
        String accessTokenSecret;
        String directorio = Constantes.directorio;

        /* *************** SETUP ****************/
        consumerKey = Constantes.TweeterCredentials.consumerKey;
        consumerSecret = Constantes.TweeterCredentials.consumerSecret;
        accessToken = Constantes.TweeterCredentials.accessToken;
        accessTokenSecret = Constantes.TweeterCredentials.accessTokenSecret;

        if (args!=null) {
            // If credentials are provided as commandline arguments
            if (args.length==4) {
                consumerKey = args[0];
                consumerSecret = args[1];
                accessToken = args[2];
                accessTokenSecret = args[3];
            }
            else if (args.length==1) {
                directorio = args[0];
            }
        }
        /* ***************       ****************/
        TopologyBuilder builder = new TopologyBuilder();

        FilterQuery tweetFilterQuery = new FilterQuery();
        tweetFilterQuery.track(new String[]{"photo"});
        // See https://github.com/kantega/storm-twitter-workshop/wiki/Basic-Twitter-stream-reading-using-Twitter4j


        TwitterSpout spout = new TwitterSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret, tweetFilterQuery);
        TweetExtractUrlBolt urlExtractor = new TweetExtractUrlBolt();
        GetImageBolt getImage = new GetImageBolt();
        TensorFlowBolt classifier = new TensorFlowBolt();
        FileWriterBolt fileWriterBolt = new FileWriterBolt(directorio);

        /*
        // Creamos un bolt de HDFS usando la clase HDFSBolt de Storm
        // use "|" instead of "," for field delimiter
        RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter("|");

        // sync the filesystem after every 1k tuples
        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        // rotate files when they reach 5MB
        FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(5.0f, FileSizeRotationPolicy.Units.MB);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath(Constantes.path);

        HdfsBolt hdfsbolt = new HdfsBolt()
                .withFsUrl(directorio)
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);
        */

        builder.setSpout("spoutLeerTwitter", spout,1);
        builder.setBolt("urlExtractor", urlExtractor,1).shuffleGrouping("spoutLeerTwitter");
        builder.setBolt("getImage", getImage,3).shuffleGrouping("urlExtractor");
        builder.setBolt("classifier", classifier,3).shuffleGrouping("getImage");
        //builder.setBolt("escribir", hdfsbolt,1).shuffleGrouping("classifier");
        builder.setBolt("escribir",fileWriterBolt,1).shuffleGrouping("classifier");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(9);

        StormSubmitter.submitTopology(Constantes.topologia, conf, builder.createTopology());


        /*
        conf.setMaxTaskParallelism(3);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("twitter-fun", conf, builder.createTopology());

        Thread.sleep(460000);

        cluster.shutdown();
        }*/
    }
}
