import bolt.*;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
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
        String path = Constantes.path;

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
                path = args[0];
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
        FileWriterBolt fileWriterBolt = new FileWriterBolt(path);


        builder.setSpout("spoutLeerTwitter", spout,1);
        builder.setBolt("urlExtractor", urlExtractor,1).shuffleGrouping("spoutLeerTwitter");
        builder.setBolt("getImage", getImage,3).shuffleGrouping("urlExtractor");
        builder.setBolt("classifier", classifier,6).shuffleGrouping("getImage");
        builder.setBolt("escribir",fileWriterBolt,1).shuffleGrouping("classifier");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(12);

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
