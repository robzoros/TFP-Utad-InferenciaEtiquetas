package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.Status;
import twitter4j.*;

import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 * User: qadeer
 * Date: 06.09.13
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */
public class TweetExtractUrlBolt extends BaseRichBolt {
    private OutputCollector _collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;

    }

    @Override
    public void execute(Tuple tuple) {
        Status tweet = (Status) tuple.getValueByField("message");
        String foto_url = "";

        // Los thumbnails de video salen solo en MediaEntity
        MediaEntity[] media = tweet.getMediaEntities();
        for (MediaEntity photo : media) {
            String tipo = photo.getType();
            if (tipo.equals("photo")) {
                foto_url = photo.getMediaURLHttps();
                if (foto_url.contains("tweet_video_thumb"))
                    _collector.emit(new Values(foto_url));
            }
        }
        
        // Hay fotos que se guardan en esta parte del tweet y no están en 
        // Media entities
        ExtendedMediaEntity[] mediaE = tweet.getExtendedMediaEntities();
        for (ExtendedMediaEntity photoE : mediaE) {
            String tipo = photoE.getType();
            if (tipo.equals("photo")) {
                foto_url = photoE.getMediaURLHttps();
                _collector.emit(new Values(foto_url));
            }
        }

        // Confirm that this tuple has been treated.
        _collector.ack(tuple);
        
            /*
            URL url = new URL(cadenaUrl);
            HttpURLConnection c = (HttpURLConnection) url.openConnection();
            c.setRequestMethod("GET");
            c.setRequestProperty("Content-length", "0");
            c.setUseCaches(false);
            c.setAllowUserInteraction(false);
            c.connect();
            int status = c.getResponseCode();

            StringBuilder sb = new StringBuilder();
            switch (status) {
                case 200:
                case 201:
                    BufferedReader br = new BufferedReader(new InputStreamReader(c.getInputStream()));
                    String line;
                    while ((line = br.readLine()) != null) {
                        sb.append(line+"\n");
                    }
                    br.close();
            

            // Las fotos dentro de un retweet no nos interesan
            // por eso solo nos interesan las fotos dentro de
            // extended_entities o entities
            JSONObject entities = null;
            JSONArray media = null;
            if (!json.isNull("extended_tweet")) {
                entities = json
                    .getJSONObject("extended_tweet")
                    .getJSONObject("entities");

            }
            else {
                entities = json.getJSONObject("entities");
                if (!entities.isNull("media")) {
                    media = entities.getJSONArray("media");
                }
            }

            if (media != null)
                for (int i=0; i<media.length();i++) {
                    JSONObject photo = media.getJSONObject(i);
                
                    String tipo = photo.getString("type");
                    // De momento solo hay fotos pero en el futuro puede haber otros media
                    if (tipo.equals("photo")) {
                        foto_url = photo.getString("media_url_https");
                        _collector.emit(new Values(foto_url));
                    }
                }

            // Confirm that this tuple has been treated.
            _collector.ack(tuple);}*/

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}
