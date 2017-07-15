package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.*;
import java.util.Map;
import java.net.URL;


/**
 * Created with IntelliJ IDEA.
 * User: qadeer
 * Date: 06.09.13
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */
public class GetImageBolt extends BaseRichBolt {
    private OutputCollector _collector;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String imageURL = tuple.getStringByField("url");

        byte[] photo = getImageFromUrl(imageURL);
        
        if (photo != null) _collector.emit(new Values(imageURL, photo));

        // Confirm that this tuple has been treated.
        _collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "photo"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
    
    private byte[] getImageFromUrl(String urlImage ) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    
        try {
            URL toDownload = new URL(urlImage);
            byte[] chunk = new byte[4096];
            int bytesRead;
            InputStream stream = toDownload.openStream();
            
            while ((bytesRead = stream.read(chunk)) > 0) {
                outputStream.write(chunk, 0, bytesRead);
            }
    
        } catch (IOException e ) {
            e.printStackTrace();
            return null;
        }
    
        return outputStream.toByteArray();
    }

}
