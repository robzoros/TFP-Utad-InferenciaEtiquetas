package bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.*;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: qadeer
 * Date: 06.09.13
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */
public class FileWriterBolt extends BaseRichBolt {
    private PrintWriter writer;
    private int count = 0;
    private OutputCollector _collector;
    private String filename;

    public FileWriterBolt(String filename){
        this.filename = filename;
    }



    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        try {
            FileWriter fw = new FileWriter(filename, true);
            BufferedWriter bw = new BufferedWriter(fw);
            writer= new PrintWriter(bw);
        } catch (IOException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }

    }

    @Override
    public void execute(Tuple tuple) {
        writer.println(tuple.getStringByField("classification"));
        if ( count % 100 == 0) writer.flush();
        // Confirm that this tuple has been treated.
        _collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
        writer.flush();
        writer.close();
        super.cleanup();
    }
}
