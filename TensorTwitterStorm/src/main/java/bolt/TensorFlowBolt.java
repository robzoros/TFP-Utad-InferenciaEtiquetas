package bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import util.LabelImage;

import java.util.Map;


/**
 * Created with IntelliJ IDEA.
 * User: qadeer
 * Date: 06.09.13
 * Time: 16:38
 * To change this template use File | Settings | File Templates.
 */
public class TensorFlowBolt extends BaseRichBolt {
    private OutputCollector _collector;
    private LabelImage labelImage;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        labelImage = new LabelImage();
    }

    @Override
    public void execute(Tuple tuple) {
        byte[] image = (byte[]) tuple.getValueByField("photo");
        String url = tuple.getStringByField("url");

        String[] clasificaciones = labelImage.classify(url, image);
        
        for (String clasificacion : clasificaciones)
            _collector.emit(new Values(clasificacion));

        // Confirm that this tuple has been treated.
        _collector.ack(tuple);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("classification"));
    }

    @Override
    public void cleanup() {
        super.cleanup();

    }
}
