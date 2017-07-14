package util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import org.tensorflow.DataType;
import org.tensorflow.Graph;
import org.tensorflow.Output;
import org.tensorflow.Session;
import org.tensorflow.Tensor;

/** Sample use of the TensorFlow Java API to label images using a pre-trained model. */
public class LabelImage {
  final static int numLabels = 3;
  private static byte[] graphDef;
  private static List<String> labels;

  public LabelImage() {
    graphDef = readAllBytesHelper(Paths.get(Constantes.modelDir, "tensorflow_inception_graph.pb"));
    labels = readAllLinesHelper(Paths.get(Constantes.modelDir, "imagenet_comp_graph_label_strings.txt"));
  }
  
  public static String[] classify (String imageUrl, byte[] imageBytes) {

    try (Tensor image = constructAndExecuteGraphToNormalizeImage(imageBytes)) {
      float[] labelProbabilities = executeInceptionGraph(graphDef, image);
      int[] indices = maxIndex(labelProbabilities, numLabels);
      String[] result = new String[numLabels];
      int count = 0;
      for(int i : indices ){
        result[count] = String.format("%s|%s|%.2f", imageUrl, labels.get(i), labelProbabilities[i] * 100f);
        count++;
      }
       
      return result;
    }
  }

  private static Tensor constructAndExecuteGraphToNormalizeImage(byte[] imageBytes) {
    try (Graph g = new Graph()) {
      GraphBuilder b = new GraphBuilder(g);
      // Some constants specific to the pre-trained model at:
      // https://storage.googleapis.com/download.tensorflow.org/models/inception5h.zip
      //
      // - The model was trained with images scaled to 224x224 pixels.
      // - The colors, represented as R, G, B in 1-byte each were converted to
      //   float using (value - Mean)/Scale.
      final int H = 224;
      final int W = 224;
      final float mean = 117f;
      final float scale = 1f;

      // Since the graph is being constructed once per execution here, we can use a constant for the
      // input image. If the graph were to be re-used for multiple input images, a placeholder would
      // have been more appropriate.
      final Output input = b.constant("input", imageBytes);
      final Output output =
          b.div(
              b.sub(
                  b.resizeBilinear(
                      b.expandDims(
                          b.cast(b.decodeJpeg(input, 3), DataType.FLOAT),
                          b.constant("make_batch", 0)),
                      b.constant("size", new int[] {H, W})),
                  b.constant("mean", mean)),
              b.constant("scale", scale));
      try (Session s = new Session(g)) {
        return s.runner().fetch(output.op().name()).run().get(0);
      }
    }
  }

  private static float[] executeInceptionGraph(byte[] graphDef, Tensor image) {
    try (Graph g = new Graph()) {
      g.importGraphDef(graphDef);
      try (Session s = new Session(g);
          Tensor result = s.runner().feed("input", image).fetch("output").run().get(0)) {
        final long[] rshape = result.shape();
        if (result.numDimensions() != 2 || rshape[0] != 1) {
          throw new RuntimeException(
              String.format(
                  "Expected model to produce a [1 N] shaped tensor where N is the number of labels, instead it produced one with shape %s",
                  Arrays.toString(rshape)));
        }
        int nlabels = (int) rshape[1];
        return result.copyTo(new float[1][nlabels])[0];
      }
    }
  }
  
  private static int[] maxIndex(float[] orig, int nummax) {
      float[] copy = Arrays.copyOf(orig,orig.length);
      Arrays.sort(copy);
      int[] result = new int[nummax];
      int count = 0;
      for(int i = orig.length - 1; i >= orig.length - nummax; i--) {
          int index = getArrayIndex(orig, copy[i]);
          result[count] = index;
          count++;
      }
      return result;
  }

  private static int getArrayIndex(float[] arr, float value) {
  
    int k=0;
    for(int i=0;i<arr.length;i++){

      if(arr[i]==value){
        k=i;
        break;
      }
    }
    return k;
  }

  private static byte[] readAllBytesHelper(Path path) {
      try {
          return Files.readAllBytes(path);
      } catch (IOException e) {
          System.err.println("Failed to read [" + path + "]: " + e.getMessage());
      }
      return null;
  }

  private static List<String> readAllLinesHelper(Path path) {
    try {
      return Files.readAllLines(path, Charset.forName("UTF-8"));
    } catch (IOException e) {
      System.err.println("Failed to read [" + path + "]: " + e.getMessage());
    }
    return null;
  }
  
  // In the fullness of time, equivalents of the methods of this class should be auto-generated from
  // the OpDefs linked into libtensorflow_jni.so. That would match what is done in other languages
  // like Python, C++ and Go.
  static class GraphBuilder {
    GraphBuilder(Graph g) {
      this.g = g;
    }

    Output div(Output x, Output y) {
      return binaryOp("Div", x, y);
    }

    Output sub(Output x, Output y) {
      return binaryOp("Sub", x, y);
    }

    Output resizeBilinear(Output images, Output size) {
      return binaryOp("ResizeBilinear", images, size);
    }

    Output expandDims(Output input, Output dim) {
      return binaryOp("ExpandDims", input, dim);
    }

    Output cast(Output value, DataType dtype) {
      return g.opBuilder("Cast", "Cast").addInput(value).setAttr("DstT", dtype).build().output(0);
    }

    Output decodeJpeg(Output contents, long channels) {
      return g.opBuilder("DecodeJpeg", "DecodeJpeg")
          .addInput(contents)
          .setAttr("channels", channels)
          .build()
          .output(0);
    }

    Output constant(String name, Object value) {
      try (Tensor t = Tensor.create(value)) {
        return g.opBuilder("Const", name)
            .setAttr("dtype", t.dataType())
            .setAttr("value", t)
            .build()
            .output(0);
      }
    }

    private Output binaryOp(String type, Output in1, Output in2) {
      return g.opBuilder(type, type).addInput(in1).addInput(in2).build().output(0);
    }

    private Graph g;
  }
}