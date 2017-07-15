/* global $*/
/* global d3*/
/* global idiomas*/
var div = d3.select("body").append("div").attr("class", "toolTip");

var axisMargin = 10,
    margin = 20,
    valueMargin = 4,
    width = 960,
    height = 1200,
    svgHeight = 900,
    labelWidth = 0;

// Para csvs separados por "~"
var dsv = d3.dsv("~");
var origen = "-1";
var fichOrigen = ""

// Cargamos estadísticas
// Número de Etiquetas Flickr
d3.text("datos/mirflickr_original.json", function(datos) {
    var numEtiquetas = datos.split("\n").filter(function(l) {return l !== ""}).length
    d3.select('#num-etiquetas').text(numEtiquetas.toLocaleString());
});

// Número de Imágenes
d3.text("datos/inception_classification.json", function(datos) {
    var lista = datos.split("\n").filter(function(l) {return l !== ""})
    var numImagenes = d3.map(lista.map(function(line) { return {id: JSON.parse(line).image}; })
        , function(d) { return d.id; }).size();
    d3.select('#num-imagenes').text(numImagenes.toLocaleString());
});

// Etiqueta más repetida Flickr
d3.text("datos/mirflickr_analisis.csv", function(datos) {
    var lista = dsv.parseRows(datos)
    var max = d3.entries(lista)
        .sort(function(a, b) { return d3.descending(+a.value[1], +b.value[1]); })[0];
    console.log(max)
    var etiqueta = max.value[0]
    var cuenta = max.value[1]
    d3.select('#etqta-flickr').text(etiqueta + " " + cuenta.toLocaleString());
});

// Etiqueta más repetida Inception
d3.text("datos/inception_analisis.csv", function(datos) {
    var lista = dsv.parseRows(datos)
    var max = d3.entries(lista)
        .sort(function(a, b) { return d3.descending(+a.value[1], +b.value[1]); })[0];
    console.log(max)
    var etiqueta = max.value[0]
    var cuenta = max.value[1]
    d3.select('#etqta-inception').text(etiqueta + " " + cuenta.toLocaleString());
});
// Fin estadísticas

// Cargamos datos de tasas de acierto de etiquetas.
$("#origen :input").change(function() {
    if (origen !== this.value) {
        origen = this.value
        fichOrigen = origen === "0" ? "datos/mirflickr_estadisticas.json" : "datos/inception_estadisticas.json"
    }
    
    d3.text(fichOrigen, tasas);
});


// Cargamos nube de idiomas
d3.text("datos/mirflickr_idiomas.csv", function(text) {

    var datos = d3.csv.parseRows(text)
    var minSize = d3.min(datos.map(function(d) { return +d[1] }));
    var maxSize = d3.max(datos.map(function(d) { return +d[1] }));

    var fill = d3.scale.category20();

    var fontScale = d3.scale.linear()
        .domain([minSize, maxSize])
        .range([12, 100]);    
    
    var labelSet = datos.map(function(idioma) { return { text: idiomas[idioma[0]], size: fontScale(+idioma[1]) } })
    
    var widthNube = 500;
    var heightNube = 500;

    d3.layout.cloud()
    	.size([widthNube, heightNube])
    	.words(labelSet)
    	.rotate(function() { return ~~(Math.random() * 2) * 90; })
    	.font("Impact")
    	.fontSize(function(d) { return d.size; })
    	.on("end", drawSkillCloud)
    	.start();

    // Finally implement `drawSkillCloud`, which performs the D3 drawing:
    // apply D3.js drawing API
    function drawSkillCloud(words) {
    	d3.select("#cloud").append("svg")
    		.attr("width", widthNube)
    		.attr("height", heightNube)
    		.attr("id", "svgcloud")
    		.append("g")
    		.attr("transform", "translate(" + ~~(widthNube / 2) + "," + ~~(heightNube / 2) + ")")
    		.selectAll("text")
    		.data(words)
    		.enter().append("text")
    		.style("font-size", function(d) { return d.size + "px"; })
    		.style("-webkit-touch-callout", "none")
    		.style("-webkit-user-select", "none")
    		.style("-khtml-user-select", "none")
    		.style("-moz-user-select", "none")
    		.style("-ms-user-select", "none")
    		.style("user-select", "none")
    		.style("cursor", "default")
    		.style("font-family", "Impact")
    		.style("fill", function(d, i) { return fill(i); })
    		.attr("text-anchor", "middle")
    		.attr("transform", function(d) {
    			return "translate(" + [d.x, d.y] + ")rotate(" + d.rotate + ")";
    		})
    		.text(function(d) { return d.text; });
    }
    
    // set the viewbox to content bounding box (zooming in on the content, effectively trimming whitespace)

    var svgCloud = document.getElementById("svgcloud");
    var bbox = svgCloud.getBBox();
    var viewBox = [bbox.x, bbox.y, bbox.width, bbox.height].join(" ");
    svgCloud.setAttribute("viewBox", viewBox);    
})