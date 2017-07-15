/* global $*/
/* global d3*/
/* global idiomas*/
var div = d3.select("body").append("div").attr("class", "toolTip");

var axisMargin = 10,
    margin = 20,
    valueMargin = 4,
    width = 960,
    height = 800,
    svgHeight = 700,
    labelWidth = 0;

// Para csvs separados por "~"
var dsv = d3.dsv("|");

var tratar = function(datos) {
    var lista = dsv.parseRows(datos).map(function(cl) {return cl[1];})
    var listaAgrupada = d3.nest()
        .key(function(d) { return d; })
        .entries(lista).map(function(d) {return {key: d.key, value: d.values.length}});
    var listaOrdenada = listaAgrupada
        .sort(function(a, b) {return b.value - a.value})
        .slice(0, 25)
        .sort( function(a, b) {
            var nameA = a.key.toUpperCase(); // ignore upper and lowercase
            var nameB = b.key.toUpperCase(); // ignore upper and lowercase
            if (nameA < nameB) {
                return -1;
            }
            if (nameA > nameB) {
                return 1;
            }
            return 0;
        });

    console.log(listaOrdenada)

    d3.select('#svgClassification').remove()
    
    var svg = d3.select('#classification')
            .append("svg")
            .attr("id", "svgClassification")
            .attr("width", width)
            .attr("height", svgHeight);
        
    var p = d3.selectAll(".report");

    // Map para tener tasa de aciertos
    var minLO = d3.min(listaOrdenada.map(function(d) { return d.value }));
    var maxLO = d3.max(listaOrdenada.map(function(d) { return d.value }));

    var barHeight = (height-axisMargin-margin*2)*0.7/listaOrdenada.length,
        barPadding = (height-axisMargin-margin*2)*0.3/listaOrdenada.length,
        bar = svg.selectAll("g")
                .data(listaOrdenada)
                .enter()
                .append("g");

    bar.attr("class", "bar")
            .attr("cx",0)
            .attr("transform", function(d, i) {
                return "translate(" + margin + "," + (i * (barHeight + barPadding) + barPadding) + ")";
            });

    bar.append("text")
            .attr("class", "label")
            .attr("y", barHeight / 2)
            .attr("dy", ".35em") //vertical align middle
            .text(function(d){ return d.key; })
            .each(function() {
               labelWidth = Math.ceil(Math.max(labelWidth, this.getBBox().width));
            });

    var scale = d3.scale.linear()
            .domain([0, maxLO])
            .range([0, width - margin*2 - labelWidth]);

    var xAxis = d3.svg.axis()
            .scale(scale)
            .tickSize(-height + 2*margin + axisMargin)
            .orient("bottom");

    bar.append("rect")
            .attr("transform", "translate(" +(labelWidth+valueMargin)+", 0)")
            .attr("height", barHeight)
            .attr("width", function(d){
                return scale(d.value);
            });

    bar.append("text")
            .attr("class", "value")
            .attr("y", barHeight / 2)
            .attr("dx", - valueMargin + labelWidth) //margin right
            .attr("dy", ".35em") //vertical align middle
            .attr("text-anchor", "end")
            .text(function(d){
                return ( d.value);
            })
            .attr("x", function(d){
                var width = this.getBBox().width;
                return Math.max(width + valueMargin, scale( d.value));
            });

    bar.on("mousemove", function(d){
            div.style("left", d3.event.pageX+10+"px");
            div.style("top", d3.event.pageY-25+"px");
            div.style("display", "inline-block");
            div.html((d.key)+": "+(d.value));
        });

    bar.on("mouseout", function(d){
            div.style("display", "none");
        });

    svg.insert("g",":first-child")
            .attr("class", "axisHorizontal")
            .attr("transform", "translate(" + (margin + labelWidth + valueMargin) + ","+ (height - axisMargin - margin)+")")
            .call(xAxis);

    //Imágenes
    var listaImagenes = dsv.parseRows(datos).map(function(cl) {return {imagen: cl[0], label:cl[1]};})
    bar.on("click", function(d){
        var imagenes = d3.map(listaImagenes.filter(function(fila){ return fila.label === d.key}), function(fila) {return fila.imagen }).keys().slice(-40);
        $("#titulo").text(d.key + " - Últimas 40 imágenes")
        d3.select('#imagenesDiv').remove()
        d3.select("#imagenes")
            .append("div")
            .attr("id", "imagenesDiv");

        imagenes.forEach(function(img) {
            d3.select("#imagenesDiv")
                .append("a")
                .attr("href", img)
                .append("img")
                .attr({height: 66, src: img });
        });

        $("#modalImagenes").modal()
    });

};

function crearGrafico () {
    // Cargamos datos 
    d3.text("/ficheros/twitter/twitter.txt", tratar);
    setTimeout(crearGrafico, 10000);
}

crearGrafico();
