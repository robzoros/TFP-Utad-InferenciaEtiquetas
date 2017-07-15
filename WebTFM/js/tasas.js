var tasas = function(datos) {
    d3.select('#svgTasas').remove()
    
    var svg = d3.select('#tasas')
            .append("svg")
            .attr("id", "svgTasas")
            .attr("width", width)
            .attr("height", svgHeight);
        
    var p = d3.selectAll(".report");

    // Map para tener tasa de aciertos
    var listTasaAciertos = datos.split("\n").filter(function(l) {return l !== ""})
        .map(function(line) { 
            var dato = JSON.parse(line)
            var label = dato.label
            var tasa = Math.round((dato.aciertos / dato.num_images) * 10000) / 100
            return  {label, tasa}
        });

    var minLO = d3.min(listTasaAciertos.map(function(d) { return d.tasa }));
    var maxLO = d3.max(listTasaAciertos.map(function(d) { return d.tasa }));

    var barHeight = (height-axisMargin-margin*2)*0.7/listTasaAciertos.length,
        barPadding = (height-axisMargin-margin*2)*0.3/listTasaAciertos.length,
        bar = svg.selectAll("g")
                .data(listTasaAciertos)
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
            .text(function(d){ return d['label'].substring(0, 25); })
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
                return scale(d['tasa']);
            });

    bar.append("text")
            .attr("class", "value")
            .attr("y", barHeight / 2)
            .attr("dx", - valueMargin + labelWidth) //margin right
            .attr("dy", ".35em") //vertical align middle
            .attr("text-anchor", "end")
            .text(function(d){
                return ( d['tasa'] + "%");
            })
            .attr("x", function(d){
                var width = this.getBBox().width;
                return Math.max(width + valueMargin, scale( d['tasa']));
            });

    bar.on("mousemove", function(d){
            div.style("left", d3.event.pageX+10+"px");
            div.style("top", d3.event.pageY-25+"px");
            div.style("display", "inline-block");
            div.html(( d['label'])+"<br>"+( d['tasa'])+"%");
        });

    bar.on("mouseout", function(d){
            div.style("display", "none");
        });

    svg.insert("g",":first-child")
            .attr("class", "axisHorizontal")
            .attr("transform", "translate(" + (margin + labelWidth + valueMargin) + ","+ (height - axisMargin - margin)+")")
            .call(xAxis);
};