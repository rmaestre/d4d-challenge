$(function() {
    var map = L.map('map', { zoomControl:true });
	map.setMaxBounds(new L.LatLngBounds(new L.LatLng(14.08138, -12.26074),new L.LatLng(0.66693, 2.02148)));
    L.tileLayer('http://176.9.70.241:7890/d4d/{z}/{x}/{y}.png', {
    	minZoom: 6,
    	maxZoom: 13,
		attribution: 'Developped by <a href="http://labs.paradigmatecnologico.com/about"><img style="height:18px;" src="http://labs.paradigmatecnologico.com/wordpress/wp-content/uploads/2011/03/paradigmaLabV31.png"></a> with cooperation of <a href="http://www.csic.es">CSIC</a>'
    }).addTo(map);
    map.setView([7.56944, -5.33936], 7);

	var popup = L.popup();
    function onMapClick(e) {
        popup
            .setLatLng(e.latlng)
            .setContent(e.latlng.toString())
            .openOn(map);
    }
    map.on('click', onMapClick);
	

	// create fullscreen control
	var fullScreen = new L.Control.FullScreen();
	// add fullscreen control to the map
	map.addControl(fullScreen);

	// detect fullscreen toggling
	map.on('enterFullscreen', function(){
		if(window.console) window.console.log('enterFullscreen');
	});
	map.on('exitFullscreen', function(){
		if(window.console) window.console.log('exitFullscreen');
	});

});