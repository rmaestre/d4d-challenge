$(function() {
    var map = L.map('map').setView([7.56944, -5.33936], 7);
    map.setMaxBounds(new L.LatLngBounds(new L.LatLng(14.08138, -12.26074),new L.LatLng(0.66693, 2.02148)));
    L.tileLayer('http://176.9.70.241:7890/d4d/{z}/{x}/{y}.png', {
    	minZoom: 6,
    	maxZoom: 13,
    }).addTo(map);
    var popup = L.popup();
    function onMapClick(e) {
        popup
            .setLatLng(e.latlng)
            .setContent(e.latlng.toString())
            .openOn(map);
    }
    map.on('click', onMapClick);
});