(function (root) {
  root.MunicipalityDisplay = function(map, container, backend) {
    var element =
        $('<div class="municipality-container">' +
            '<div class="municipality-wrapper">' +
            '</div>' +
            '</div>');
    container.append(element);

    eventbus.on('map:moved', function (event) {
      //Municipality name could be shown at 5 km zoom level (level 5 = 5 Km)
      if (map.getView().getZoom() >= 5) {
        var centerLonLat = map.getView().getCenter();
        backend.getMunicipalityFromCoordinates(centerLonLat[0], centerLonLat[1], function (vkmResult) {
              var municipalityInfo = vkmResult.kunta ? vkmResult.kunta : "Tuntematon";
              container.find('.municipality-wrapper').text(municipalityInfo);
            }, function () {
              container.find('.municipality-wrapper').text('');
            }
        );
      }else
        container.find('.municipality-wrapper').text('');
    });
  };
})(this);