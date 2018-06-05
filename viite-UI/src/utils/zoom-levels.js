(function() {
  window.zoomlevels = {
    isInRoadLinkZoomLevel: function(zoom) {
      return zoom >= this.minZoomForRoadLinks;
    },
    isInAssetZoomLevel: function(zoom) {
      return zoom >= this.minZoomForAssets;
    },
    getAssetZoomLevelIfNotCloser: function(zoom) {
      return zoom < 10 ? 10 : zoom;
    },
    minZoomForAssets: 6,
    minZoomForDirectionalMarkers: 11,
    minZoomForRoadLinks: 5,
    minZoomForEditMode: 10,
    maxZoomLevelsForCalibrationPoints: 8,
    maxZoomLevel: 15
  };
})();