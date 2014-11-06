/*jshint expr: true*/
define(['chai', 'TestHelpers'], function(chai, testHelpers) {
  var expect = chai.expect;
  var openLayersMap;
  var splitBackendCalls = [];

  var speedLimitTestData = SpeedLimitSplitTestData.generateSpeedLimitLinks();
  var roadLinkTestData = SpeedLimitSplitTestData.generateRoadLinks();

  var speedLimitSplitting = function(id, roadLinkId, splitMeasure, limit, success, failure) {
    splitBackendCalls.push({
      id: id,
      roadLinkId: roadLinkId,
      splitMeasure: splitMeasure,
      limit: limit
    });
    success([{ id: id, limit: 50, speedLimitLinks: [speedLimitTestData[0]]}, { id: 123456, limit: limit, speedLimitLinks: [speedLimitTestData[0]] }]);
  };

  var speedLimitConstructor = function(id) {
    return {
      speedLimitLinks: _.filter(speedLimitTestData, function(limit){ return limit.id == id;})
    };
  };

  describe('when loading application in edit mode with speed limit data', function() {
    before(function (done) {


      testHelpers.restartApplication(function (map) {
        openLayersMap = map;
        $('.speed-limits').click();
        testHelpers.clickVisibleEditModeButton();
        done();
      }, testHelpers.defaultBackend()
          .withStartupParameters({ lon: 0.0, lat: 0.0, zoom: 11 })
          .withRoadLinkData(roadLinkTestData)
          .withSpeedLimitsData(speedLimitTestData)
          .withSpeedLimitSplitting(speedLimitSplitting)
          .withSpeedLimitConstructor(speedLimitConstructor));
    });

    describe('and selecting cut tool', function () {
      before(function () {
        $('.action.cut').click();
      });

      describe('and cutting a speed limit', function () {
        before(function () {
          var pixel = openLayersMap.getPixelFromLonLat(new OpenLayers.LonLat( 0.0, 58.0));
          openLayersMap.events.triggerEvent('click', {target: {}, srcElement: {}, xy: {x: pixel.x, y: pixel.y}});
        });

        it('the speed limit should be split into two features', function() {
          var split1_points = testHelpers.getPointsOfSpeedLimitLineFeatures(openLayersMap, 1123812);
          var split2_points = testHelpers.getPointsOfSpeedLimitLineFeatures(openLayersMap, null);

          expect(split1_points[0].x).to.equal(0);
          expect(split1_points[0].y).to.equal(0);
          expect(split1_points[1].x).to.equal(0);
          expect(split1_points[1].y).to.be.closeTo(58.0, 0.5);

          expect(split2_points[0].x).to.equal(0);
          expect(split2_points[0].y).to.be.closeTo(58.0, 0.5);
          expect(split2_points[1].x).to.equal(0);
          expect(split2_points[1].y).to.equal(100);
        });

        describe('and setting limit for new speed limit', function () {
          before(function () {
            $('select.speed-limit option[value="100"]').prop('selected', true).change();
          });

          describe('and saving split speed limit', function () {
            before(function () {
              $('.save.btn').click();
            });

            it('relays split speed limit to backend', function () {
              expect(splitBackendCalls).to.have.length(1);
              var backendCall = _.first(splitBackendCalls);
              expect(backendCall.id).to.equal(1123812);
              expect(backendCall.roadLinkId).to.equal(5540);
              expect(backendCall.splitMeasure).to.be.closeTo(58.0, 0.5);
              expect(backendCall.limit).to.equal(100);
            });
          });
        });
      });
    });
  });
});