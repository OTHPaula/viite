(function(root) {
  root.SelectedProjectLink = function(projectLinkCollection) {

    var current = [];
    var ids = [];
    var dirty = false;
    var splitSuravage = {};
    //var LinkGeomSource = LinkValues.LinkGeomSource;
    var LinkStatus = LinkValues.LinkStatus;
    var preSplitData = null;
    var nearest = null;

    var open = function (id, multiSelect) {
      if (!multiSelect) {
        current = projectLinkCollection.getProjectLink([id]);
        ids = [id];
      } else {
        ids = projectLinkCollection.getMultiProjectLinks(id);
        current = projectLinkCollection.getProjectLink(ids);
      }
      eventbus.trigger('projectLink:clicked', get(id));
    };

    var openWithErrorMessage = function (linkId, errorMessage) {
        ids = projectLinkCollection.getMultiProjectLinks(linkId);
        current = projectLinkCollection.getProjectLink(ids);

        eventbus.trigger('projectLink:errorClicked', get(linkId), errorMessage);
    };

    var orderSplitParts = function(links) {
      var splitLinks = _.partition(links, function(link) {
        return !_.isUndefined(link.connectedLinkId);
      });
      return _.sortBy(splitLinks[0], function (s) { return s.status == LinkStatus.Transfer.value ? 1 : s.status; });
    };

    var getLinkMarker = function(linkList, statusList) {
      var filter = _.filter(linkList, function (link) {
        return _.contains(statusList,link.status);
      });
      if (filter.length > 1) {
        var min = _.min(_.map(filter, function (template) {
          return template.startAddressM;
        }));

        var max = _.max(_.map(filter, function (template) {
          return template.endAddressM;
        }));

        var toReturn = filter[0];
        toReturn.startAddressM = min;
        toReturn.endAddressM = max;
        return toReturn;
      }
      return filter[0];
    };

    var openSplit = function (linkid, multiSelect) {
      if (!multiSelect) {
        current = projectLinkCollection.getProjectLink([linkid]);
        ids = [linkid];
      } else {
        ids = projectLinkCollection.getMultiProjectLinks(linkid);
        current = projectLinkCollection.getProjectLink(ids);
      }
      var orderedSplitParts = orderSplitParts(get());
      var suravageA = getLinkMarker(orderedSplitParts, [LinkStatus.Transfer.value, LinkStatus.Unchanged.value]);
      var suravageB = getLinkMarker(orderedSplitParts, [LinkStatus.New.value]);
      var terminatedC = getLinkMarker(orderedSplitParts, [LinkStatus.Terminated.value]);
      suravageA.marker = "A";
      if (!suravageB) {
        suravageB = zeroLengthSplit(suravageA);
        suravageA.points = suravageA.originalGeometry;
      }
      suravageB.marker = "B";
      if (terminatedC) {
        terminatedC.marker = "C";
      }
      eventbus.trigger('split:projectLinks',  [suravageA, suravageB, terminatedC]);
      var splitPoint = GeometryUtils.connectingEndPoint(suravageA.points, suravageB.points);
      projectLinkCollection.getCutLine(suravageA.linkId, splitPoint);
    };

    var preSplitSuravageLink = function(suravage) {
      projectLinkCollection.preSplitProjectLinks(suravage, nearest);
    };

    var zeroLengthSplit = function(suravageLink) {
      return {
        connectedLinkId: suravageLink.connectedLinkId,
        linkId: suravageLink.linkId,
        startAddressM: 0,
        endAddressM: 0,
        startMValue: 0,
        endMValue: 0
      };
    };

    var zeroLengthTerminated = function(adjacentLink) {
      return {
        connectedLinkId: adjacentLink.connectedLinkId,
        linkId: adjacentLink.linkId,
        status: LinkStatus.Terminated.value,
        startAddressM: 0,
        endAddressM: 0,
        startMValue: 0,
        endMValue: 0
      };
    };
/*
    var splitSuravageLinks = function(nearestSuravage, split, mousePoint, callback) {
      var left = _.cloneDeep(nearestSuravage);
      left.points = split.firstSplitVertices;

      var right = _.cloneDeep(nearestSuravage);
      right.points = split.secondSplitVertices;
      var measureLeft = calculateMeasure(left);
      var measureRight = calculateMeasure(right);
      splitSuravage.created = left;
      splitSuravage.created.endMValue = measureLeft;
      splitSuravage.existing = right;
      splitSuravage.existing.endMValue = measureRight;
      splitSuravage.created.splitPoint = mousePoint;
      splitSuravage.existing.splitPoint = mousePoint;

      splitSuravage.created.id = null;
      splitSuravage.splitMeasure = split.splitMeasure;

      splitSuravage.created.marker = 'A';
      splitSuravage.existing.marker = 'B';

      callback(splitSuravage);
    };

    var getPoint = function(link) {
      if (link.sideCode == LinkValues.SideCode.AgainstDigitizing.value) {
        return _.first(link.points);
      } else {
        return _.last(link.points);
      }
    };

    var calculateMeasure = function(link) {
      var points = _.map(link.points, function(point) {
        return [point.x, point.y];
      });
      return new ol.geom.LineString(points).getLength();
    };
*/
    var isDirty = function() {
      return dirty;
    };

    var setDirty = function(value) {
      dirty = value;
    };

    var openShift = function(linkIds) {
      if (linkIds.length === 0) {
        cleanIds();
        close();
      } else {
        var added = _.difference(linkIds, ids);
        ids = linkIds;
        current = _.filter(current, function(link) {
          return _.contains(linkIds, link.getData().id || link.getData().linkId);
          }
        );
        current = current.concat(projectLinkCollection.getProjectLink(added));
        eventbus.trigger('projectLink:clicked', get());
      }
    };

    var get = function(id) {
      var clicked = _.filter(current, function (c) {
        if (c.getData().id > 0) {
          return c.getData().id === id;
        } else {
          return c.getData().linkId === id;
        }
      });
      var others = _.filter(_.map(current, function(projectLink) { return projectLink.getData(); }), function (link) {
        if (link.id > 0) {
          return link.id !== id;
        } else {
          return link.linkId !== id;
        }
      });
      if (!_.isUndefined(clicked[0])) {
        return [clicked[0].getData()].concat(others);
      }
      return others;
    };

    var getPreSplitData = function() {
      return preSplitData;
    };

    var setCurrent = function(newSelection) {
      current = newSelection;
    };

    var getCurrent = function () {
      return _.map(current, function(curr) {
        return curr.getData();
      });
    };

    var isSelected = function(linkId) {
      return _.contains(ids, linkId);
    };

    var clean = function() {
      current = [];
    };

    var cleanIds = function() {
      ids = [];
    };

    var close = function() {
      current = [];
      eventbus.trigger('layer:enableButtons', true);
    };

    var revertSuravage = function() {
      splitSuravage = {};
    };

    var getNearestPoint = function () {
      return nearest;
    };

    var setNearestPoint = function(point) {
      nearest = point;
    };

    eventbus.on('projectLink:preSplitSuccess', function(data) {
      preSplitData = data;
      var suravageA = data.a;
      if (!suravageA) {
        suravageA = zeroLengthSplit(data.b);
      }
      var suravageB = data.b;
      if (!suravageB) {
        suravageB = zeroLengthSplit(suravageA);
        suravageB.status = LinkStatus.New.value;
      }
      var terminatedC = data.c;
      if (!terminatedC) {
        terminatedC = zeroLengthTerminated(suravageA);
      }
      ids = projectLinkCollection.getMultiProjectLinks(suravageA.linkId);
      current = projectLinkCollection.getProjectLink(_.flatten(ids));
      suravageA.marker = "A";
      suravageB.marker = "B";
      terminatedC.marker = "C";
      suravageA.text = "SUUNNITELMALINKKI";
      suravageB.text = "SUUNNITELMALINKKI";
      terminatedC.text = "NYKYLINKKI";
      suravageA.splitPoint = nearest;
      suravageB.splitPoint = nearest;
      terminatedC.splitPoint = nearest;
      applicationModel.removeSpinner();
      eventbus.trigger('split:projectLinks', [suravageA, suravageB, terminatedC]);
      eventbus.trigger('split:cutPointFeature', data.split, terminatedC);
    });

    return {
      open: open,
      openWithErrorMessage: openWithErrorMessage,
      openShift: openShift,
      openSplit: openSplit,
      get: get,
      clean: clean,
      cleanIds: cleanIds,
      close: close,
      isSelected: isSelected,
      setCurrent: setCurrent,
      getCurrent: getCurrent,
      isDirty: isDirty,
      setDirty: setDirty,
      preSplitSuravageLink: preSplitSuravageLink,
      getPreSplitData: getPreSplitData,
      revertSuravage: revertSuravage,
      //getNearestPoint: getNearestPoint,
      setNearestPoint: setNearestPoint
    };
  };
})(this);
