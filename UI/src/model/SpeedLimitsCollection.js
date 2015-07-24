(function(root) {
  root.SpeedLimitsCollection = function(backend) {
    var speedLimits = [];
    var dirty = false;
    var selection = null;
    var self = this;
    var splitSpeedLimits = {};

    var maintainSelectedSpeedLimitChain = function(collection) {
      if (!selection) return collection;

      var isSelected = function (speedLimit) { return selection.isSelected(speedLimit); };

      var collectionPartitionedBySelection = _.groupBy(collection, function(speedLimitGroup) {
        return _.some(speedLimitGroup, isSelected);
      });
      var groupContainingSelection = _.flatten(collectionPartitionedBySelection[true] || []);

      var collectionWithoutGroup = collectionPartitionedBySelection[false] || [];
      var groupWithoutSelection = _.reject(groupContainingSelection, isSelected);

      return collectionWithoutGroup.concat([groupWithoutSelection]).concat([selection.get()]);
    };

    var handleSplit = function(collection) {
      var existingSplit = _.has(splitSpeedLimits, 'existing') ? [splitSpeedLimits.existing] : [];
      var createdSplit = _.has(splitSpeedLimits, 'created') ? [splitSpeedLimits.created] : [];
      return _.map(collection, function(group) { return _.reject(group, { id: splitSpeedLimits.existing.id }); })
          .concat(existingSplit)
          .concat(createdSplit);
    };

    this.getAll = function() {
      var allWithSelectedSpeedLimitChain = maintainSelectedSpeedLimitChain(speedLimits);

      if (selection && selection.isSplit()) {
        return handleSplit(allWithSelectedSpeedLimitChain);
      } else {
        return allWithSelectedSpeedLimitChain;
      }
    };

    // TODO: Add sidecode to generatedId
    var generateUnknownLimitId = function(speedLimit) {
      return speedLimit.mmlId.toString() +
          speedLimit.startMeasure.toFixed(2) +
          speedLimit.endMeasure.toFixed(2);
    };

    this.fetch = function(boundingBox) {
      backend.getSpeedLimits(boundingBox, function(speedLimitGroups) {
        var partitionedSpeedLimitGroups = _.groupBy(speedLimitGroups, function(speedLimitGroup) {
          return _.some(speedLimitGroup, function(speedLimit) { return _.has(speedLimit, "id"); });
        });
        var knownSpeedLimits = partitionedSpeedLimitGroups[true] || [];
        var unknownSpeedLimits = _.map(partitionedSpeedLimitGroups[false], function(speedLimitGroup) {
          return _.map(speedLimitGroup, function(speedLimit) {
            return _.merge({}, speedLimit, { generatedId: generateUnknownLimitId(speedLimit) });
          });
        }) || [];
        speedLimits = knownSpeedLimits.concat(unknownSpeedLimits);
        eventbus.trigger('speedLimits:fetched', self.getAll());
      });
    };

    this.getUnknown = function(generatedId) {
      // TODO: Fix this when splitting is implemented
      throw "Not Implemented";
    };

    var isUnknown = function(speedLimit) {
      return !_.has(speedLimit, 'id');
    };

    var isEqual = function(a, b) {
      return (_.has(a, 'generatedId') && _.has(b, 'generatedId') && (a.generatedId === b.generatedId)) ||
        ((!isUnknown(a) && !isUnknown(b)) && (a.id === b.id));
    };

    this.getGroup = function(segment) {
      return _.find(speedLimits, function(speedLimitGroup) {
        return _.some(speedLimitGroup, function(s) { return isEqual(s, segment); });
      });
    };

    this.setSelection = function(sel) {
      selection = sel;
    };

    this.replaceGroup = function(segment, newGroup) {
      var replaceInCollection = function(collection, segment, newGroup) {
        return _.reject(collection, function(speedLimitGroup) {
          return _.some(speedLimitGroup, function(s) {
            return isEqual(s, segment);
          });
        }).concat([newGroup]);
      };
      if (splitSpeedLimits.created) {
        splitSpeedLimits.created.value = newGroup[0].value;
      }
      speedLimits = replaceInCollection(speedLimits, segment, newGroup);
      return newGroup;
    };

    var calculateMeasure = function(link) {
      var points = _.map(link.points, function(point) {
        return new OpenLayers.Geometry.Point(point.x, point.y);
      });
      return new OpenLayers.Geometry.LineString(points).getLength();
    };

    this.splitSpeedLimit = function(id, mmlId, split, callback) {
      var link = _.find(_.flatten(speedLimits), { id: id });
      var towardsLinkChain = link.towardsLinkChain;

      var left = _.cloneDeep(link);
      left.points = towardsLinkChain ? split.firstSplitVertices : split.secondSplitVertices;

      var right = _.cloneDeep(link);
      right.points = towardsLinkChain ? split.secondSplitVertices : split.firstSplitVertices;

      if (calculateMeasure(left) < calculateMeasure(right)) {
        splitSpeedLimits.created = left;
        splitSpeedLimits.existing = right;
      } else {
        splitSpeedLimits.created = right;
        splitSpeedLimits.existing = left;
      }

      splitSpeedLimits.created.id = null;
      splitSpeedLimits.splitMeasure = split.splitMeasure;
      splitSpeedLimits.splitMmlId = mmlId;
      dirty = true;
      callback(splitSpeedLimits.created);
      eventbus.trigger('speedLimits:fetched', self.getAll());
    };

    this.saveSplit = function(callback) {
      backend.splitSpeedLimit(splitSpeedLimits.existing.id, splitSpeedLimits.splitMmlId, splitSpeedLimits.splitMeasure, splitSpeedLimits.created.value, function(updatedSpeedLimits) {
        var existingId = splitSpeedLimits.existing.id;
        splitSpeedLimits = {};
        dirty = false;

        var newSpeedLimit = _.find(updatedSpeedLimits, function(speedLimit) { return speedLimit.id !== existingId; });
        callback(newSpeedLimit);

        eventbus.trigger('speedLimit:saved');

        eventbus.trigger('speedLimit:cut');
      });
    };

    this.cancelSplit = function() {
      dirty = false;
      splitSpeedLimits = {};
      eventbus.trigger('speedLimits:fetched', self.getAll());
    };

    this.isDirty = function() {
      return dirty;
    };

  };
})(this);
