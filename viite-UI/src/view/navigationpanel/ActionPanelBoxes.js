(function(ActionPanelBoxes) {
  var selectToolIcon = '<img src="images/select-tool.svg"/>';
  var cutToolIcon = '<img src="images/cut-tool.svg"/>';
  var addToolIcon = '<img src="images/add-tool.svg"/>';

  var Tool = function(toolName, icon, selectedAssetModel) {
    var className = toolName.toLowerCase();
    var element = $('<div class="action"/>').addClass(className).attr('action', toolName).append(icon).click(function() {
      executeOrShowConfirmDialog(function() {
        applicationModel.setSelectedTool(toolName);
      });
    });
    var deactivate = function() {
      element.removeClass('active');
    };
    var activate = function() {
      element.addClass('active');
    };

    return {
      element: element,
      deactivate: deactivate,
      activate: activate,
      name: toolName
    };
  };

  var ToolSelection = function(tools) {
    var element = $('<div class="panel-section panel-actions" />');
    _.each(tools, function(tool) {
      element.append(tool.element);
    });
    var hide = function() {
      element.hide();
    };
    var show = function() {
      element.show();
    };
    var deactivateAll = function() {
      _.each(tools, function(tool) {
        tool.deactivate();
      });
    };
    var reset = function() {
      deactivateAll();
      tools[0].activate();
    };
    eventbus.on('tool:changed', function(name) {
      _.each(tools, function(tool) {
        if (tool.name != name) {
          tool.deactivate();
        } else {
          tool.activate();
        }
      });
    });

    hide();

    return {
      element: element,
      reset: reset,
      show: show,
      hide: hide
    };
  };

  ActionPanelBoxes.selectToolIcon = selectToolIcon;
  ActionPanelBoxes.cutToolIcon = cutToolIcon;
  ActionPanelBoxes.addToolIcon = addToolIcon;
  ActionPanelBoxes.Tool = Tool;
  ActionPanelBoxes.ToolSelection = ToolSelection;

  ActionPanelBoxes.SpeedLimitBox = function(selectedSpeedLimit) {
    var speedLimits = [120, 100, 90, 80, 70, 60, 50, 40, 30, 20];
    var speedLimitLegendTemplate = _.map(speedLimits, function(speedLimit) {
      return '<div class="legend-entry">' +
               '<div class="label">' + speedLimit + '</div>' +
               '<div class="symbol linear speed-limit-' + speedLimit + '" />' +
             '</div>';
    }).join('');

    var expandedTemplate = [
      '<div class="panel">',
      '  <header class="panel-header expanded">',
      '    Nopeusrajoitukset',
      '  </header>',
      '  <div class="panel-section panel-legend linear-asset-legend speed-limit-legend">',
            speedLimitLegendTemplate,
      '  </div>',
      '</div>'].join('');

    var elements = {
      expanded: $(expandedTemplate)
    };

    var toolSelection = new ToolSelection([
      new Tool('Select', selectToolIcon, selectedSpeedLimit),
      new Tool('Cut', cutToolIcon, selectedSpeedLimit)
    ]);
    var editModeToggle = new EditModeToggleButton(toolSelection);

    var bindExternalEventHandlers = function() {
      eventbus.on('userData:fetched', function (userData) {
        if (_.contains(userData.roles, 'operator') || _.contains(userData.roles, 'premium')) {
          toolSelection.reset();
          elements.expanded.append(toolSelection.element);
          elements.expanded.append(editModeToggle.element);
        }
      });
      eventbus.on('application:readOnly', function(readOnly) {
        elements.expanded.find('.panel-header').toggleClass('edit', !readOnly);
      });
    };

    bindExternalEventHandlers();

    var element = $('<div class="panel-group speed-limits"/>')
      .append(elements.expanded)
      .hide();

    function show() {
      editModeToggle.toggleEditMode(applicationModel.isReadOnly());
      element.show();
    }

    function hide() {
      element.hide();
    }

    return {
      title: 'Nopeusrajoitus',
      layerName: 'speedLimit',
      element: element,
      show: show,
      hide: hide
    };
  };

  var executeOrShowConfirmDialog = function(f) {
    if (applicationModel.isDirty()) {
      new Confirm();
    } else {
      f();
    }
  };

  ActionPanelBoxes.AssetBox = function(selectedMassTransitStopModel) {
    var toolSelection = new ToolSelection([
      new Tool('Select', selectToolIcon, selectedMassTransitStopModel),
      new Tool('Add', addToolIcon, selectedMassTransitStopModel)
    ]);

    var editModeToggle = new EditModeToggleButton(toolSelection);

    var roadTypeLegend = [
        '  <div class="panel-section panel-legend road-link-legend">',
        '    <div class="legend-entry">',
        '      <div class="label">Valtion omistama</div>',
        '      <div class="symbol linear road"/>',
        '   </div>',
        '   <div class="legend-entry">',
        '     <div class="label">Kunnan omistama</div>',
        '     <div class="symbol linear street"/>',
        '   </div>',
        '   <div class="legend-entry">',
        '     <div class="label">Yksityisen omistama</div>',
        '     <div class="symbol linear private-road"/>',
        '   </div>',
        '   <div class="legend-entry">',
        '     <div class="label">Ei tiedossa tai kevyen liikenteen väylä</div>',
        '     <div class="symbol linear unknown"/>',
        '   </div>',
        '  </div>'
    ].join('');

    var expandedTemplate = [
      '<div class="panel">',
      '  <header class="panel-header expanded">',
      '    Joukkoliikenteen pysäkki',
      '  </header>',
      '  <div class="panel-section">',
      '    <div class="checkbox">',
      '      <label>',
      '        <input name="current" type="checkbox" checked> Voimassaolevat',
      '      </label>',
      '    </div>',
      '    <div class="checkbox">',
      '      <label>',
      '        <input name="future" type="checkbox"> Tulevat',
      '      </label>',
      '    </div>',
      '    <div class="checkbox">',
      '      <label>',
      '        <input name="past" type="checkbox"> K&auml;yt&ouml;st&auml; poistuneet',
      '      </label>',
      '    </div>',
      '    <div class="checkbox road-type-checkbox">',
      '      <label>',
      '        <input name="road-types" type="checkbox"> Hallinnollinen luokka',
      '      </label>',
      '    </div>',
      '  </div>',
      roadTypeLegend,
      '</div>'].join('');

    var elements = {
      expanded: $(expandedTemplate)
    };

    var bindDOMEventHandlers = function() {
      var validityPeriodChangeHandler = function(event) {
        executeOrShowConfirmDialog(function() {
          var el = $(event.currentTarget);
          var validityPeriod = el.prop('name');
          massTransitStopsCollection.selectValidityPeriod(validityPeriod, el.prop('checked'));
        });
      };

      elements.expanded.find('.checkbox').find('input[type=checkbox]').change(validityPeriodChangeHandler);
      elements.expanded.find('.checkbox').find('input[type=checkbox]').click(function(event) {
        if (applicationModel.isDirty()) {
          event.preventDefault();
        }
      });

      var expandedRoadTypeCheckboxSelector = elements.expanded.find('.road-type-checkbox').find('input[type=checkbox]');

      var roadTypeSelected = function(e) {
        var checked = e.currentTarget.checked;
        applicationModel.setRoadTypeShown(checked);
      };

      expandedRoadTypeCheckboxSelector.change(roadTypeSelected);
    };

    var toggleRoadType = function(bool) {
      var expandedRoadTypeCheckboxSelector = elements.expanded.find('.road-type-checkbox').find('input[type=checkbox]');

      elements.expanded.find('.road-link-legend').toggle(bool);
      expandedRoadTypeCheckboxSelector.prop("checked", bool);
    };

    var bindExternalEventHandlers = function() {
      eventbus.on('validityPeriod:changed', function() {
        var toggleValidityPeriodCheckbox = function(validityPeriods, el) {
          $(el).prop('checked', validityPeriods[el.name]);
        };

        var checkboxes = $.makeArray(elements.expanded.find('input[type=checkbox]'));
        _.forEach(checkboxes, _.partial(toggleValidityPeriodCheckbox, massTransitStopsCollection.getValidityPeriods()));
      });

      eventbus.on('asset:saved asset:created', function(asset) {
        massTransitStopsCollection.selectValidityPeriod(asset.validityPeriod, true);
      }, this);

      eventbus.on('userData:fetched', function (userData) {
        if (_.contains(userData.roles, 'operator') || _.contains(userData.roles, 'premium') || _.isEmpty(userData) || _.isEmpty(userData.roles)) {
          toolSelection.reset();
          elements.expanded.append(toolSelection.element);
          elements.expanded.append(editModeToggle.element);
        }
      });

      eventbus.on('road-type:selected', toggleRoadType);
    };

    bindDOMEventHandlers();

    bindExternalEventHandlers();

    toggleRoadType(true);

    var element = $('<div class="panel-group mass-transit-stops"/>')
      .append(elements.expanded)
      .hide();

    function show() {
      editModeToggle.toggleEditMode(applicationModel.isReadOnly());
      element.show();
    }

    function hide() {
      element.hide();
    }

    return {
      title: 'Joukkoliikenteen pysäkki',
      layerName: 'massTransitStop',
      element: element,
      show: show,
      hide: hide
    };
  };
})(window.ActionPanelBoxes = window.ActionPanelBoxes || {});

