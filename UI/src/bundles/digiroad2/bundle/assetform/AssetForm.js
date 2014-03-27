Oskari.clazz.define("Oskari.digiroad2.bundle.assetform.AssetForm",

    function(config) {
        this.sandbox = null;
        this.started = false;
        this.mediator = null;
        this._enumeratedPropertyValues = null;
        this._featureDataAssetId = null;
        this._backend = defineDependency('backend', window.Backend);
        this._readOnly = true;

        function defineDependency(dependencyName, defaultImplementation) {
            var dependency = _.isObject(config) ? config[dependencyName] : null;
            return dependency || defaultImplementation;
        }
    }, {
        __name : 'AssetForm',

        getName : function() {
            return this.__name;
        },
        setSandbox : function(sandbox) {
            this.sandbox = sandbox;
        },
        getSandbox : function() {
            return this.sandbox;
        },
        update : function() {
        },
        start : function() {
            var me = this;
            if(me.started) {
                return;
            }
            me.started = true;
            // Should this not come as a param?
            var sandbox = Oskari.$('sandbox');
            sandbox.register(me);
            me.setSandbox(sandbox);

            for(var p in me.eventHandlers) {
                if(me.eventHandlers.hasOwnProperty(p)) {
                    sandbox.registerForEventByName(me, p);
                }
            }
        },
        init : function() {
            eventbus.on('asset:fetched assetPropertyValue:fetched asset:created', this._initializeEditExisting, this);
            eventbus.on('asset:unselected', this._closeAsset, this);
            eventbus.on('asset:placed', function(asset) {
                this._selectedAsset = asset;
                this._backend.getAssetTypeProperties(10);
            }, this);
            eventbus.on('assetTypeProperties:fetched', function(properties) {
                this._initializeCreateNew(properties);
            }, this);
            eventbus.on('assetPropertyValue:changed', function(data) {
                if (data.propertyData[0].propertyId == 'validityDirection') {
                    this._changeAssetDirection(data);
                }
            }, this);
            eventbus.on('application:readOnly', function(readOnly) {
                this._readOnly = readOnly;
            }, this);
            eventbus.on('enumeratedPropertyValues:fetched', function(values) {
                this._enumeratedPropertyValues = values;
            }, this);
            
            this._templates = Oskari.clazz.create('Oskari.digiroad2.bundle.assetform.template.Templates');
            this._getPropertyValues();

            return null;
        },
        _initializeEditExisting : function(asset) {
            var me = this;
            this._selectedAsset = asset;
            me._featureDataAssetId = asset.id;
            var position = {bearing: asset.bearing, lonLat: {lon: asset.lon, lat: asset.lat}, validityDirection: asset.validityDirection};
            var featureData = me._makeContent(asset.propertyData);
            var streetView = me._getStreetView(position);
            var featureAttributes = me._templates.featureDataWrapper({ header: busStopHeader(asset), streetView: streetView, attributes: featureData, controls: null });
            jQuery("#featureAttributes").html(featureAttributes);
            me._addDatePickers();
            if (this._readOnly) {
              $('#featureAttributes button').prop('disabled', true);
              $('#featureAttributes input').prop('disabled', true);
              $('#featureAttributes select').prop('disabled', true);
              $('#featureAttributes textarea').prop('disabled', true);
            }
            jQuery(".featureAttributeText , .featureAttributeLongText").on("blur", function () {
                var data = jQuery(this);
                me._savePropertyData(me._propertyValuesOfTextElement(data), data.attr('data-propertyId'));
            });
            jQuery("select.featureattributeChoice").on("change", function () {
                var data = jQuery(this);
                me._savePropertyData(me._propertyValuesOfSelectionElement(data), data.attr('data-propertyId'));
            });
            jQuery("div.featureattributeChoice").on("change", function () {
                var data = jQuery(this);
                me._savePropertyData(me._propertyValuesOfMultiCheckboxElement(data), data.attr('data-propertyId'));
            });
            jQuery("button.featureAttributeButton").on("click", function () {
                var data = jQuery(this);
                me._savePropertyData(me._propertyValuesOfButtonElement(data), data.attr('data-propertyId'));
            });
            jQuery(".featureAttributeDate").on("blur", function () {
                var data = jQuery(this);
                me._savePropertyData(me._propertyValuesOfDateElement(data), data.attr('data-propertyId'));
            });

            function busStopHeader(asset) {
                if (_.isNumber(asset.externalId)) {
                    return 'Valtakunnallinen ID: ' + asset.externalId;
                }
                else return 'Ei valtakunnallista ID:tä';
            }
        },
        
        _changeAssetDirection: function(data) {
            var newValidityDirection = data.propertyData[0].values[0].propertyValue;
            var validityDirection = jQuery('.featureAttributeButton[data-propertyid="validityDirection"]');
            validityDirection.attr('value', newValidityDirection);
            jQuery('.streetView').html(this._getStreetView(_.merge({}, this._selectedAsset.position, { validityDirection: newValidityDirection })));
        },
        
        _initializeCreateNew: function(properties) {
            var me = this;
            var featureAttributesElement = jQuery('#featureAttributes');
            var featureData = me._makeContent(properties);
            var streetView = me._getStreetView(me._selectedAsset.position);
            var featureAttributesMarkup = me._templates.featureDataWrapper({ header : 'Uusi Pysäkki', streetView : streetView, attributes : featureData, controls: me._templates.featureDataControls({}) });
            featureAttributesElement.html(featureAttributesMarkup);
            me._addDatePickers();

            var validityDirection = featureAttributesElement.find('.featureAttributeButton[data-propertyid="validityDirection"]');
            var assetDirectionChangedHandler = function() {
                var value = validityDirection.attr('value');
                var newValidityDirection = validityDirection.attr('value') == 2 ? 3 : 2;
                eventbus.trigger('assetPropertyValue:changed', {
                    propertyData: [{
                        propertyId: validityDirection.attr('data-propertyId'),
                        values: [{propertyValue: newValidityDirection}]
                    }]
                });
            };

            featureAttributesElement.find('div.featureattributeChoice').on('change', function() {
                var jqElement = jQuery(this);
                var values = me._propertyValuesOfMultiCheckboxElement(jqElement);
                eventbus.trigger('assetPropertyValue:changed', {
                  propertyData: [{
                      propertyId: jqElement.attr('data-propertyId'),
                      values: values
                  }]
                });
            });

            validityDirection.click(assetDirectionChangedHandler);

            featureAttributesElement.find('button.cancel').on('click', function() {
                eventbus.trigger('asset:cancelled');
                eventbus.trigger('asset:unselected');
                me._closeAsset();
            });

            featureAttributesElement.find('button.save').on('click', function() {
                var textElements = featureAttributesElement.find('.featureAttributeText , .featureAttributeLongText');
                var textElementAttributes = _.map(textElements, function(textElement) {
                    var jqElement = jQuery(textElement);
                    return {
                        propertyId: jqElement.attr('data-propertyId'),
                        propertyValues: me._propertyValuesOfTextElement(jqElement)
                    };
                });

                var buttonElements = featureAttributesElement.find('.featureAttributeButton');
                var buttonElementAttributes = _.map(buttonElements, function(buttonElement) {
                    var jqElement = jQuery(buttonElement);
                    return {
                        propertyId: jqElement.attr('data-propertyId'),
                        propertyValues: me._propertyValuesOfButtonElement(jqElement)
                    };
                });

                var selectionElements = featureAttributesElement.find('select.featureattributeChoice');
                var selectionElementAttributes = _.map(selectionElements, function(selectionElement) {
                   var jqElement = jQuery(selectionElement);
                    return {
                        propertyId: jqElement.attr('data-propertyId'),
                        propertyValues: me._propertyValuesOfSelectionElement(jqElement)
                    };
                });

                var multiCheckboxElements = featureAttributesElement.find('div.featureattributeChoice');
                var multiCheckboxElementAttributes = _.map(multiCheckboxElements, function(multiCheckboxElement) {
                   var jqElement = jQuery(multiCheckboxElement);
                    return {
                        propertyId: jqElement.attr('data-propertyId'),
                        propertyValues: me._propertyValuesOfMultiCheckboxElement(jqElement)
                    };
                });

                var dateElements = featureAttributesElement.find('.featureAttributeDate');
                var dateElementAttributes = _.map(dateElements, function(dateElement) {
                    var jqElement = jQuery(dateElement);
                    return {
                        propertyId: jqElement.attr('data-propertyId'),
                        propertyValues: me._propertyValuesOfDateElement(jqElement)
                    };
                });

                var saveAsset = function(data) {
                    var properties = _.chain(data)
                        .map(function(attr) {
                            return {id: attr.propertyId,
                                values: attr.propertyValues};
                        })
                        .map(function(p) {
                            if (p.id == 200 && _.isEmpty(p.values)) {
                                p.values = [{propertyDisplayValue: "Pysäkin tyyppi",
                                    propertyValue: 99}];
                            }
                            return p;
                        })
                        .value();
                    me._backend.createAsset(
                        {assetTypeId: 10,
                            lon: me._selectedAsset.lon,
                            lat: me._selectedAsset.lat,
                            roadLinkId:  me._selectedAsset.roadLinkId,
                            bearing:  me._selectedAsset.bearing,
                            properties: properties});
                };

                saveAsset(textElementAttributes
                    .concat(selectionElementAttributes)
                    .concat(buttonElementAttributes)
                    .concat(multiCheckboxElementAttributes)
                    .concat(dateElementAttributes));
            });
        },
        _getStreetView: function(assetPosition) {
            var wgs84 = OpenLayers.Projection.transform(
                new OpenLayers.Geometry.Point(assetPosition.lonLat.lon, assetPosition.lonLat.lat),
                new OpenLayers.Projection('EPSG:3067'), new OpenLayers.Projection('EPSG:4326'));
            return this._templates.streetViewTemplate({ wgs84X: wgs84.x, wgs84Y: wgs84.y, heading: (assetPosition.validityDirection === 3 ? assetPosition.bearing - 90 : assetPosition.bearing + 90) });
        },
        _addDatePickers: function () {
            var $validFrom = jQuery('.featureAttributeDate[data-propertyId=validFrom]');
            var $validTo = jQuery('.featureAttributeDate[data-propertyId=validTo]');
            if ($validFrom.length > 0 && $validTo.length > 0) {
                dateutil.addDependentDatePickers($validFrom, $validTo);
            }
        },
        _propertyValuesOfTextElement: function(element) {
            return [{
                propertyValue : 0,
                propertyDisplayValue : element.val()
            }];
        },
        _propertyValuesOfButtonElement: function(element) {
            return [{
                propertyValue: element.attr('value') == 2 ? 2 : 3,
                propertyDisplayValue: element.attr('name')
            }];
        },
        _propertyValuesOfSelectionElement: function(element) {
            return [{
                propertyValue : Number(element.val()),
                propertyDisplayValue : element.attr('name')
            }];
        },
        _propertyValuesOfMultiCheckboxElement: function(element) {
            return _.chain(element.find('input'))
                    .filter(function(childElement) {
                        return $(childElement).is(':checked');
                    })
                    .map(function(childElement) {
                      return {
                        propertyValue: Number(childElement.value),
                        propertyDisplayValue: element.attr('name')
                      };
                    })
                    .value();
        },

        _propertyValuesOfDateElement: function(element) {
            return _.isEmpty(element.val()) ? [] : [{
                propertyValue : 0,
                propertyDisplayValue : dateutil.finnishToIso8601(element.val())
            }];
        },
        _getPropertyValues: function() {
            var me = this;
            me._backend.getEnumeratedPropertyValues(10);
        },
        _savePropertyData: function(propertyValue, propertyId) {
console.log("PROPVAL: " + JSON.stringify(propertyValue));
console.log("PROPID: " + propertyId);
            if (propertyId == 200 && _.isEmpty(propertyValue)) {
                propertyValue = [{
                    propertyDisplayValue: "Pysäkin tyyppi",
                    propertyValue: 99
                }];
            }

            var me = this;
            me._backend.putAssetPropertyValue(this._featureDataAssetId, propertyId, propertyValue);
        },

        _makeContent: function(contents) {
            var me = this;
            var html = "";
            _.forEach(contents,
                function (feature) {
                    var propertyType = feature.propertyType;
                    if (propertyType === "text" || propertyType === "long_text") {
                        feature.propertyValue = "";
                        feature.propertyDisplayValue = "";
                        if (feature.values[0]) {
                            feature.propertyValue = feature.values[0].propertyValue;
                            feature.propertyDisplayValue = feature.values[0].propertyDisplayValue;
                        }
                        html += me._getTextTemplate(propertyType, feature);
                    } else if (propertyType === "read_only_text") {
                        feature.propertyValue = "";
                        feature.propertyDisplayValue = "";
                        if (feature.values[0]) {
                            feature.propertyValue = feature.values[0].propertyValue;
                            feature.propertyDisplayValue = feature.values[0].propertyDisplayValue;
                        }
                        html += me._templates.featureDataTemplateReadOnlyText(feature);
                    } else if (propertyType === "single_choice" && feature.propertyId !== 'validityDirection') {
                        feature.propertyValue = me._getSelect(feature.propertyName, feature.values, feature.propertyId, '');
                        html += me._templates.featureDataTemplate(feature);
                    } else if (feature.propertyId === 'validityDirection') {
                        feature.propertyValue = 2;
                        if (feature.values[0]) {
                            feature.propertyValue = feature.values[0].propertyValue === 2 ? 3 : 2;
                        }
                        html += me._templates.featureDataTemplateButton(feature);
                    } else if (feature.propertyType === "multiple_choice") {
                        feature.propertyValue = me._getMultiCheckbox(feature.propertyName, feature.values, feature.propertyId);
                        html += me._templates.featureDataTemplate(feature);
                    } else if (propertyType === "date") {
                        feature.propertyValue = "";
                        feature.propertyDisplayValue = "";
                        if (feature.values[0]) {
                            feature.propertyValue = dateutil.iso8601toFinnish(feature.values[0].propertyDisplayValue);
                            feature.propertyDisplayValue = dateutil.iso8601toFinnish(feature.values[0].propertyDisplayValue);
                        }
                        html += me._templates.featureDataTemplateDate(feature);
                    }  else {
                        feature.propertyValue ='Ei toteutettu';
                        html += me._templates.featureDataTemplateNA(feature);
                    }
                }
            );
            return html;
        },
        _getTextTemplate: function(propertyType, feature) {
            return propertyType === "long_text" ? this._templates.featureDataTemplateLongText(feature) : this._templates.featureDataTemplateText(feature);
        },
        _getSelect: function(name, values, propertyId, multiple) {
            var me = this;
            var options = '<select data-propertyId="'+propertyId+'" name="'+name+'" class="featureattributeChoice" ' + multiple +'>';
            var valuesNro = _.map(values, function(x) { return x.propertyValue;});
            var propertyValues = _.find(me._enumeratedPropertyValues, function(property) { return property.propertyId === propertyId; });
            _.forEach(propertyValues.values,
                function(optionValue) {
                    var selectedValue ='';
                    if (_.contains(valuesNro, optionValue.propertyValue)) {
                        selectedValue = 'selected="true" ';
                    }
                    optionValue.selectedValue = selectedValue;
                    options +=  me._templates.featureDataTemplateChoice(optionValue);
                }
            );

            options +="</select>";
            return options;
        },

        _getMultiCheckbox: function(name, values, propertyId) {
            var invalidValues = [99];
            var me = this;
            var checkboxes = '<div data-propertyId="' + propertyId +
                '" name="' + name +
                '" class="featureattributeChoice">';
            var valuesNro = _.pluck(values, 'propertyValue');
            var propertyValues = _.find(me._enumeratedPropertyValues, function(property) { return property.propertyId === propertyId; });
            _.forEach(propertyValues.values,
                function(inputValue) {
                    if (!_.contains(invalidValues, inputValue.propertyValue)) {
                        var checkedValue = '';
                        if (_.contains(valuesNro, inputValue.propertyValue)) {
                            checkedValue = 'checked ';
                        }
                        inputValue.checkedValue = checkedValue;
                        inputValue.propertyId = propertyId;
                        inputValue.name = propertyId + '_' + inputValue.propertyValue;
                        checkboxes +=  me._templates.featureDataTemplateCheckbox(inputValue);
                    }
                }
            );

            checkboxes +="</div>";
            return checkboxes;
        },
        onEvent : function(event) {
            var me = this;
            var handler = me.eventHandlers[event.getName()];
            if(handler) {
                return handler.apply(this, [event]);
            }
            return undefined;
        },
        _closeAsset: function() {
            jQuery("#featureAttributes").html('');
            dateutil.removeDatePickersFromDom();
        },
        _directionChange: function () {
            var validityDirection = jQuery("[data-propertyid='validityDirection']");
            var selection = parseInt(validityDirection.attr('value'), 10);
            var propertyValues = [{propertyDisplayValue: "Vaikutussuunta", propertyValue: selection }];
            this._savePropertyData(propertyValues, 'validityDirection');
        },
        stop : function() {
            var me = this;
            var sandbox = this.sandbox;
            for(var p in me.eventHandlers) {
                if(me.eventHandlers.hasOwnProperty(p)) {
                    sandbox.unregisterFromEventByName(me, p);
                }
            }
            me.sandbox.unregister(me);
            me.started = false;
        }
    }, {
        protocol : ['Oskari.bundle.BundleInstance', 'Oskari.mapframework.module.Module']
    });

