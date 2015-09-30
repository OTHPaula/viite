(function(root) {
  root.PiecewiseLinearAssetFormElements = function(unit, editControlLabels, className, defaultValue) {
    return {
      singleValueElement: singleValueElement,
      bindEvents: bindEvents,
      bindMassUpdateDialog: bindMassUpdateDialog
    };

    function generateClassName(sideCode) {
      return sideCode ? className + '-' + sideCode : className;
    }

    function singleValueElement(selectedLinearAsset, sideCode) {
      var withoutValue = selectedLinearAsset.isUnknown() ? 'checked' : '';
      var withValue = selectedLinearAsset.isUnknown() ? '' : 'checked';

      var readOnlyFormGroup = '' +
        '<div class="form-group read-only">' +
          '<label class="control-label">' + editControlLabels.title + '</label>' +
          '<p class="form-control-static ' + className + '">' + valueString(selectedLinearAsset) + '</p>' +
        '</div>';

      var editableFormGroup = '' +
        '<div class="form-group editable">' +
          sideCodeMarker(sideCode) +
          '<label class="control-label">' + editControlLabels.title + '</label>' +
          '<div class="choice-group">' +
            '<div class="radio">' +
              '<label>' + editControlLabels.disabled +
                '<input ' + 
                  'class="' + generateClassName(sideCode) + '" ' +
                  'type="radio" name="' + generateClassName(sideCode) + '" ' +
                  'value="disabled" ' + withoutValue + '/>' +
              '</label>' +
            '</div>' +
            '<div class="radio">' +
              '<label>' + editControlLabels.enabled +
                '<input ' +
                  'class="' + generateClassName(sideCode) + '" ' +
                  'type="radio" name="' + generateClassName(sideCode) + '" ' +
                  'value="enabled" ' + withValue + '/>' +
              '</label>' +
            '</div>' +
            measureInput(selectedLinearAsset, sideCode) +
          '</div>' +
        '</div>';

      return readOnlyFormGroup + editableFormGroup;
    }

    function sideCodeMarker(sideCode) {
      if (_.isUndefined(sideCode)) {
        return '';
      } else {
        return '<span class="marker">' + sideCode + '</span>';
      }
    }

    function bindMassUpdateDialog(rootElement) {
      var inputElement = rootElement.find('.input-unit-combination input.' + className);
      var toggleElement = rootElement.find('.radio input.' + className);
      function setValue(value){
        $('#hid').text(value);
      }
      var inputElementValue = function() {
        var removeWhitespace = function(s) {
          return s.replace(/\s/g, '');
        };
        var value = parseInt(removeWhitespace(inputElement.val()), 10);
        return _.isFinite(value) ? value : undefined;
      };

      inputElement.on('input', function() {
        setValue(inputElementValue());
      });

      toggleElement.on('change', function(event) {
        var disabled = $(event.currentTarget).val() === 'disabled';
        inputElement.prop('disabled', disabled);
        if (disabled) {
          setValue('');
        } else {
          setValue(inputElementValue());
        }
      });
    }

    function bindEvents(rootElement, selectedLinearAsset, sideCode) {
      var inputElement = rootElement.find('.input-unit-combination input.' + generateClassName(sideCode));
      var toggleElement = rootElement.find('.radio input.' + generateClassName(sideCode));
      var valueSetters = {
        a: selectedLinearAsset.setAValue,
        b: selectedLinearAsset.setBValue
      };
      var setValue = valueSetters[sideCode] || selectedLinearAsset.setValue;
      var valueRemovers = {
        a: selectedLinearAsset.removeAValue,
        b: selectedLinearAsset.removeBValue
      };
      var removeValue = valueRemovers[sideCode] || selectedLinearAsset.removeValue;

      var inputElementValue = function() {
        var removeWhitespace = function(s) {
          return s.replace(/\s/g, '');
        };
        var value = parseInt(removeWhitespace(inputElement.val()), 10);
        return _.isFinite(value) ? value : undefined;
      };

      inputElement.on('input', function(event) {
        setValue(inputElementValue());
      });

      toggleElement.on('change', function(event) {
        var disabled = $(event.currentTarget).val() === 'disabled';
        inputElement.prop('disabled', disabled);
        if (disabled) {
          removeValue();
        } else {
          var value = unit ? inputElementValue() : defaultValue;
          setValue(value);
        }
      });
    }

    function valueString(selectedLinearAsset) {
      if (unit) {
        return selectedLinearAsset.getValue() ? selectedLinearAsset.getValue() + ' ' + unit : '-';
      } else {
        return selectedLinearAsset.isUnknown() ? 'ei ole' : 'on';
      }
    }

    function measureInput(selectedLinearAsset, sideCode) {
      if (unit) {
        var value = selectedLinearAsset.getValue() ? selectedLinearAsset.getValue() : '';
        var disabled = selectedLinearAsset.isUnknown() ? 'disabled' : '';
        return '' +
          '<div class="input-unit-combination input-group">' +
            '<input ' +
              'type="text" ' +
              'class="form-control ' + generateClassName(sideCode) + '" ' +
              'value="' + value  + '" ' + disabled + ' >' +
            '<span class="input-group-addon">' + unit + '</span>' +
          '</div>';
      } else {
        return '';
      }
    }
  };
})(this);
