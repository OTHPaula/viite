(function(ActionPanel) {

    var panelControl =
        '<div class="panelControl">' +
                        '<div class="panelControlLine"></div>' +
                        '<div class="panelControlLine"></div>' +
                        '<div class="panelControlLine"></div>' +
                    '</div>'+
            '<div class="panelLayerGroup"></div>';


    jQuery("#maptools").append(panelControl);
    AssetActionPanel('asset', 'Joukkoliikenteen pysäkit', true, 'bus-stop.png');
    AssetActionPanel('linearAsset', 'Nopeusrajoitukset', false, 'speed-limit.png');
    eventbus.trigger('layer:selected','asset');
    Backend.getUserRoles();

    var editMessage = $('<div class="editMessage">Muokkaustila: muutokset tallentuvat automaattisesti</div>');
    jQuery(".container").append(editMessage.hide());

    var handleEditMessage = function(readOnly) {
        if(readOnly) {
            editMessage.hide();
        } else {
            editMessage.show();
        }
    };

    eventbus.on('application:readOnly', handleEditMessage);

}(window.ActionPanel = window.ActionPanel));
