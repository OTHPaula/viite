(function (root) {
    root.RoadNamingToolWindow = function (roadNameCollection) {

        var newId = -1000;

        var nameToolSearchWindow = $('<div id="name-search-window" class="form-horizontal naming-list"></div>').hide();
        nameToolSearchWindow.append('<button class="close btn-close" id="closeRoadNameTool">x</button>');
        nameToolSearchWindow.append('<div class="content">Tienimi</div>');
        nameToolSearchWindow.append('<div class="name-tool-content-new">' +
            '<label class="name-tool-content-new label">Tie</label>' +
            '<div class = "panel-header">' +
            '<input type="text" class="road-input" title="Tie Nimi" id="roadSearchParameter">' +
            '<div id="buttons-div" style="display: inline-flex;">' +
            '<button id="executeRoadSearch" class="btn btn-sm btn-primary button-spacing">Hae</button>' +
            //Regular display: inline-block
            '<button id="createRoad" class="btn btn-sm btn-primary" style="display: none">Luo Tie</button>' +
            '</div>' +
            '<div id="table-labels">' +
            '<label class="content-new label" style="width:138px">Tie</label>' +
            '<label class="content-new label" style="width: 242px">Tien nimi</label>' +
            '<label class="content-new label" style="width: 100px">Alkupvm</label>' +
            '<label class="content-new label" style="width: 100px">Loppupvm</label>' +
            '</div>' +
            '</div>');

        nameToolSearchWindow.append('<div id="road-list" style="width:810px; height:400px; overflow:auto;"></div>');

        var staticFieldRoadNumber = function (dataField, roadId, fieldName) {
            var field;
            field = '<div sty>' +
                '<input class="input-road-details-readonly" style="width: 110px" value="' + dataField + '" data-FieldName="' + fieldName + '" readonly >' +
                '</div>';
            return field;
        };

        var staticFieldRoadList = function (dataField, writable, roadId, fieldName) {
            var field;
            var inputClass = (writable ? "form-control" : "input-road-details-readonly");
            var readOnly = (writable ? "" : "readonly");
            var leftMargin = (writable ? "margin-left: 8px;" : "");
            if ((fieldName === "startDate" || fieldName === "endDate") && writable) {
                field = '<div id="addDatePickerHere" value="' + dataField + '" data-roadId="' + roadId + '" data-FieldName="' + fieldName + '">' +
                    '<input id="addDatePickerToInputHere" class="' + inputClass + '" value="' + dataField + '" ' + readOnly + ' data-roadId="' + roadId + '" data-FieldName="' + fieldName + '" style="margin-top: 0px; ' + leftMargin + ' width: 85%">' +
                    '</div>';
            } else {
                field = '<div>' +
                    '<input class="' + inputClass + '" value="' + dataField + '" ' + readOnly + ' data-roadId="' + roadId + '" data-FieldName="' + fieldName + '" style="margin-top: 0px; ' + leftMargin + ' width: 85%">' +
                    '</div>';
            }
            return field;
        };

        var addSaveEvent = function () {
            var saveButton = '<button id="saveChangedRoads" class="btn btn-primary save btn-save-road-data">Tallenna</button>';
            $('#road-list').append(saveButton);
            $('#saveChangedRoads').on('click', function (clickEvent) {
                roadNameCollection.saveChanges();
            });
        };

        var retroactivelyAddDatePickers = function () {
            var inputs = $('#addDatePickerToInputHere:not([placeholder])');
            dateutil.addSingleDependentDatePicker(inputs);
            $('.pika-single.is-bound').css("width", "auto")
        };

        function toggle() {
            $('.container').append('<div class="modal-overlay confirm-modal"><div class="modal-dialog"></div></div>');
            $('.modal-dialog').append(nameToolSearchWindow.toggle());
            bindEvents();
        }

        function hide() {
            nameToolSearchWindow.hide();
            $('#saveChangedRoads').remove();
            $('.modal-overlay').remove();
        }

        function bindEvents() {
            eventbus.on("namingTool: toggleCreate", function () {
                if ($("#createRoad").is(":visible")) {
                    $("#createRoad").css({display: "inline-block"});
                } else {
                    $("#createRoad").css({display: "none"});
                }
            });

            nameToolSearchWindow.on('click', 'button.close', function () {
                $('.roadList-item').remove();
                roadNameCollection.clearBoth();
                hide();
            });

            nameToolSearchWindow.on('click', '#executeRoadSearch', function () {
                var roadParam = $('#roadSearchParameter').val();
                $('.roadList-item').remove();
                $('#saveChangedRoads').remove();
                roadNameCollection.fetchRoads(roadParam);
            });

            eventbus.on("roadNameTool: roadsFetched", function (roadData) {
                var html = '<table id="roadList-table" style="align-content: left;align-items: left;table-layout: fixed;width: 100%;">';
                if (!_.isEmpty(roadData)) {
                    _.each(roadData, function (road) {
                        var writable = road.endDate === "";
                        html += '<tr class="roadList-item">' +
                            '<td style="width: 150px;">' + staticFieldRoadNumber(road.roadNumber, road.id) + '</td>' +
                            '<td style="width: 250px;">' + staticFieldRoadList(road.roadNameFi, writable, road.id, "roadName") + '</td>' +
                            '<td style="width: 110px;">' + staticFieldRoadList(road.startDate, false, road.id, "startDate") + '</td>' +
                            '<td style="width: 110px;">' + staticFieldRoadList(road.endDate, writable, road.id, "endDate") + '</td>';
                        if (road.endDate === "") {
                            html += '<td>' + '<div id="plus_minus_buttons" data-roadId="' + road.id + '" data-roadNumber="' + road.roadNumber + '"><button class="project-open btn btn-new" style="alignment: middle; margin-bottom:6px; margin-left: 10px" id="new-road-name" data-roadId="' + road.id + '" data-roadNumber="' + road.roadNumber + '">+</button></div>' + '</td>' +
                                '</tr>' + '<tr style="border-bottom:1px solid darkgray; "><td colspan="100%"></td></tr>';
                        } else {
                            html += '<td>' + '<button class="project-open btn btn-new" style="visibility:hidden; alignment: right; margin-bottom:6px; margin-left: 70px" id="spaceFillerButton">+</button>' + '</td>' +
                                '</tr>' + '<tr style="border-bottom:1px solid darkgray; "><td colspan="100%"></td></tr>';
                        }
                    });
                    html += '</table>';
                    $('#road-list').html($(html));
                    retroactivelyAddDatePickers();

                    addSaveEvent();
                    $('.form-control').on("change", function (eventObject) {
                        var target = $(eventObject.target);
                        var roadId = target.attr("data-roadId");
                        var fieldName = target.attr("data-FieldName");
                        var fieldValue = target.val();
                        roadNameCollection.registerEdition(roadId, fieldName, fieldValue);
                    });

                    $('#new-road-name').on("click", function (eventObject) {
                        var target = $(eventObject.target);
                        target.css("visibility", "hidden");
                        var originalRoadId = target.attr("data-roadId");
                        var roadNumber = target.attr("data-roadNumber");
                        $('#roadList-table').append('<tr class="roadList-item" id="newRoadName" data-originalRoadId ="' + originalRoadId + '" data-roadNumber="' + roadNumber + '">' +
                            '<td style="width: 150px;">' + staticFieldRoadNumber(roadNumber, newId) + '</td>' +
                            '<td style="width: 250px;">' + staticFieldRoadList("", true, newId, "roadName") + '</td>' +
                            '<td style="width: 110px;">' + staticFieldRoadList("", true, newId, "startDate") + '</td>' +
                            '<td style="width: 110px;">' + staticFieldRoadList("", true, newId, "endDate") + '</td>' +
                            '<td>' + '<div id="plus_minus_buttons" data-roadId="' + newId + '" data-roadNumber="' + roadNumber + '"><button class="project-open btn btn-new" style="alignment: middle; margin-bottom:6px; margin-left: 10px" id="undo-new-road-name" data-roadId="' + originalRoadId + '" data-roadNumber="' + roadNumber + '">-</button></div>' + '</td>' +
                            '</tr>' + '<tr style="border-bottom:1px solid darkgray; "><td colspan="100%"></td></tr>');
                        retroactivelyAddDatePickers();
                        $('#undo-new-road-name').on("click", function (eventObject) {
                            var target = $(eventObject.target);
                            var roadId = target.attr("data-roadId");
                            var roadNumber = target.attr("data-roadNumber");
                            $('#new-road-name[data-roadid|=' + roadId + '][data-roadnumber|=' + roadNumber + ']').css("visibility", "visible");
                            $('#newRoadName[data-originalRoadId|=' + roadId + '][data-roadnumber|=' + roadNumber + ']').remove();
                        });
                    });

                } else {
                    html += '</table>';
                    $('#road-list').html($(html));
                    retroactivelyAddDatePickers();
                }
            });
        }

        return {
            toggle: toggle,
            hide: hide,
            element: nameToolSearchWindow,
            bindEvents: bindEvents
        };

    };
})(this);