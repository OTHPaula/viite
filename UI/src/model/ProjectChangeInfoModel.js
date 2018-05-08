(function(root) {
  root.ProjectChangeInfoModel = function(backend) {

    var roadInfoList=[{endAddressM:1,endRoadPartNumber:0,roadNumber:0,startAddressM:0,startRoadPartNumber:0,trackCode:0}];
    var changesInfo=[{changetype:0,discontinuity:"jatkuva",roadType:9,source:roadInfoList,target:roadInfoList,reversed: false}];
    var projectChanges={id:0,name:"templateproject", user:"templateuser",ely:0,changeDate:"1980-01-28",changeInfoSeq:changesInfo};



    function getChanges(projectID){
      backend.getChangeTable(projectID,function(changedata) {
        var parsedResult=roadChangeAPIResultParser(changedata);
        if (!_.isUndefined(parsedResult) && parsedResult.discontinuity !== null) {
          eventbus.trigger('projectChanges:fetched', parsedResult);
        }
      });
    }

    function roadChangeAPIResultParser(changeData) {
      projectChanges=changeData;
      return projectChanges;
    }

    return{
      roadChangeAPIResultParser: roadChangeAPIResultParser,
      getChanges: getChanges
    };
  };
})(this);