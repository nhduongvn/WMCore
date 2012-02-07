WMStats.namespace("RequestView")

WMStats.RequestView = (function() {
    
    var _data = null;
    var _containerDiv = null;
    var _url = WMStats.Globals.couchDBViewPath + 'campaign-request';
    var _options = {'include_docs': true};
    var _tableID = "requestTable";
    
   
    
        
    function _getOrDefault(baseObj, objList, val) {
        
        if (baseObj[objList[0]]) { 
            if (objList.length == 1) {
                return baseObj[objList[0]];
            } else {
                return _getOrDefault(baseObj[objList[0]], objList.slice(1), val);
            }
        } else {
            return val;
        } 
    }
    
    function _get(baseObj, objStr, val) {
        objList = objStr.split('.');
        return _getOrDefault(baseObj, objList, val); 
    }
    
    var tableConfig = {
        "aoColumns": [
            { "mDataProp": "workflow", "sTitle": "workflow"},
            { "mDataProp": "requestor", "sTitle": "requestor"},
            { "mDataProp": "request_type", "sTitle": "type"},
            { "mDataProp": "inputdataset", "sTitle": "inputdataset"},
            { "mDataProp": "site_white_list", "sTitle": "site white list"},
            //{ "mDataProp": "status.inWMBS", "sTitle": "in wmbs", 
            //               "sDefaultContent": 0, "bVisible": false},
            /*
            { "mDataProp": "status.queued.first", "sTitle": "queued first", 
                           "sDefaultContent": 0 , "bVisible": false},
            { "mDataProp": "status.queued.retry", "sTitle": "queued retry", 
                           "sDefaultContent": 0, "bVisible": false },
            */
            { "sTitle": "queued", 
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.queued.first", 0) + 
                                    _get(o.aData, "status.queued.retry", 0));
                          }
            },
            /*               
            { "mDataProp": "status.submitted.first", "sTitle": "submitted first", 
                           "sDefaultContent": 0, "bVisible": false },
            { "mDataProp": "status.submitted.retry", "sTitle": "submitted retry", 
                           "sDefaultContent": 0, "bVisible": false },
            */
            { "sTitle": "pending", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.pending", 0);
                          }
            },
            { "sTitle": "running", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.submitted.running", 0);
                          }
            },
            
            /*
            { "mDataProp": "status.failure.create", "sTitle": "create fail", 
                           "sDefaultContent": 0, "bVisible": false  },
            { "mDataProp": "status.failure.submit", "sTitle": "submit fail", 
                           "sDefaultContent": 0, "bVisible": false },
            { "mDataProp": "status.failure.exception", "sTitle": "exception fail", 
                           "sDefaultContent": 0, "bVisible": false },
            */
            { "sTitle": "failure",
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.failure.create", 0) + 
                                    _get(o.aData, "status.failure.submit", 0) + 
                                    _get(o.aData, "status.failure.exception", 0));
                          }
            },
            
            { "sTitle": "canceled", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.canceled", 0);
                          }},
            { "sTitle": "success",
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.success", 0);
                          }},
            { "sTitle": "cool off", 
              "fnRender": function ( o, val ) {
                            return _get(o.aData, "status.cooloff", 0);
                          }
            },
            { "sTitle": "pre-cooloff",
              "fnRender": function ( o, val ) {
                            return (_get(o.aData, "status.submitted.retry", 0) + 
                                    _get(o.aData, "status.queued.retry", 0));
                          }
            },
            /*
            { "mDataProp": "total_jobs", "sTitle": "total estimate jobs", 
                           "sDefaultContent": 0, "bVisible": false},
            */
            { "sTitle": "queue injection",  
              "fnRender": function ( o, val ) {
                              return (_get(o.aData, "tatus.inWMBS",  0) / 
                                      _get(o.aData, 'total_jobs', 1));
                          }}
            //TODO add more data
        ]
    }

    function getData() {
        return _data;
    }
    
    var keysFromIDs = function(data) {
        var keys = [];
        for (var i in data.rows){
            keys.push(data.rows[i].id);
        }
        //TODO not sure why JSON.stringify cause the problem
        return keys;      
    }   
                
    var getRequestDetailsAndCreateTable = function (agentIDs, reqmgrData) {
        var options = {'keys': keysFromIDs(agentIDs), 'reduce': false, 'include_docs': true};
        //TODO need to change to post call
        var url = WMStats.Globals.couchDBViewPath + 'latest-request'
        $.get(url, options,
              function(agentData) {
                  //combine reqmgrData(reqmgr_request) and agent_request(agentData) data 
                  var requestCache = WMStats.Requests()
                  requestCache.updateBulkRequests(reqmgrData.rows)
                  requestCache.updateBulkRequests(agentData.rows)
                  
                  // set the data cache
                  _data = requestCache.getList();
                  
                  //create table
                  tableConfig.aaData = _data;
                  var selector = _containerDiv + " table#" + _tableID;
                  return WMStats.Table(tableConfig).create(selector)
              },
              'json')
    }
    
    var getLatestRequestIDsAndCreateTable = function (reqmgrData) {
        /*
         * get list of request ids first from the couchDB then get the details of the requests.
         * This is due to the reduce restiction on couchDB - can't be one http call. 
         */
    
        var options = {"keys": keysFromIDs(reqmgrData), "reduce": true, 
                       "group": true, "descending": true};
        //TODO need to change to post call
        var url = WMStats.Globals.couchDBViewPath + 'latest-request';
        $.get(url, options,
              function(agentIDs) {
                  getRequestDetailsAndCreateTable(agentIDs, reqmgrData)
              },
              'json')
    }
    
    
    function createTable(selector, url, options) {
        if (!url) {url = _url;}
        if (!options) {options = _options;}
        _containerDiv = selector;
        
        $(selector).html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="'+ _tableID + '"></table>' );
        $.get(url, options, getLatestRequestIDsAndCreateTable, 'json')
    }
    
    return {'getData': getData, 'createTable': createTable};
    
     
})();
    