
var http = require('http');
var request = require('request');
var url = require('url');

const fs = require('fs');

// create a webserver
http.createServer(function (req, res) {    

    if ((req.url).toString().indexOf('favicon.ico')>=0){
        res.end('');
    } else {

        console.log("created http server");

        const URL_prefix = 'https://ripda.dhis.dhmis.org/staging';

        var username = "Greg_Rowles";
        var password = "Hispian@1";
        var auth = "Basic " + new Buffer(username + ":" + password).toString("base64");
        var orgUnit_id = 'oehzJnthZZD'; //fs Mafube LM
        var PEcr = '2017Q1';
        var ouLevel = '';
        var dxArrpairs = [];

        if (((req.url).toString().split('?')[1]) != undefined){

            var q = ((req.url).toString().split('?')[1]).split('&');

            for(var i=0; i<q.length; i++) {
                var arrP = (q[i]).split('=');
                if (arrP[0] == 'pe'){
                    PEcr = arrP[1];
                }
                if (arrP[0] == 'ou'){
                    orgUnit_id = arrP[1];
                }
                if (arrP[0] == 'oulevel'){
                    ouLevel = arrP[1];
                }
            }

        }

        console.log((req.url).toString());

        var url = URL_prefix + '/api/dataSets.json?fields=id,name,dataSetElements[dataElement[id,name,attributeValues[value,attribute[id]]]]&filter=id:in:[wQ7XU962RIH,vv8ed5J7Frf,xjqRVGdYcu7]&filter=dataSetElements.dataElement.attributeValues.attribute.id:eq:TU0Z0GOyEV5';

        console.log("loading OU dataset (Elements)");

        request.get( {
            url : url,
            headers : {
                "Authorization" : auth
            }
        }, function(error, response, body) {

                var dxUIDs = '', dxNames = '';

                if (!error && response.statusCode === 200) {

                    for(var d=0; d<JSON.parse(body).dataSets.length; d++) {
                        for(var b=0; b<JSON.parse(body).dataSets[d].dataSetElements.length; b++) {
                            if ( (dxUIDs).indexOf(JSON.parse(body).dataSets[d].dataSetElements[b].dataElement.id+';') < 0){
                                dxUIDs += JSON.parse(body).dataSets[d].dataSetElements[b].dataElement.id + ';';
                                dxNames += JSON.parse(body).dataSets[d].dataSetElements[b].dataElement.name + ',';
                                for(var a=0; a<JSON.parse(body).dataSets[d].dataSetElements[b].dataElement.attributeValues.length; a++) {
                                    if (JSON.parse(body).dataSets[d].dataSetElements[b].dataElement.attributeValues[a].attribute.id == 'TU0Z0GOyEV5'){
                                        dxArrpairs.push ({ dx: JSON.parse(body).dataSets[d].dataSetElements[b].dataElement.id, qip: JSON.parse(body).dataSets[d].dataSetElements[b].dataElement.attributeValues[a].value })
                                    }
                                }
                            }
                        }
                    }

                    dxUIDs = dxUIDs.substring(0,dxUIDs.length-1);

                    console.log("calling Analytics API");

                    var peMM = getMMcriteria(PEcr);
                    var dtmTimeNow = ((new Date().toISOString().split('T')[1]).split('.')[0]).replace(/:/g,'');
                    var url = URL_prefix + '/api/26/analytics.json?dimension=WsZjXKlqUN0:lvOtc4VXYKo;qIZre0ATr0b;Jbh3wnNuN2j&dimension=pe:' + peMM + '&dimension=cPD0W9FikTR:LArumsK99c4;kHGxIFekzcG&dimension=dx:' + dxUIDs + '&dimension=ou:' + ((ouLevel.length > 0) ? 'LEVEL-'+ouLevel+';' : '') + orgUnit_id + '&displayProperty=NAME&outputIdScheme=UID&uniqueparm=' + dtmTimeNow;

                    //res.writeHead(200, {'Content-Type': 'text/html'});
                    //res.write("<div>"+url+"</div><br>");

                    request.get( {
                            url : url,
                            headers : {
                                "Authorization" : auth
                            }
                        }, function(error1, response1, body1) {

                            if (!error1 && response1.statusCode === 200) {
                                var data = loadDataArray(JSON.parse(body1),PEcr);
                                //res.writeHead(200, {'Content-Type': 'text/json'});
                                var fname = 'export/ripda_'+orgUnit_id+'_'+getyyyymmdd()+'_'+dtmTimeNow+'.json';
                                fs.writeFileSync(fname, data);
                                //res.writeHead(200, {'Content-Type': 'text/json'});
                                //res.end(data);
                                res.end('');
                            } else {
                                res.end('Error\n'+error1);
                            }
                        } );
                } else {
                    res.end('Error\n'+error);
                }

        });
    }

    function getDxQIPpair(dx){
        var sReturn = '';
        for(var s = 0; s < dxArrpairs.length; s++) {
            if (dxArrpairs[s].dx == dx){
                sReturn = dxArrpairs[s].qip;
                break;
            }
        }
        return sReturn;
    }

    function getMergedPEarr(peObj,qArr){
        var iStepCounter = 0, qI = 0, ArrRet = [];
        for(pe = 0; pe < peObj.length; pe++) {
            iStepCounter += 1;
            ArrRet.push ({ pe: peObj[pe], type: 'mm', alt: peObj[pe], name: peObj[pe] })
            if (iStepCounter == 3){
                ArrRet.push ({ pe: qArr[qI], type: 'q', alt: (qArr[qI]).toString().split('Q')[1], name: (qArr[qI]).toString().split('Q')[0]+'April' })
                qI += 1;
                iStepCounter = 0;
            }
        }
        return ArrRet;
    }

    function loadDataArray(myData,peC){

        var myArr = [];
        var qArr = peC.split(';');
        var dxArr = myData.metaData.dimensions.dx;
        var ouArr = myData.metaData.dimensions.ou;
        var peArr = getMergedPEarr(myData.metaData.dimensions.pe,qArr);
        var peSeq = getMMgroupSeq(peC);
        var dtmStamp = new Date().toISOString();

        console.log("Initiating data array");

        for(dx = 0; dx < dxArr.length; dx++) {
            var dxPair = getDxQIPpair(dxArr[dx]);
            if (dxPair.length > 0){
                for(ou = 0; ou < ouArr.length; ou++) {
                    for(pe = 0; pe < peArr.length; pe++) {
                        myArr.push({
                            dx:     dxArr[dx],
                            dataElement: dxPair,
                            pe:     peArr[pe].pe,
                            period: peArr[pe].name,
                            peType: peArr[pe].type,
                            q:      ( (peArr[pe].type == 'mm') ? peMMseq(peArr[pe].pe,peSeq) : (peArr[pe].pe).split('Q')[1]),
                            orgUnit:     ouArr[ou],
                            categoryOptionCombo: ( (peArr[pe].type != 'mm') ? ( ((peArr[pe].pe).toString().indexOf('Q1') > 0) ? 'EcMJ7gpxg6T' : ( ((peArr[pe].pe).toString().indexOf('Q2') > 0) ? 'I6unoam3wPR' : ( ((peArr[pe].pe).toString().indexOf('Q3') > 0) ? 'fOdVzDXDIWl' : ( ((peArr[pe].pe).toString().indexOf('Q4') > 0) ? 'MlxF1hgrSQZ' : '' ) ) ) ) : ''),
                            attributeOptionCombo: ( (peArr[pe].type == 'mm') ? '' : 'n2OgrayehoK' ),
                            source: "",    // LArumsK99c4
                            dhis:   "",    // kHGxIFekzcG
                            diff:   "",
                            value: "",
                            rowid: ouArr[ou] + '.' + dxArr[dx] + '.' + peArr[pe].pe, //+ '.' + ( (peArr[pe].type == 'mm') ? peMMseq(peArr[pe].pe,peSeq) : peArr[pe].pe )
                            seqGroup: ouArr[ou] + '.' + dxArr[dx] + '.' + ( (peArr[pe].type == 'mm') ? peMMseq(peArr[pe].pe,peSeq) : '_' + (peArr[pe].alt) ) , 
                            storedBy: "Greg_Rowles",
                            created: dtmStamp,
                            lastUpdated: dtmStamp,
                            followUp: false, 
                            delete: 0
                        });
                    }
                }
            }
        }

        console.log(" ~ rows: " + myArr.length);
        console.log("Loading data values");

        for(r = 0; r < myArr.length; r++) {
            for(i = 0; i < myData.rows.length; i++) {            
                if ( (myData.rows[i][0] == myArr[r].dx) && (myData.rows[i][4] == myArr[r].orgUnit) && (myData.rows[i][2] == myArr[r].pe) ) {
                    if (myData.rows[i][3] == 'LArumsK99c4') {
                        myArr[r]['source'] = parseFloat(myData.rows[i][5]);
                    }
                    if (myData.rows[i][3] == 'kHGxIFekzcG') {
                        myArr[r]['dhis'] = parseFloat(myData.rows[i][5]);
                    }
                    myArr[r]['categoryOptionCombo'] = myData.rows[i][4]; 
                }
            }
            if ( (myArr[r]['source']).length == 0){
                myArr[r]['diff'] = 0;
            } else {
                if ( (myArr[r]['dhis']).length == 0){
                    myArr[r]['diff'] = myArr[r]['source'];
                } else {
                    myArr[r]['diff'] = Math.abs(myArr[r]['source'] - myArr[r]['dhis']);
                }
            }
        }

        var iAggDhis = 0, iAggSource = 0, iAggDiff = 0;
        var lastSeqGroup = '';

        console.log("Loading Aggregate data values");

        for(r = 0; r < myArr.length; r++) {
            if ( (lastSeqGroup.length > 0) && (lastSeqGroup != myArr[r]['seqGroup']) ) {
                if ( ( (myArr[r]['seqGroup']).indexOf('_') > 0 ) && ( (myArr[r]['seqGroup']).replace('_','') == lastSeqGroup ) ){
                    myArr[r]['dhis'] = iAggDhis;
                    myArr[r]['source'] = iAggSource;
                    myArr[r]['diff'] = iAggDiff;
                    iAggDhis = 0, iAggSource = 0, iAggDiff = 0;
                    lastSeqGroup = '';
                }
            } else {
                lastSeqGroup = myArr[r]['seqGroup'];
                iAggDhis += ( ( (myArr[r]['dhis']).toString().length > 0 ) ? parseFloat(myArr[r]['dhis']) : 0);
                iAggSource += ( ( (myArr[r]['source']).toString().length > 0 ) ? parseFloat(myArr[r]['source']) : 0);
                iAggDiff += ( ( (myArr[r]['diff']).toString().length > 0 ) ? parseFloat(myArr[r]['diff']) : 0);
            }
        }

        var sRem = "", bRem = false;

        console.log("Enumerating missing values");

        for(r = 0; r < myArr.length; r++) {
            if (  (myArr[r].peType == 'mm') ) { //(((myArr[r]['source']).toString().length == 0) && ((myArr[r]['dhis']).toString().length == 0)) ||
                //sRem += (r + ','); // REMOVE ROWS WHERE NO VALUES EXIST
                bRem = true;
                myArr[r].delete = 1;
            } else {
                if ((myArr[r]['source']).toString().length == 0){
                    myArr[r]['value'] = 0;
                    //myArr[r]['diff'] = 0;
                    //sRem += (r + ','); // REMOVE CALCULATIONS WHERE NO AUDIT TOOK PLACE
                    bRem = true;
                    myArr[r].delete = 1;
                } else {
                    if ((myArr[r]['dhis']).toString().length == 0){
                        //myArr[r]['diff'] = myArr[r]['source'];
                    } else {
                        if (myArr[r]['source'] == 0){
                            myArr[r]['value'] = 0;
                            //sRem += (r + ','); // REMOVE ROWS WHERE NO VALUES EXIST
                            bRem = true;
                            myArr[r].delete = 1;
                        }
                    }
                    myArr[r]['value'] = (parseFloat(Math.abs(myArr[r]['diff']) / myArr[r]['source']) * 100).toFixed(1).toString().replace('.0','');
                }
            }
        }

        if (bRem == true){
            var countArr = myArr.length;
            console.log("Removing empty rows");
            for (i=myArr.length-1;i>=0;--i) {
                if (myArr[i].delete === 1) {
                    myArr.splice(i, 1); // Remove even numbers
                }
            }
            countArr = (countArr - myArr.length);
        }

        console.log("Resulting aggregate rows: " + myArr.length);

        myArr.forEach(function(v){ delete v.dx; delete v.pe; delete v.peType; delete v.q; delete v.rowid; delete v.source; delete v.dhis; delete v.diff; delete v.seqGroup; });

        return '{ "dataValues": ' + (JSON.stringify(myArr)) + ' }';

    }

    function LoadHTMLresultsTable(myData){

        var sReturn = '';
        sReturn += '<table class=""><thead><tr>';
        for(var i = 0; i < myData.headers.length; i++) {
            //sReturn += '<th class="">' + ( (myData.metaData.items[myData.headers[i].name].name == undefined) ? myData.headers[i].name : myData.metaData.items[myData.headers[i].name].name ) + '</th>';
            sReturn += '<th class="">' + myData.headers[i].name + '</th>';
        }
        sReturn += '</tr></tr></thead>';
        sReturn += '<tbody>';
        for(var i = 0; i < myData.rows.length; i++) {
            sReturn += '<tr>';
            for(var p = 0; p < myData.headers.length; p++) {
                if (myData.headers[p].name != 'value'){
                    sReturn += '<td title="' + myData.rows[i][p] + '" class="">' + myData.metaData.items[myData.rows[i][p]].name + '</td>';
                } else {
                    sReturn += '<td class="">' + myData.rows[i][p] + '</td>';
                }
            }
            sReturn += '</tr>';
        }
        sReturn += '</tbody></table>';
        return sReturn;
    }

    function getMMcriteria(qPe){

        var PEcr = '';
        var qArr = qPe.split(';');

        for(p = 0; p < qArr.length; p++) {
            var ArrT = (qArr[p]).split('Q');
            if (parseFloat(ArrT[1]) == 1){
                PEcr += (ArrT[0] + '01;' + ArrT[0] + '02;' + ArrT[0] + '03;');
            }
            if (parseFloat(ArrT[1]) == 2){
                PEcr += (ArrT[0] + '04;' + ArrT[0] + '05;' + ArrT[0] + '06;');
            }
            if (parseFloat(ArrT[1]) == 3){
                PEcr += (ArrT[0] + '07;' + ArrT[0] + '08;' + ArrT[0] + '09;');
            }
            if (parseFloat(ArrT[1]) == 4){
                PEcr += (ArrT[0] + '10;' + ArrT[0] + '11;' + ArrT[0] + '12;');
            }
        }

        return PEcr.substring(0,PEcr.length-1);

    }

    function getMMgroupSeq(qPe){

        var PEcr = [], qMM = '';
        var qArr = qPe.split(';');

        for(p = 0; p < qArr.length; p++) {
            var ArrT = (qArr[p]).split('Q');
            if (parseFloat(ArrT[1]) == 2){
                qMM = (ArrT[0] + '04;' + ArrT[0] + '05;' + ArrT[0] + '06;');
                PEcr.push ({ name: qArr[p], seq: 1, q: ArrT[1], qMM: qMM });
            }
            if (parseFloat(ArrT[1]) == 3){
                qMM = (ArrT[0] + '07;' + ArrT[0] + '08;' + ArrT[0] + '09;');
                PEcr.push ({ name: qArr[p], seq: 2, q: ArrT[1], qMM: qMM });
            }
            if (parseFloat(ArrT[1]) == 4){
                qMM = (ArrT[0] + '10;' + ArrT[0] + '11;' + ArrT[0] + '12;');
                PEcr.push ({ name: qArr[p], seq: 3, q: ArrT[1], qMM: qMM });
            }
            if (parseFloat(ArrT[1]) == 1){
                qMM = (ArrT[0] + '01;' + ArrT[0] + '02;' + ArrT[0] + '03;');
                PEcr.push ({ name: qArr[p], seq: 4, q: ArrT[1], qMM: qMM });
            }
        }

        return PEcr;

    }

    function peMMseq(pe,ArrM){
        for(i = 0; i < ArrM.length; i++) {
            if ( (ArrM[i].qMM).indexOf(pe) >= 0 ){
                return ArrM[i].q;
            }
        }
    }

    function getyyyymmdd(){
        var x = new Date();
        var y = x.getFullYear().toString();
        var m = (x.getMonth() + 1).toString();
        var d = x.getDate().toString();
        (d.length == 1) && (d = '0' + d);
        (m.length == 1) && (m = '0' + m);
        return (y + m + d);
    }

}).listen(1440);

// log what that we started listening on localhost:1440
console.log('Server running at 127.0.0.1:1440');