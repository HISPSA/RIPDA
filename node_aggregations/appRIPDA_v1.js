
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

        var url = URL_prefix + '/api/dataSets.json?fields=id,name,dataSetElements[dataElement[id,name,attributeValues[value,attribute[id]]]]&filter=id:in:[wQ7XU962RIH,vv8ed5J7Frf,xjqRVGdYcu7]';

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
                                if (JSON.parse(body).dataSets[d].dataSetElements[b].dataElement.attributeValues.length>0){
                                    if (JSON.parse(body).dataSets[d].dataSetElements[b].dataElement.attributeValues[0].attribute.id == 'TU0Z0GOyEV5'){
                                        dxArrpairs.push ({ dx: JSON.parse(body).dataSets[d].dataSetElements[b].dataElement.id, qip: JSON.parse(body).dataSets[d].dataSetElements[b].dataElement.attributeValues[0].value })
                                    }
                                }
                            }
                        }
                    }

                    dxUIDs = dxUIDs.substring(0,dxUIDs.length-1);

                    var peMM = getMMcriteria(PEcr);
                    var dtmTimeNow = ((new Date().toISOString().split('T')[1]).split('.')[0]).replace(/:/g,'');
                    var url = URL_prefix + '/api/26/analytics.json?dimension=WsZjXKlqUN0:lvOtc4VXYKo;qIZre0ATr0b;Jbh3wnNuN2j&dimension=pe:' + peMM + '&dimension=cPD0W9FikTR:LArumsK99c4;kHGxIFekzcG&dimension=dx:' + dxUIDs + '&dimension=ou:' + ((ouLevel.length > 0) ? 'LEVEL-'+ouLevel+';' : '') + orgUnit_id + '&displayProperty=NAME&outputIdScheme=UID&uniqueparm=' + dtmTimeNow;

                    res.writeHead(200, {'Content-Type': 'text/html'});
                    res.write("<div>"+url+"</div><br>");

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

    function loadDataArray(myData,peC){

        var myArr = [];
        var dxArr = myData.metaData.dimensions.dx;
        var ouArr = myData.metaData.dimensions.ou;
        var peArr = myData.metaData.dimensions.pe;
        var qArr = peC.split(';');
        var peSeq = getMMgroupSeq(peC);
        var dtmStamp = new Date().toISOString();

        for(dx = 0; dx < dxArr.length; dx++) {
            var dxPair = getDxQIPpair(dxArr[dx]);
            if (dxPair.length > 0){
                for(ou = 0; ou < ouArr.length; ou++) {
                    for(pe = 0; pe < peArr.length; pe++) {
                        myArr.push({
                            dx:     dxArr[dx],
                            dataElement: dxArr[dx],
                            pe:     peArr[pe],
                            period: peArr[pe],
                            peType: 'mm',
                            q:      peMMseq(peArr[pe],peSeq),
                            orgUnit:     ouArr[ou],
                            categoryOptionCombo: '',
                            attributeOptionCombo: '',
                            source: "",    // LArumsK99c4
                            dhis:   "",    // kHGxIFekzcG
                            diff:   "",
                            value: "",
                            rowid: ouArr[ou] + '.' + dxArr[dx] + '.' + peArr[pe] + '.' + peMMseq(peArr[pe],peSeq),
                            seqGroup: ouArr[ou] + '.' + dxArr[dx] + '.' + peMMseq(peArr[pe],peSeq),
                            storedBy: "Greg_Rowles",
                            created: dtmStamp,
                            lastUpdated: dtmStamp,
                            followUp: false
                        });
                    }
                    for(pe = 0; pe < qArr.length; pe++) {
                        myArr.push({
                            dx:     dxArr[dx],
                            dataElement: getDxQIPpair(dxArr[dx]),
                            pe:     qArr[pe],
                            period: ( ((qArr[pe]).toString().indexOf('Q') > 0) ? ((qArr[pe]).toString().split('Q')[0] + 'April') : qArr[pe] ),
                            peType: 'q',
                            q:      (qArr[pe]).split('Q')[1],
                            orgUnit:     ouArr[ou],
                            categoryOptionCombo: ( ((qArr[pe]).toString().indexOf('Q1') > 0) ? 'EcMJ7gpxg6T' : ( ((qArr[pe]).toString().indexOf('Q2') > 0) ? 'I6unoam3wPR' : ( ((qArr[pe]).toString().indexOf('Q3') > 0) ? 'fOdVzDXDIWl' : ( ((qArr[pe]).toString().indexOf('Q4') > 0) ? 'MlxF1hgrSQZ' : '' ) ) ) ),
                            attributeOptionCombo: 'n2OgrayehoK',
                            source: "",    // LArumsK99c4
                            dhis:   "",    // kHGxIFekzcG
                            diff:   "",
                            value: "",
                            rowid: ouArr[ou] + '.' + dxArr[dx] + '.' + qArr[pe],
                            seqGroup: ouArr[ou] + '.' + dxArr[dx] + '._' + (qArr[pe]).split('Q')[1],
                            storedBy: "Greg_Rowles",
                            created: dtmStamp,
                            lastUpdated: dtmStamp,
                            followUp: false
                        });
                    }
                }
            }
        }

        for(i = 0; i < myData.rows.length; i++) {
            for(r = 0; r < myArr.length; r++) {
                if ( (myData.rows[i][0] == myArr[r].dx) && (myData.rows[i][4] == myArr[r].orgUnit) && (myData.rows[i][2] == myArr[r].pe) ) {
                    if (myData.rows[i][3] == 'LArumsK99c4') {
                        myArr[r]['source'] = parseFloat(myData.rows[i][5]);
                    }
                    if (myData.rows[i][3] == 'kHGxIFekzcG') {
                        myArr[r]['dhis'] = parseFloat(myData.rows[i][5]);
                    }
                    myArr[r]['categoryOptionCombo'] = myData.rows[i][4]; //( ((myArr[r]['pe']).toString().indexOf('Q1') > 0) ? 'I6unoam3wPR' : '' )
                }
            }
        }

        var sort_by = function(field, reverse, primer){
            var key = primer ? function(x) {return primer(x[field])} : function(x) {return x[field]};
            reverse = !reverse ? 1 : -1;
            return function (a, b) { return a = key(a), b = key(b), reverse * ((a > b) - (b > a)); } 
         }

        myArr.sort(sort_by('rowid', false));

        var iAggDhis = 0, iAggSource = 0;
        var lastSeqGroup = '';

        for(r = 0; r < myArr.length; r++) {
            if ( (lastSeqGroup.length > 0) && (lastSeqGroup != myArr[r]['seqGroup']) ) {
                if ( ( (myArr[r]['seqGroup']).indexOf('_') > 0 ) && ( (myArr[r]['seqGroup']).replace('_','') == lastSeqGroup ) ){
                    myArr[r]['dhis'] = iAggDhis;
                    myArr[r]['source'] = iAggSource;
                    iAggDhis = 0, iAggSource = 0;
                    lastSeqGroup = '';
                }
            } else {
                lastSeqGroup = myArr[r]['seqGroup'];
                iAggDhis += ( ( (myArr[r]['dhis']).toString().length > 0 ) ? parseFloat(myArr[r]['dhis']) : 0);
                iAggSource += ( ( (myArr[r]['source']).toString().length > 0 ) ? parseFloat(myArr[r]['source']) : 0);
            }
        }

        var sRem = "";

        for(r = 0; r < myArr.length; r++) {
            if ( (((myArr[r]['source']).toString().length == 0) && ((myArr[r]['dhis']).toString().length == 0)) || (myArr[r].peType == 'mm') ) { //|| (myArr[r].peType == 'mm')
                sRem += (r + ','); // REMOVE ROWS WHERE NO VALUES EXIST
            } else {
                if ((myArr[r]['source']).toString().length == 0){
                    myArr[r]['value'] = 0;
                    myArr[r]['diff'] = 0;
                    sRem += (r + ','); // REMOVE CALCULATIONS WHERE NO AUDIT TOOK PLACE
                } else {
                    if ((myArr[r]['dhis']).toString().length == 0){
                        myArr[r]['value'] = (parseFloat(Math.abs(myArr[r]['source'] - 0) / myArr[r]['source']) * 100).toFixed(0);
                        myArr[r]['diff'] = myArr[r]['source'];
                    } else {
                        if (myArr[r]['source'] == 0){
                            myArr[r]['value'] = 0;
                            sRem += (r + ','); // REMOVE ROWS WHERE NO VALUES EXIST
                        } else {
                            myArr[r]['value'] = (parseFloat(Math.abs(myArr[r]['source'] - myArr[r]['dhis']) / myArr[r]['source']) * 100).toFixed(0);
                        }
                        myArr[r]['diff'] = Math.abs(myArr[r]['source'] - myArr[r]['dhis']);
                    }
                }
            }
        }

        if (sRem.length > 0){
            var arrRem = sRem.substring(sRem,sRem.length-1).split(',');
            console.log("Removing empty rows ("+arrRem.length+")");
            for (var i = arrRem.length -1; i >= 0; i--)
            myArr.splice(arrRem[i],1);
        }

        console.log("Resulting data rows: " + myArr.length);

        myArr.forEach(function(v){ delete v.pe; delete v.dx; delete v.peType; delete v.q; delete v.source; delete v.dhis; delete v.diff; delete v.rowid; delete v.seqGroup; });
        //res.write('{ "dataValues": ' + (JSON.stringify(myArr)) + ' }');
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