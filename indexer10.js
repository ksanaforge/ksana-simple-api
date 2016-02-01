/* legacy code for indexer==10, all segnames are loaded when kdb is opened*/
var kde=require("ksana-database");
var kse=require("ksana-search");
var plist=require("ksana-search/plist");

var txtids2key=function(txtids) {
	if (typeof txtids!=="object") {
		txtids=[txtids];
	}
	var i,fseg,out=[];
	for (i=0;i<txtids.length;i+=1) {
		fseg=this.uti2fileSeg(txtids[i]);
		if (!fseg) {
			out.push(["filecontents",0,0]);
		} else {
			out.push(["filecontents",fseg.file,fseg.seg]);
		}
	}
	return out;
};
var _sibling=function(db,uti,cb){
		var keys=txtids2key.call(db,uti);
		
		if (!keys) {
			cb("invalid uti: "+uti);
			return;
		}
		var segs=db.getFileSegNames(keys[0][1]);
		if (!segs) {
			cb("invalid file id: "+keys[0][1]);
			return;			
		}
		var idx=segs.indexOf(uti);
		cb(0,{sibling:segs,idx:idx});
}
var sibling=function(opts,cb) {
	var uti=opts.uti;

	kde.open(opts.db,function(err,db){
		if (err) {
			cb(err);
			return;
		}
		if ( opts.vpos !==undefined && opts.uti===undefined) {
			db.vpos2uti(opts.vpos,function(uti){
				_sibling(db,uti,cb);
			});
		} else {
			_sibling(db,opts.uti,cb);
		}
	});
};
var nextUti=function(opts,cb){
	kde.open(opts.db,function(err,db){
		if (err) {
			cb(err);
		} else {
			cb(0,db.nextTxtid(opts.uti));
		}
	});
};
//make sure db is opened
var prevUti=function(opts,cb){
	kde.open(opts.db,function(err,db){
		if (err) {
			cb(err);
		} else {
			cb(0,db.prevTxtid(opts.uti));
		}
	});
};

var iterateInner=function(db,funcname,opts,cb,context){
	var out=[], next=opts.uti,  count=opts.count||10,  func=db[funcname+"Txtid"], i;

	for (i=0;i<count;i+=1) {
		next=func.call(db,next);
		if (!next) {
			break;
		}

		if (funcname==="nextTxtid") {
			out.push(next);
		} else {
			out.unshift(next);
		}
	}

	opts.uti=out;
	
};

var fetch_res_prepare_keys=function(db,opts){
	var i,u,keys,vpos,uti=opts.uti;
	if (!uti) {
		uti=[];
		vpos=opts.vpos;
		if (typeof vpos!=="object") {
			vpos=[vpos];
		}
		for (i=0;i<vpos.length;i+=1) {
			u=db.vpos2uti(vpos[i]);
			uti.push(u);
		}
	}
	if (typeof uti!=="object") {
		uti=[uti];
	}
	keys=txtids2key.call(db,uti);
	if (!keys || !keys.length || keys[0][1]===undefined) {
		cb("uti not found: "+uti+" in "+opts.db);
		return;
	}
	opts.uti=uti;
	return keys;
}

var groupByUti=function(db,rawresult,regex,filterfunc,cb) {
	var i,matches,seghits,items=[],out=[],item,vpos,seghit,reg,
	  segnames=db.get("segnames"),segoffsets=db.get("segoffsets");

	if (!rawresult||!rawresult.length) {
		//no q , filter all field
		matches=filterField(segnames,regex,filterfunc);

		items=matches.map(function(item){
			return {text:item.text, uti:item.text, vpos:segoffsets[item.idx]};
		});
		cb(0,items);

	} else {
    seghits= plist.groupbyposting2(rawresult, segoffsets);
    reg=new RegExp(regex);

		filterfunc=filterfunc|| reg.test.bind(reg);
    for (i=0;i<seghits.length;i+=1) {
      seghit=seghits[i];
      if (seghit &&seghit.length) {
	      item=segnames[i-1];
	      vpos=segoffsets[i-1];
			  if (filterfunc(item,regex)) {
			  	out.push({text:item,uti:item,hits:seghit,vpos:vpos});
	      }      	
      }
    }
    cb(0,out,db);
	}
};
module.exports={sibling:sibling,nextUti:nextUti,prevUti:prevUti,iterateInner:iterateInner
,groupByUti:groupByUti,fetch_res_prepare_keys:fetch_res_prepare_keys};