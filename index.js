/*
	TODO : fetch tags
	render tags

*/

var kse=require("ksana-search");
var plist=require("ksana-search/plist");
var kde=require("ksana-database");
var bsearch=kde.bsearch;

//make sure db is opened
var nextUti=function(opts,cb){
	kde.open(opts.db,function(err,db){
		if (err) cb(err);
		else cb(0,db.nextTxtid(opts.uti));
	});		
}
//make sure db is opened
var prevUti=function(opts,cb){
	kde.open(opts.db,function(err,db){
		if (err) cb(err);
		else cb(0,db.prevTxtid(opts.uti));
	});		
}

var __iterate=function(db,funcname,opts,cb,context){
	var out=[];
	var next=opts.uti;
	var count=opts.count||10;
	var func=db[funcname];
	for (var i=0;i<count;i++) {
		next=func.call(db,next);
		if (!next) break;

		if (funcname==="nextTxtid") {
			out.push(next);
		} else {
			out.unshift(next);
		}
	}
	opts.uti=out;
	fetch(opts,cb,context);
}
var _iterate=function(funcname,opts,cb,context) {
	if (!opts.q) {
		kde.open(opts.db,function(err,db){
			if (!err) __iterate(db,funcname,opts,cb,context);
			else console.error(err);
		});
		return;
	}
	kse.search(opts.db,opts.q,function(err,res){
		if (!err) __iterate(res.engine,funcname,opts,cb,context);
		else console.error(err);
	});
}

var next=function(opts,cb,context) {
	_iterate("nextTxtid",opts,cb,context);
}
var prev=function(opts,cb,context) {
	_iterate("prevTxtid",opts,cb,context);
}

var toc=function(opts,cb,context) {
	var that=this;
	
	if (!opts.q) {
		kde.open(opts.db,function(err,db){
			var tocname=opts.tocname||db.get("meta").toc;
			db.getTOC({tocname:tocname},function(data){
				cb(0,{name:tocname,toc:data,tocname:tocname});
			});
		});
		return;
	}

	kse.search(opts.db,opts.q,{},function(err,res){
		if (!res) throw "cannot open database "+opts.db;
		var tocname=opts.tocname||res.engine.get("meta").toc;
		res.engine.getTOC({tocname:tocname},function(data){
			cb(0,{name:tocname,toc:data,hits:res.rawresult,tocname:tocname});
		});
	});
}

var txtids2key=function(txtids) {
	if (typeof txtids!="object") {
		txtids=[txtids];
	}
	var out=[];
	for (var i=0;i<txtids.length;i++) {
		var fseg=this.txtid2fileSeg(txtids[i]);
		out.push(["filecontents",fseg.file,fseg.seg]);
	}
	return out;
}

var hits2markup=function( Q,file,seg, text){
	var seg1=this.fileSegToAbsSeg(file,seg);
	var vpos=this.absSegToVpos(seg1);
	var vpos2=this.absSegToVpos(seg1+1);
	var hits=kse.excerpt.realHitInRange(Q,vpos,vpos2,text);
	return hits;
}

var fetch_res=function(engine,Q,opts,cb){
	var uti=opts.uti;
		if (!uti) {
			uti=[];
			var vpos=opts.vpos;
			if (typeof vpos!="object") vpos=[vpos];
			for (var i=0;i<vpos.length;i++) {
				var u=engine.vpos2txtid(vpos[i]);
				uti.push(u);
			}
		}
		if (typeof uti!=="object") uti=[uti];
		var keys=txtids2key.call(engine,uti);
		if (typeof keys[0][1]=="undefined") {
			cb("uti not found: "+uti+" in "+opts.db);
			return;
		}

		engine.get(keys,function(data){
			var out=[];
			for (var i=0;i<keys.length;i++) {
				var hits=null;
				if (Q) hits=hits2markup.call(engine,Q,keys[i][1],keys[i][2],data[i]);
				var vpos=engine.txtid2vpos(uti[i]);
				out.push({uti:uti[i],text:data[i],hits:hits,vpos:vpos});
			}
			cb(0,out);
		});
}
var fetch=function(opts,cb,context) {
	var that=this;
	if (!opts.uti && !opts.vpos) {
		cb("missing uti or vpos");
		return;
	}
	if (!opts.q) {
			kde.open(opts.db,function(err,db){
				if (err) {
					cb(err)
				} else {
					fetch_res(db,null,opts,cb);
				}
			});
	} else {
		kse.search(opts.db,opts.q,{},function(err,res){
			if (err) {
				cb(err);
			} else {
				fetch_res(res.engine,res,opts,cb)
			}
		});		
	}
}

var excerpt2defaultoutput=function(excerpt) {
	var out=[];
	for (var i=0;i<excerpt.length;i++) {
		var ex=excerpt[i];
		var vpos=this.fileSegToVpos(ex.file,ex.seg);
		var txtid=this.vpos2txtid(vpos);
		out.push({uti:txtid,text:ex.text,hits:ex.realHits});
	}
	return out;
}
//use startfrom to specifiy starting posting
var excerpt=function(opts,cb,context) {
	var that=this;
	if (!opts.q) {
		cb("missing q");
		return;
	}
	kse.search(opts.db,opts.q,{nohighlight:true,range:{from:opts.from}},
		function(err,res){
		if (err) {
			cb(err);
		} else {
			//console.log(res.rawresult)
			cb(0,excerpt2defaultoutput.call(res.engine,res.excerpt));
		}
	});	
}

var beginWith=function(s,txtids) {
	var out=[];
	for (var i=1;i<s.length;i++) {
		tofind=s.substr(0,i);
		var idx=bsearch(txtids,tofind);
		if (idx>-1) out.push(txtids[idx]);
	}
	return out;
}
var scan=function(opts,cb,context) {
	kse.search(opts.db,opts.q,{},function(err,res){
		if (err) {
			cb(err);
			return;
		}
		var db=res.engine;
		var out=[];
		var segnames=db.get("segnames");
		for (var i=0;i<opts.sentence.length;i++) {
			var q=opts.sentence.substr(i);
			out=out.concat(beginWith(q,segnames));
		}
		cb(0,out);
	});
}

var filterField=function(items,regex,filterfunc) {
	if (!regex) return items;
	var reg=new RegExp(regex);
	var out=[];
	filterfunc=filterfunc|| reg.test.bind(reg);
	for (var i=0;i<items.length;i++) {
		var item=items[i];
		if (filterfunc(item,regex)) {
			out.push(item);
		}
	}
	return out;
}

var groupByField=function(db,searchres,field,regex,filterfunc,cb) {
	db.get(["fields",field],function(fields){
		var rawresult=searchres.rawresult;
		db.get([["fields",field+"_vpos"],["fields",field+"_depth"]],function(res){
			var fieldsvpos=res[0],fieldsdepth=res[1];
			if (!rawresult||!rawresult.length) {
				var matches=filterField(fields,regex,filterfunc);
				cb(0,matches,null,fieldsvpos);
			} else {
				var fieldhits= plist.groupbyposting2(rawresult, fieldsvpos);
				fieldhits.shift();
		    var matches=[],hits=[],vpos=[];
		    var reg=new RegExp(regex);
		    filterfunc=filterfunc|| reg.test.bind(reg);
		    var prevdepth=65535,inrange=false;
		    for (var i=0;i<fieldhits.length;i++) {
		      var fieldhit=fieldhits[i];
		      var item=fields[i];

		      //all depth less than prevdepth will considered in range.
		      if (filterfunc(item,regex)) {
		      	inrange=true;
		      	if (prevdepth>fieldsdepth[i]) {
		      		prevdepth=fieldsdepth[i];
		      	}
		      } else if (inrange) {
		      	if (fieldsdepth[i]==prevdepth) {//turn off inrange
		      		inrange=false;
		      		prevdepth=65535;
		      	}
		      }

		      if (!fieldhit || !fieldhit.length) continue;
		      if (inrange) {
		      	matches.push(item);
		      	hits.push(fieldhit);
		      	vpos.push(fieldhit[0]);
		      }
		    }		
				cb(0,matches,hits,vpos);
			};
		});
	});
}

var groupByTxtid=function(db,searchresult,regex,filterfunc,cb) {
	var rawresult=searchresult.rawresult;
	if (!rawresult||!rawresult.length) {
		//no q , filter all field
		if (searchresult.query) {
			cb(0,[]);
		} else {
			var values=db.get("segnames");
			var matches=filterField(values,regex,filterfunc);
			cb(0,matches);
		}
	} else {
		var segoffsets=db.get("segoffsets");
    var seghits= plist.groupbyposting2(rawresult, segoffsets); 
    var txtid=db.get("segnames");
    var matches=[],hits=[];
    var reg=new RegExp(regex);
		 filterfunc=filterfunc|| reg.test.bind(reg);
    for (var i=0;i<seghits.length;i++) {
      var seghit=seghits[i];
      if (!seghit || !seghit.length) continue;
      var item=txtid[i-1];
		  if (filterfunc(item,regex)) {
      	matches.push(item);
      	hits.push(seghit);
      }
    }
    cb(0,matches,hits);
	}
}

var filter=function(opts,cb) {
	kse.search(opts.db,opts.q,{},function(err,res){
		if (err) {
			cb(err);
			return;
		}
		var db=res.engine;
		filterfunc=opts.filterfunc||null;
		if (opts.field) {
			groupByField(db,res,opts.field,opts.regex,filterfunc,cb);
		} else {
			groupByTxtid(db,res,opts.regex,filterfunc,cb);
		}
	});
}
var listkdb=kde.listkdb;


var fillHits=function(searchable,tofind,cb) {
	var taskqueue=[],out=[];
	for (var i=0;i<searchable.length;i++) {
		(function(dbname){
			taskqueue.push(function(err,data){
				if (typeof data=='object' && data.__empty) {
					//not pushing the first call
				} else {
					searchable.map(function(db){
						if (db.shortname===data.dbname) {
							db.hits=data.rawresult.length;
						}
					})
				}
				kse.search(dbname,tofind,taskqueue.shift(0,data));
			});;
		})(searchable[i].fullname)
	};

	taskqueue.push(function(err,data){

		searchable.sort(function(a,b){
			return b.hits-a.hits;
		});

		cb(searchable);
	});
	taskqueue.shift()(0,{__empty:true});
}
var tryOpen=function(kdbid,cb){
	if ((window.location.protocol==="file:" && typeof process==="undefined") 
	|| typeof io==="undefined" ) {
		cb("local file mode");
		return;
	}
	kde.open(kdbid,function(err){
		cb(err);
	});
}
var renderHits=function(text,hits,func){
  var ex=0,out=[];
  hits=hits||[];
  for (var i=0;i<hits.length;i++) {
    var now=hits[i][0];
    if (now>ex) {
      out.push(func({key:i},text.substring(ex,now)));
    }
    out.push(func({key:"h"+i, className:"hl"+hits[i][2]},text.substr(now,hits[i][1])));
    ex=now+=hits[i][1];
  }
  out.push(func({key:i+1},text.substr(ex)));
  return out;
}  

var get=function(dbname,key,cb) { //low level get
	var db=kde.open(dbname,function(err,db){
		if (err) {
			cb(err);
		} else {
			db.get(key,cb);
		}
	});
}
var vpos2txtid=function(dbname,vpos,cb){
	var db=kde.open(dbname,function(err,db){
		if (err) cb(err);
		else cb(0,db.vpos2txtid(vpos));
	});
}
var txtid2vpos=function(dbname,txtid,cb){
	var db=kde.open(dbname,function(err,db){
		if (err) cb(err);
		else cb(0,db.txtid2vpos(txtid));
	});
}

var bsearchVposInToc = function (toc, obj, near) { 
  var low = 0,
  high = toc.length;
  while (low < high) {
    var mid = (low + high) >> 1;
    if (toc[mid].vpos==obj) return mid-1;
    toc[mid].vpos < obj ? low = mid + 1 : high = mid;
  }
  if (near) return low-1;
  else if (toc[low].vpos==obj) return low;else return -1;
};
//from ksana2015-stackto
var enumAncestors=function(toc,cur) {
    if (!toc || !toc.length) return;
    if (cur==0) return [];
    var n=cur-1;
    var depth=toc[cur].d - 1;
    var parents=[];
    while (n>=0 && depth>0) {
      if (toc[n].d==depth) {
        parents.unshift(n);
        depth--;
      }
      n--;
    }
    parents.unshift(0); //first ancestor is root node
    return parents;
}

var breadcrumb=function(opts,cb,context) {
	kde.open(opts.db,function(err,db){

		var vpos=opts.vpos;
		if (opts.uti && typeof vpos==="undefined") {
			vpos=db.txtid2vpos(opts.uti);
		}

		if (!vpos) {
			cb("must have uti or vpos");
			return;
		}

		var tocname=opts.tocname||db.get("meta").toc;
		db.getTOC({tocname:tocname},function(toc){
			var p=bsearchVposInToc(toc,vpos,true);
			var nodes=enumAncestors(toc,p);
			nodes.push(p);
			var breadcrumb=nodes.map(function(n){return toc[n]});
			cb(0,{ nodes: nodes, breadcrumb:breadcrumb, toc:toc});
		});
	});
}

var API={
	next:next,
	prev:prev,
	nextUti:nextUti,
	prevUti:prevUti,
	vpos2txtid:vpos2txtid,
	txtid2vpos:txtid2vpos,	
	toc:toc,
	fetch:fetch,
	excerpt:excerpt,
	scan:scan,
	filter:filter,
	listkdb:listkdb,
	fillHits:fillHits,
	renderHits:renderHits,
	tryOpen:tryOpen,
	get:get,
	breadcrumb:breadcrumb
}
module.exports=API;