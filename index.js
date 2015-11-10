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

var _iterate=function(funcname,opts,cb,context) {
	kse.search(opts.db,opts.q,function(err,res){
		var db=res.engine;
		if (err) {
			cb(err);
			return;
		}
		var out=[];
		var next=opts.uti;
		var count=opts.count||10;
		var func=db[funcname];
		for (var i=0;i<count;i++) {
			var next=func(next);
			if (!next) break;
			out.push(next);
		}
		opts.uti=out;
		fetch(opts,cb,context);
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
var fetch=function(opts,cb,context) {
	var that=this;
	if (!opts.uti && !opts.vpos) {
		cb("missing uti or vpos");
		return;
	}
	kse.search(opts.db,opts.q,{},function(err,res){
		if (err) {
			cb(err);
		} else {
			var uti=opts.uti;
			if (!uti) {
				uti=[];
				var vpos=opts.vpos;
				if (typeof vpos!="object") vpos=[vpos];
				for (var i=0;i<vpos.length;i++) {
					var u=res.engine.vpos2txtid(vpos[i]);
					uti.push(u);
				}
			}
			if (typeof uti!=="object") uti=[uti];
			var keys=txtids2key.call(res.engine,uti);
			if (typeof keys[0][1]=="undefined") {
				cb("uti not found: "+uti+" in "+opts.db);
				return;
			}

			res.engine.get(keys,function(data){
				var out=[];
				for (var i=0;i<keys.length;i++) {
					var hits=hits2markup.call(res.engine,res,keys[i][1],keys[i][2],data[i]);
					var vpos=res.engine.txtid2vpos(uti[i]);
					out.push({uti:uti[i],text:data[i],hits:hits,vpos:vpos});
				}
				cb(0,out);
			});
		}
	});	
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
	get:get
}
module.exports=API;