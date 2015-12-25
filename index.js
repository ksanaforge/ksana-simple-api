/*
	TODO : fetch tags
	render tags

*/

var kse=require("ksana-search");
var plist=require("ksana-search/plist");
var kde=require("ksana-database");
var bsearch=kde.bsearch;
var treenodehits=require("./treenodehits");
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
	kse.search(opts.db,opts.q,opts,function(err,res){
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

var _clearHits=function(toc){
	for (var i=0;i<toc.length;i++) {
		//remove the field, make sure treenodehits will calculate new value.
		if (typeof toc[i].hit!=="undefined") delete toc[i].hit;
	}
}

var breadcrumb=function(db,opts,toc,tocname){
	var vpos=opts.vpos;
	if (opts.uti && typeof vpos==="undefined") {
			vpos=db.txtid2vpos(opts.uti);
	}
	var p=bsearchVposInToc(toc,vpos,true);
	if (p==-1) {
		var out={nodeseq:[0],breadcrumb:[toc[0]]};
	}  else {
		var nodeseq=enumAncestors(toc,p);
		nodeseq.push(p);
		var breadcrumb=nodeseq.map(function(n){return toc[n]});
		var out={ nodeseq: nodeseq, breadcrumb:breadcrumb};
	}
	if (tocname) {
		out.name=tocname;
		out.tocname=tocname; //legacy code
		out.toc=toc;
	}

	return out;
}
var toc=function(opts,cb,context) {
	var that=this;
	
	if (!opts.q) {
		kde.open(opts.db,function(err,db){
			var tocname=opts.name||opts.tocname||db.get("meta").toc;
			db.getTOC({tocname:tocname},function(toc){
				if (opts.vpos||opts.uti) {
					cb(0,breadcrumb(db,opts,toc,tocname));
				} else {
					cb(0,{name:tocname,toc:toc,tocname:tocname});	
				}
			});
		});
		return;
	}

	kse.search(opts.db,opts.q,opts,function(err,res){
		if (!res) throw "cannot open database "+opts.db;
		var tocname=opts.name||opts.tocname||res.engine.get("meta").toc;
		var db=res.engine;
		res.engine.getTOC({tocname:tocname},function(toc){
			_clearHits(toc);
			if (opts.vpos||opts.uti) {
					var out=breadcrumb(db,opts,toc,tocname);
					out.nodeseq.map(function(seq){
						//console.log(seq)
						treenodehits(toc,res.rawresult,seq);
					})
					cb(0,out);
			} else {
				cb(0,{name:tocname,toc:toc,hits:res.rawresult,tocname:tocname});	
			}
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
		if (!fseg) out.push(["filecontents",0,0]);
		else out.push(["filecontents",fseg.file,fseg.seg]);
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

var fetch_res=function(db,Q,opts,cb){
	var uti=opts.uti;
		if (!uti) {
			uti=[];
			var vpos=opts.vpos;
			if (typeof vpos!="object") vpos=[vpos];
			for (var i=0;i<vpos.length;i++) {
				var u=db.vpos2txtid(vpos[i]);
				uti.push(u);
			}
		}
		if (typeof uti!=="object") uti=[uti];

		var keys=txtids2key.call(db,uti);
		if (!keys || !keys.length || typeof keys[0][1]=="undefined") {
			cb("uti not found: "+uti+" in "+opts.db);
			return;
		}

		db.get(keys,function(data){
			var out=[],i=0;
			for (i=0;i<keys.length;i++) {
				var hits=null;
				if (Q) hits=hits2markup.call(db,Q,keys[i][1],keys[i][2],data[i]);
				var vpos=db.txtid2vpos(uti[i])||null;
				var item={uti:uti[i],text:data[i],hits:hits,vpos:vpos};
				if (opts.fields) {
					var fields=opts.fields;
					if (typeof fields=="string") fields=[fields];
					item.values=[];
					for (var j=0;j<fields.length;j++) {
						item.values.push(getFieldByVpos(db,fields[j],vpos));
					}
				}
				out.push(item);
			}
			if (typeof opts.breadcrumb!=="undefined") {
				db.getTOC({tocname:opts.breadcrumb||db.get("meta").toc},function(toc){
					for (i=0;i<out.length;i++) {
						var oo=breadcrumb(db,{uti:out[i].uti},toc);
						out[i].breadcrumb=oo.breadcrumb;
						out[i].nodeseq=oo.nodeseq;
					}
					cb(0,out);
				});
			} else {
				cb(0,out);
			}
			
		});
}
var fetch=function(opts,cb,context) {
	var that=this;
	if (!opts.uti && !opts.vpos) {
		cb("missing uti or vpos");
		return;
	}
	var fetchfields=function(db,cb1){
		var fields=opts.fields;
		if (typeof opts.fields=="string") {
			fields=[opts.fields];
		}

		var fieldskey=[];
		for (var i=0;i<fields.length;i++) {
			fieldskey.push(["fields",fields[i]]);
			fieldskey.push(["fields",fields[i]+"_vpos"]);
		}
		//console.log("fetching",fieldskey)
		db.get(fieldskey,cb1);
	}
	if (!opts.q) {
			kde.open(opts.db,function(err,db){
				if (err) {
					cb(err)
				} else {
					if (opts.fields) {
						fetchfields(db,function(){
							fetch_res(db,null,opts,cb);		
						});
					} else {
						fetch_res(db,null,opts,cb);	
					}
				}
			});
	} else {
		kse.search(opts.db,opts.q,opts,function(err,res){
			if (err) {
				cb(err);
			} else {
					if (opts.fields) {
						fetchfields(res.engine,function(){
							fetch_res(res.engine,res,opts,cb)
						});
					} else {
						fetch_res(res.engine,res,opts,cb)		
					}
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
		out.push({uti:txtid,text:ex.text,hits:ex.realHits,vpos:vpos});
	}
	return out;
}
//use startfrom to specifiy starting posting
var excerpt=function(opts,cb,context) {
	var that=this;
	var from=opts.from;
	if (!opts.q) {
		cb("missing q");
		return;
	}

	var range={},newopts={};
	if (opts.from) range.from=opts.from;
	if (opts.count) range.maxseg=opts.count;
	for (var i in opts) {
		newopts[i]=opts[i];
	}
	newopts.nohighlight=true;
	newopts.range=range;

	kse.search(opts.db,opts.q,newopts,
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
	kse.search(opts.db,opts.q,opts,function(err,res){
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
	if (!regex) return [];
	var reg=new RegExp(regex);
	var out=[];
	filterfunc=filterfunc|| reg.test.bind(reg);
	for (var i=0;i<items.length;i++) {
		var item=items[i];
		if (filterfunc(item,regex)) {
			out.push({text:item,idx:i});
		}
	}
	return out;
}

var _groupByField=function(db,rawresult,field,regex,filterfunc,postfunc,cb) {
	db.get(["fields",field],function(fields){
		if (!fields) {
			cb("field "+field+" not found");
			return;
		}
		db.get([["fields",field+"_vpos"],["fields",field+"_depth"]],function(res){
			var items=[], fieldsvpos=res[0],fieldsdepth=res[1];
			if (!rawresult||!rawresult.length) {
				var matches=filterField(fields,regex,filterfunc);

				for (var i=0;i<matches.length;i++) {
					var item=matches[i];
					items.push({text:item.text, uti: db.vpos2txtid(fieldsvpos[item.idx]), vpos:fieldsvpos[item.idx]});
				}
				cb(0,items);
			} else {
				var fieldhits= plist.groupbyposting2(rawresult, fieldsvpos);
				fieldhits.shift();
		    var out=[];
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
		      	var item={text: item, vpos: fieldhit[0], uti:db.vpos2txtid(fieldhit[0]) , hits:fieldhit};
		      	postfunc&&postfunc(item);
		      	items.push(item);
		      }
		    }		
				cb(0,items);
			};
		});
	});
}

var groupByTxtid=function(db,rawresult,regex,filterfunc,cb) {
	if (!rawresult||!rawresult.length) {
		//no q , filter all field
		var values=db.get("segnames");
		var segoffsets=db.get("segoffsets");
		var matches=filterField(values,regex,filterfunc);

		items=matches.map(function(item){
			return {text:item.text, uti:item.text, vpos:segoffsets[item.idx]};
		})
		cb(0,items);

	} else {
		var segoffsets=db.get("segoffsets");
    var seghits= plist.groupbyposting2(rawresult, segoffsets); 
    var txtid=db.get("segnames");
    var out=[];
    var reg=new RegExp(regex);
		 filterfunc=filterfunc|| reg.test.bind(reg);
    for (var i=0;i<seghits.length;i++) {
      var seghit=seghits[i];
      if (!seghit || !seghit.length) continue;
      var item=txtid[i-1];
      var vpos=segoffsets[i-1];
		  if (filterfunc(item,regex)) {
		  	out.push({text:item,uti:item,hits:seghit,vpos:vpos});
      }
    }
    cb(0,out);
	}
}

var _group=function(db,opts,res,cb){
	filterfunc=opts.filterfunc||null;
	var rawresult=res.rawresult;
	if (opts.field) {
		if (opts.ranges) rawresult=trimResult(rawresult,opts.ranges);
		_groupByField(db,rawresult,opts.field,opts.regex,filterfunc,null,cb);
	} else {
		if ((!res.rawresult || !res.rawresult.length)&& res.query) {
			cb(0,[]);//no search result
		} else {
			if (opts.ranges) rawresult=trimResult(rawresult,opts.ranges);
			groupByTxtid(db,rawresult,opts.regex,filterfunc,cb);
		}
	}
}
var trimResult=function(rawresult,_ranges) {
	//assume ranges not overlap
	//TODO, combine continuous range
	var res=[];
	var ranges=JSON.parse(JSON.stringify(_ranges));
	ranges.sort(function(a,b){return a[0]-b[0]});
	for (var i=0;i<ranges.length;i++) {
		var start=ranges[i][0],end=ranges[i][1];
		var s=bsearch(rawresult,start,true);
		var e=bsearch(rawresult,end,true);
		var r=rawresult.slice(s,e);
		res=res.concat(r);
	}
	return res;
}
var filter=function(opts,cb) {
	if (!opts.q){
		if (!opts.regex) {
			cb(0,[]);
			return;
		}
		kde.open(opts.db,function(err,db){
			_group(db,opts,{rawresult:null},cb);
		})
	} else {
		kse.search(opts.db,opts.q,opts,function(err,res){
			_group(res.engine,opts,res,cb);
		});
	}
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

var enumAncestors=function(toc,n) {
	var parents=[0];
	var cur=toc[n];
	var depth=0, now=0;
	while ( toc[now+1]&&toc[now+1].vpos<= cur.vpos ) {
		now++;
		var next=toc[now].n;
		//jump to sibling which has closest vpos
		while ((next && toc[next].vpos<=cur.vpos) || toc[now+1].d==toc[now].d) {
			if (next) {
				now=next;
				next=toc[now].n;				
			} else {e
				next=now+1; //next row is sibling
			}
		}
		if (toc[now].d>depth && toc[now].d<cur.d) {
			parents.push(now);
			depth=toc[now].d;
		} else break;
	}
	return parents;
}

var sibling=function(opts,cb,context) {
	var uti=opts.uti;

	kde.open(opts.db,function(err,db){
		if (typeof opts.vpos !=="undefined" && typeof opts.uti==="undefined") {
			uti=db.vpos2txtid(opts.vpos);
		}

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
	});
}
var findField=function(array,item) {
	for(var i=0;i<array.length;i++){
		if (array[i]===item) {
			return i;
		}
	}
	return -1;
}

var getFieldByVpos=function(db,field,vpos) {
	//make sure field already in cache
	var fieldvpos=db.get(["fields",field+"_vpos"]);
	var fieldvalue=db.get(["fields",field]);
	if (!fieldvpos || !fieldvalue)return null;

	var i=bsearch(fieldvpos,vpos,true);
	if (i>-1) return fieldvalue[i-1];
}
var getFieldRange=function(opts,cb,context){
	var error=null;
	if (!opts.field) error="missing field"
	if (!opts.values && !opts.value) error="missing value"
	if (!opts.db) error="missing db"
	var values=opts.values;

	if (opts.value && typeof opts.value=="string" && !opts.values){
		values=[opts.value];
	}
	if (error) {
		cb(error);
		return;
	}
	get(opts.db,[["meta","vsize"],["fields",opts.field],["fields",opts.field+"_vpos"]],function(data){
		if (!data) {
			cb("error loading field "+opts.field);
			return;
		}
		var vsize=data[0], fieldcaption=data[1], fieldvpos=data[2], out=[];
		for (var i=0;i<values.length;i++) {
			var v=values[i];
			var p=findField(fieldcaption,v);
			var start=vsize,end=vsize;
			if (p>-1) start=fieldvpos[p];
			if (p<fieldvpos.length-1) {
				end=fieldvpos[p+1];
			}
			out.push([start,end]);
		}
		cb(0,out);
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
	sibling:sibling,
	excerpt:excerpt,
	scan:scan,
	filter:filter,
	listkdb:listkdb,
	fillHits:fillHits,
	renderHits:renderHits,
	tryOpen:tryOpen,
	get:get,
	treenodehits:treenodehits,
	getFieldRange:getFieldRange
}
module.exports=API;