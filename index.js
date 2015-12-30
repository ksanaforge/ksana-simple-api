"use strict";
var kse=require("ksana-search");
var plist=require("ksana-search/plist");
var kde=require("ksana-database");
var bsearch=kde.bsearch;
var treenodehits=require("./treenodehits");
//make sure db is opened
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


var clearHits=function(toc){
	var i;
	for (i=0;i<toc.length;i+=1) {
		//remove the field, make sure treenodehits will calculate new value.
		if (toc[i].hit!==undefined) {
			delete toc[i].hit;
		}
	}
};

var bsearchVposInToc = function (toc, obj, near) { 
  var mid,low = 0,
  high = toc.length;
  while (low < high) {
    mid = (low + high) >> 1;
    if (toc[mid].vpos===obj) {
    	return mid-1;
    }
    if (toc[mid].vpos < obj) {
    	low = mid + 1;
    } else {
    	high = mid;
    } 
  }
  if (near) {
  	return low-1;
  } 
  if (toc[low].vpos===obj) {
  	return low;
  }
  return -1;
};
var enumAncestors=function(toc,n) {
	var parents=[0], cur=toc[n],depth=0, now=0,next;
	while ( toc[now+1]&&toc[now+1].vpos<= cur.vpos ) {
		now+=1;
		next=toc[now].n;
		//jump to sibling which has closest vpos
		while ((next && toc[next].vpos<=cur.vpos) || (toc[now+1] && toc[now+1].d===toc[now].d)) {
			if (next) {
				now=next;
				next=toc[now].n;				
			} else {
				next=now+1; //next row is sibling
			}
		}
		if (toc[now].d>depth && toc[now].d<cur.d) {
			parents.push(now);
			depth=toc[now].d;
		} else {
			break;
		}
	}
	return parents;
};

var breadcrumb=function(db,opts,toc,tocname){
	var nodeseq,breadcrumb,out,p,vpos=opts.vpos;
	if (opts.uti && opts.vpos===undefined) {
			vpos=db.txtid2vpos(opts.uti);
	}
	p=bsearchVposInToc(toc,vpos,true);
	if (p===-1) {
		out={nodeseq:[0],breadcrumb:[toc[0]]};
	}  else {
		nodeseq=enumAncestors(toc,p);
		nodeseq.push(p);
		breadcrumb=nodeseq.map(function(n){return toc[n];});
		out={ nodeseq: nodeseq, breadcrumb:breadcrumb};
	}
	if (tocname) {
		out.name=tocname;
		out.tocname=tocname; //legacy code
		out.toc=toc;
	}

	return out;
};
var toc=function(opts,cb,context) {
	if (!opts.q) {
		kde.open(opts.db, opts, function(err,db){
			if (err) {
				cb(err);
			}
			var tocname=opts.name||opts.tocname||db.get("meta").toc;
			db.getTOC({tocname:tocname},function(toc){
				if (opts.vpos||opts.uti) {
					cb(0,breadcrumb(db,opts,toc,tocname));
				} else {
					cb(0,{name:tocname,toc:toc,tocname:tocname});	
				}
			});
		},context);
		return;
	}

	kse.search(opts.db,opts.q,opts,function(err,res){
		if (err || !res) {
			throw "cannot open database "+opts.db;
		}
		var tocname=opts.name||opts.tocname||res.engine.get("meta").toc,db=res.engine;
		res.engine.getTOC({tocname:tocname},function(toc){
			clearHits(toc);
			if (opts.vpos||opts.uti) {
					var out=breadcrumb(db,opts,toc,tocname);
					out.nodeseq.map(function(seq){
						//console.log(seq)
						treenodehits(toc,res.rawresult,seq);
					});
					cb(0,out);
			} else {
				cb(0,{name:tocname,toc:toc,hits:res.rawresult,tocname:tocname});	
			}
		});
	});
};

var getFieldByVpos=function(db,field,vpos) {
	//make sure field already in cache
	var i,fieldvpos=db.get(["fields",field+"_vpos"]),
	fieldvalue=db.get(["fields",field]);
	if (!fieldvpos || !fieldvalue) {
		return null;
	}

	i=bsearch(fieldvpos,vpos,true);
	if (i>-1) {
		return fieldvalue[i-1];
	}
};

var txtids2key=function(txtids) {
	if (typeof txtids!=="object") {
		txtids=[txtids];
	}
	var i,fseg,out=[];
	for (i=0;i<txtids.length;i+=1) {
		fseg=this.txtid2fileSeg(txtids[i]);
		if (!fseg) {
			out.push(["filecontents",0,0]);
		} else {
			out.push(["filecontents",fseg.file,fseg.seg]);
		}
	}
	return out;
};


var fetch_res=function(db,Q,opts,cb){
	var i,u,keys,vpos,uti=opts.uti;
	if (!uti) {
		uti=[];
		vpos=opts.vpos;
		if (typeof vpos!=="object") {
			vpos=[vpos];
		}
		for (i=0;i<vpos.length;i+=1) {
			u=db.vpos2txtid(vpos[i]);
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

	db.get(keys,function(data){
		var fields,item,out=[],j,k,hits=null,seg1,vp,vp_end;
		for (j=0;j<keys.length;j+=1) {
			hits=null;
			seg1=this.fileSegToAbsSeg(keys[j][1],keys[j][2]);
			vp=this.absSegToVpos(seg1);
			vp_end=this.absSegToVpos(seg1+1);
			if (Q) {
				hits=kse.excerpt.realHitInRange(Q,vp,vp_end,data[j]);
			}

			item={uti:uti[j],text:data[j],hits:hits,vpos:vp,vpos_end:vp_end};
			if (opts.fields) {
				fields=opts.fields;
				if (typeof fields==="string") {
					fields=[fields];
				}
				item.values=[];
				for (k=0;k<fields.length;k+=1) {
					item.values.push(getFieldByVpos(db,fields[k],vpos));
				}
			}
			out.push(item);
		}
		if (opts.breadcrumb!==undefined) {
			db.getTOC({tocname:opts.breadcrumb||db.get("meta").toc},function(toc){
				var oo;
				for (i=0;i<out.length;i+=1) {
					oo=breadcrumb(db,{uti:out[i].uti},toc);
					out[i].breadcrumb=oo.breadcrumb;
					out[i].nodeseq=oo.nodeseq;
				}
				cb(0,out);
			});
		} else {
			cb(0,out);
		}
		
	});
};
var fetchfields=function(opts,db,cb1){
	var i,fields=opts.fields,fieldskey=[];
	if (typeof opts.fields==="string") {
		fields=[opts.fields];
	}


	for (i=0;i<fields.length;i+=1) {
		fieldskey.push(["fields",fields[i]]);
		fieldskey.push(["fields",fields[i]+"_vpos"]);
	}
	//console.log("fetching",fieldskey)
	db.get(fieldskey,cb1);
};

var fetch=function(opts,cb) {
	if (!opts.uti && !opts.vpos) {
		cb("missing uti or vpos");
		return;
	}

	if (!opts.q) {
			kde.open(opts.db,function(err,db){
				if (err) {
					cb(err);
				} else {
					if (opts.fields) {
						fetchfields(opts,db,function(){
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
						fetchfields(opts,res.engine,function(){
							fetch_res(res.engine,res,opts,cb);
						});
					} else {
						fetch_res(res.engine,res,opts,cb);
					}
			}
		});		
	}
};

var excerpt2defaultoutput=function(excerpt) {
	var out=[],i,ex,vpos,txtid;
	for (i=0;i<excerpt.length;i+=1) {
		ex=excerpt[i];
		vpos=this.fileSegToVpos(ex.file,ex.seg);
		txtid=this.vpos2txtid(vpos);
		out.push({uti:txtid,text:ex.text,hits:ex.realHits,vpos:vpos});
	}
	return out;
};
//use startfrom to specifiy starting posting
var excerpt=function(opts,cb) {
	var i,range={},newopts={};
	if (!opts.q) {
		cb("missing q");
		return;
	}

	if (opts.from) {
		range.from=opts.from;
	}
	if (opts.count) {
		range.maxseg=opts.count;
	}
	for (i in opts) {
		if (opts.hasOwnProperty(i)){
			newopts[i]=opts[i];	
		}
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
};

var beginWith=function(s,txtids) {
	var out=[],i,tofind,idx;
	for (i=1;i<s.length;i+=1) {
		tofind=s.substr(0,i);
		idx=bsearch(txtids,tofind);
		if (idx>-1) {
			out.push(txtids[idx]);
		}
	}
	return out;
};

var scan=function(opts,cb,context) {
	kse.search(opts.db,opts.q,opts,function(err,res){
		if (err) {
			cb(err);
			return;
		}
		var q,db=res.engine,out=[],i,segnames=db.get("segnames");
		for (i=0;i<opts.sentence.length;i+=1) {
			q=opts.sentence.substr(i);
			out=out.concat(beginWith(q,segnames));
		}
		cb(0,out);
	},context);
};

var filterField=function(items,regex,filterfunc) {
	if (!regex) {
		return [];
	}
	var i, item,reg=new RegExp(regex), out=[];
	filterfunc=filterfunc|| reg.test.bind(reg);
	for (i=0;i<items.length;i+=1) {
		item=items[i];
		if (filterfunc(item,regex)) {
			out.push({text:item,idx:i});
		}
	}
	return out;
};

var groupByField=function(db,rawresult,field,regex,filterfunc,postfunc,cb) {
	db.get(["fields",field],function(fields){
		if (!fields) {
			cb("field "+field+" not found");
			return;
		}
		db.get([["fields",field+"_vpos"],["fields",field+"_depth"]],function(res){
			var fieldhit,reg,i,item,matches,items=[], fieldsvpos=res[0],fieldsdepth=res[1],
			prevdepth=65535,inrange=false, fieldhits;
			if (!rawresult||!rawresult.length) {
				matches=filterField(fields,regex,filterfunc);

				for (i=0;i<matches.length;i+=1) {
					item=matches[i];
					items.push({text:item.text, uti: db.vpos2txtid(fieldsvpos[item.idx]), vpos:fieldsvpos[item.idx]});
				}
				cb(0,items);
			} else {
				fieldhits = plist.groupbyposting2(rawresult, fieldsvpos);
				reg =new RegExp(regex);

				fieldhits.shift();
		    filterfunc=filterfunc|| reg.test.bind(reg);
		    for (i=0;i<fieldhits.length;i+=1) {
		      fieldhit=fieldhits[i];
		      item=fields[i];

		      //all depth less than prevdepth will considered in range.
		      if (filterfunc(item,regex)) {
		      	inrange=true;
		      	if (prevdepth>fieldsdepth[i]) {
		      		prevdepth=fieldsdepth[i];
		      	}
		      } else if (inrange) {
		      	if (fieldsdepth[i]===prevdepth) {//turn off inrange
		      		inrange=false;
		      		prevdepth=65535;
		      	}
		      }

		      if (inrange && fieldhits && fieldhit.length) {
		      	item={text: item, vpos: fieldhit[0], uti:db.vpos2txtid(fieldhit[0]) , hits:fieldhit};
		      	if (postfunc) {
		      		postfunc(item);
		      	}
		      	items.push(item);
		      }
		    }		
				cb(0,items);
			}
		});
	});
};

var groupByTxtid=function(db,rawresult,regex,filterfunc,cb) {
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
    cb(0,out);
	}
};


var trimResult=function(rawresult,rs) {
	//assume ranges not overlap, todo :combine continuous range
	var start,end,s,e,r,i,res=[],
	  ranges=JSON.parse(JSON.stringify(rs));
	ranges.sort(function(a,b){return a[0]-b[0];});
	for (i=0;i<ranges.length;i+=1) {
		start=ranges[i][0];
		end=ranges[i][1];
		s=bsearch(rawresult,start,true);
		e=bsearch(rawresult,end,true);
		r=rawresult.slice(s,e);
		res=res.concat(r);
	}
	return res;
};
var groupInner=function(db,opts,res,cb){
	var filterfunc=opts.filterfunc||null,
	  rawresult=res.rawresult;
	if (opts.field) {
		if (opts.ranges) {
			rawresult=trimResult(rawresult,opts.ranges);
		}
		groupByField(db,rawresult,opts.field,opts.regex,filterfunc,null,cb);
	} else {
		if ((!res.rawresult || !res.rawresult.length)&& res.query) {
			cb(0,[]);//no search result
		} else {
			if (opts.ranges) {
				rawresult=trimResult(rawresult,opts.ranges);
			}
			groupByTxtid(db,rawresult,opts.regex,filterfunc,cb);
		}
	}
};
var filter=function(opts,cb) {
	if (!opts.q){
		if (!opts.regex) {
			cb(0,[]);
			return;
		}
		kde.open(opts.db,function(err,db){
			if (err) {
				cb(err);
				return;
			}
			groupInner(db,opts,{rawresult:null},cb);
		});
	} else {
		kse.search(opts.db,opts.q,opts,function(err,res){
			if (err) {
				cb(err);
				return;
			}
			groupInner(res.engine,opts,res,cb);
		});
	}
};
var listkdb=kde.listkdb;

var task=function(dbname,searchable,taskqueue,tofind){
		taskqueue.push(function(err,data){
			if (!err && !(typeof data==='object' && data.empty)) {
				searchable.map(function(db){
					if (db.shortname===data.dbname) {
						db.hits=data.rawresult.length;
					}
				});
			}
			kse.search(dbname,tofind,taskqueue.shift(0,data));
		});
};

var fillHits=function(searchable,tofind,cb) {
	var i,taskqueue=[];
	for (i=0;i<searchable.length;i+=1) {
		task(searchable[i].fullname,searchable,taskqueue,tofind);
	}

	taskqueue.push(function(){
		searchable.sort(function(a,b){
			return b.hits-a.hits;
		});
		cb(searchable);			
	});
	taskqueue.shift()(0,{empty:true});
};

var tryOpen=function(kdbid,cb){
	if ((window.location.protocol==="file:" && window.process===undefined) 
	|| window.io===undefined ) {
		cb("local file mode");
		return;
	}
	kde.open(kdbid,function(err){
		cb(err);
	});
};
var renderHits=function(text,hits,func){
  var i, ex=0,out=[],now;
  hits=hits||[];
  for (i=0;i<hits.length;i+=1) {
    now=hits[i][0];
    if (now>ex) {
      out.push(func({key:i},text.substring(ex,now)));
    }
    out.push(func({key:"h"+i, className:"hl"+hits[i][2]},text.substr(now,hits[i][1])));
    ex = now+hits[i][1];
  }
  out.push(func({key:i+1},text.substr(ex)));
  return out;
};

var get=function(dbname,key,cb) { //low level get
	kde.open(dbname,function(err,db){
		if (err) {
			cb(err);
		} else {
			db.get(key,cb);
		}
	});
};

var vpos2uti=function(opts,cb){
	kde.open(opts.db,function(err,db){
		if (err) {
			cb(err);
		} else {
			cb(0,db.vpos2txtid(opts.vpos));
		}
	});
};

var uti2vpos=function(opts,cb){
	kde.open(opts.db,function(err,db){
		if (err) {
			cb(err);
		} else {
			cb(0,db.txtid2vpos(opts.uti));
		}
	});
};



var sibling=function(opts,cb) {
	var uti=opts.uti,keys,segs,idx;

	kde.open(opts.db,function(err,db){
		if (err) {
			cb(err);
			return;
		}
		if ( opts.vpos !==undefined && opts.uti===undefined) {
			uti=db.vpos2txtid(opts.vpos);
		}

		keys=txtids2key.call(db,uti);
		
		if (!keys) {
			cb("invalid uti: "+uti);
			return;
		}
		segs=db.getFileSegNames(keys[0][1]);
		if (!segs) {
			cb("invalid file id: "+keys[0][1]);
			return;			
		}
		idx=segs.indexOf(uti);
		cb(0,{sibling:segs,idx:idx});
	});
};

var findField=function(array,item) {
	var i;
	for(i=0;i<array.length;i+=1){
		if (array[i]===item) {
			return i;
		}
	}
	return -1;
};

var getFieldRange=function(opts,cb){
	var i,v,p,start,end,vsize,fieldcaption,fieldvpos,out,error=null,values=opts.values;
	if (!opts.field) {
		error="missing field";
	}
	if (!opts.values) {
		error="missing value";
	}
	if (!opts.db) {
		error="missing db";
	}

	if (typeof opts.values==="string"){
		values=[opts.values];
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
		vsize=data[0];
		fieldcaption=data[1];
		fieldvpos=data[2];
		out=[];
		for (i=0;i<values.length;i+=1) {
			v=values[i];
			p=findField(fieldcaption,v);
			start=vsize;
			end=vsize;
			if (p>-1) {
				start=fieldvpos[p];
			}
			if (p<fieldvpos.length-1) {
				end=fieldvpos[p+1];
			}
			out.push([start,end]);
		}
		cb(0,out);
	});
};

var iterateInner=function(db,funcname,opts,cb,context){
	var out=[], next=opts.uti,  count=opts.count||10,  func=db[funcname], i;
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
	fetch(opts,cb,context);
};

var iterate=function(funcname,opts,cb,context) {
	if (!opts.q) {
		kde.open(opts.db,function(err,db){
			if (!err) {
				iterateInner(db,funcname,opts,cb,context);
			}
			else {
				cb(err);
			}
		},context);
		return;
	}
	kse.search(opts.db,opts.q,opts,function(err,res){
		if (!err) {
			iterateInner(res.engine,funcname,opts,cb,context);
		} else {
			cb(err);
		}
	},context);
};

var search=function(opts,cb,context){
	kse.search(opts.db,opts.q,opts,function(err,res){
		cb(err,res);
	},context);
};

var next=function(opts,cb,context) {
	iterate("nextTxtid",opts,cb,context);
};

var prev=function(opts,cb,context) {
	iterate("prevTxtid",opts,cb,context);
};

var API={
	next:next,
	prev:prev,
	nextUti:nextUti,
	prevUti:prevUti,
	vpos2uti:vpos2uti,
	uti2vpos:uti2vpos,	
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
	getFieldRange:getFieldRange,
	search:search

};
module.exports=API;
