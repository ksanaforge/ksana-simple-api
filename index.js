"use strict";
var kse=require("ksana-search");
var plist=require("ksana-search/plist");
var kde=require("ksana-database");
var bsearch=kde.bsearch;
var treenodehits=require("./treenodehits");
var createTask=require("./taskqueue").createTask;
var indexer10=require("./indexer10"); //old format

var nextUti=function(opts,cb){
	kde.open(opts.db,function(err,db){
		if (err) {
			cb(err);
		} else {
			if (db.get("meta").indexer===10) {
				indexer10.nextUti(opts,cb);
				return;
			}
			db.uti2fileSeg(opts.uti,function(fseg){
				var segments=db.get(["segments",fseg.file]);
				if (fseg.seg-1<segments.length) {
					fseg.seg+=1;
					var uti=db.fileSeg2uti(fseg);
					cb(0, uti );
				} else {
					cb("last uti "+opts.uti+" cannot next");
				}
			});
		}
	});
};
//make sure db is opened
var prevUti=function(opts,cb){
	kde.open(opts.db,function(err,db){
		if (err) {
			cb(err);
		} else {
			if (db.get("meta").indexer===10) {
				indexer10.prevUti(opts,cb);
				return;
			}
			db.uti2fileSeg(opts.uti,function(fseg){
				var segments=db.get(["segments",fseg.file]);
				if (fseg.seg>0) {
					fseg.seg-=1;
					var uti=db.fileSeg2uti(fseg);
					cb(0,uti);	
				} else {
					cb("first uti "+opts.uti+" cannot prev");
				}
			});
		}
	});
};
//return next hit vpos and uti
var nextHit=function(opts,cb){
	kse.search(opts.db,opts.q,opts,function(err,res){
		if (err) {
			cb(err);
			return;
		}
		var i=bsearch(res.rawresult,opts.vpos,true);
		if (i===-1 || i===res.rawresult.length-1) {
			cb("cannot find next hit");
			return;
		}
		var nextvpos=res.rawresult[i+1];

		res.engine.vpos2uti(nextvpos+1,function(uti){
			cb(0,{uti:uti,vpos:nextvpos});	
		}); //nextvpos is the first character
		
	});
}
//return prev hit vpos and uti
var prevHit=function(opts,cb){
	kse.search(opts.db,opts.q,opts,function(err,res){
		if (err) {
			cb(err);
			return;
		}
		var i=bsearch(res.rawresult,opts.vpos,true);
		if (i===-1 || i===0) {
			cb("cannot find prev hit");
			return;
		}
		var prevvpos=res.rawresult[i-1];
		//var uti=res.engine.vpos2txtid(prevvpos+1);
		res.engine.vpos2uti(prevvpos+1,function(uti){
			cb(0,{uti:uti,vpos:prevvpos});
		});
		
	});
}


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
			vpos=db.uti2vpos(opts.uti);
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
		kde.open(opts.db,function(err,db){
			if (err) {
				cb(err);
				return;
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
				cb(0,{name:tocname,toc:toc,vhits:res.rawresult,tocname:tocname});	
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

var getFieldsInRange=function(db,fieldtext,fieldvpos,fieldlen,fielddepth,from,to,text) {
	var out=[],hits=[],texts=[],depth,vlen,vp,i=bsearch(fieldvpos,from,true);
	var ft,s,l,previ;

	while (i>-1) {
		vp=fieldvpos[i];
		vlen=fieldlen[i];
		if (vp>=to) break;

		ft=fieldtext[i];
		vlen=fieldlen[i];
		depth=fielddepth[i];
		hits.push([ vp, vlen , depth]);
		texts.push(ft);
		if (fieldvpos[i+1]>=to)break;
		previ=i;
		i=bsearch(fieldvpos,fieldvpos[i+1],true);
		if (i===previ) break;
	}

	var out=kse.vpos2pos(db, from , text,  hits);

	return {texts:texts, vpos: hits, realpos: out};
}
var field2markup = function(db,markupfields,from,to,text) {
	var i,field,out={},fieldtext,fieldvpos,fieldlen,fielddepth,markups;
	if (typeof markupfields=='string') {
		markupfields=[markupfields];
	}

	for (i=0;i<markupfields.length;i++) {
		field=markupfields[i];
		fieldtext=db.get(['fields',field]);
		fieldvpos=db.get(['fields',field+'_vpos']);
		fieldlen=db.get(['fields',field+'_len']);
		fielddepth=db.get(['fields',field+'_depth']);

		if (!fieldlen) return out;
		markups=getFieldsInRange(db,fieldtext,fieldvpos,fieldlen,fielddepth,from,to,text);
		out[field]=markups;
	}
	return out;
}

var getFileSeg=function(db,opts,cb){
	var vpos=opts.vpos,uti=opts.uti,fileseg,one=false;
	if (vpos) {
		if (typeof vpos==="number") {
			vpos=[vpos];
		}

		fileseg=vpos.map(function(vp){ return db.fileSegFromVpos(vp)});
		
		db.loadSegmentId(fileseg,function(){
			cb(fileseg);	
		});
		
	} else if (uti) {
		if (typeof uti==="string") uti=[uti];
		//convert uti to filecontents,file,seg
		db.getFileSegFromUti(uti,cb);
	}
}

var fetchcontent=function(keys,db,Q,opts,cb){
	db.get(keys,function(data){
			var fields,item,out=[],i,j,k,vhits=null,hits=null,seg1,vp,vp_end;
			for (j=0;j<keys.length;j+=1) {
				hits=null;
				seg1=db.fileSegToAbsSeg(keys[j][1],keys[j][2]);
				vp=db.absSegToVpos(seg1);
				vp_end=db.absSegToVpos(seg1+1);
				if (Q) {
					vhits=kse.excerpt.hitInRange(Q,vp,vp_end,data[j]);
					hits=kse.excerpt.calculateRealPos(Q.tokenize,Q.isSkip,vp,data[j],vhits);
				}

				item={uti:opts.uti[j],text:data[j],hits:hits,vhits:vhits,vpos:vp,vpos_end:vp_end};
				if (opts.fields) {
					fields=opts.fields;
					if (typeof fields=='boolean') {
						fields=db.get('meta').toc;
					}

					if (typeof fields==="string") {
						fields=[fields];
					}

					if (fields) {
						item.values=[];
						for (k=0;k<fields.length;k+=1) {
							item.values.push(getFieldByVpos(db,fields[k],vp_end));
						}					
					}
				}

				if (opts.asMarkup) { 
					var asMarkup=opts.asMarkup;
					if (typeof asMarkup=='boolean') {
						asMarkup=db.get('meta').toc;
					}
					if (asMarkup) item.markups=field2markup(db,asMarkup,vp,vp_end,data[j]);
				}
				out.push(item);
			}
			var tocname=opts.breadcrumb;
			if (tocname && typeof tocname!="string") tocname=db.get("meta").toc;
			if (tocname!==undefined) {
				db.getTOC({tocname:tocname},function(toc){
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
}

var fetch_res=function(db,Q,opts,cb){
	if (db.get("meta").indexer===10) {
		var keys=indexer10.fetch_res_prepare_keys(db,opts);
		fetchcontent(keys,db,Q,opts,cb);
	} else {
		getFileSeg(db,opts,function(file_segments){
			var uti=opts.uti;
			if (typeof uti==="string") uti=[uti];
		 	var keys=file_segments.map(function(fseg){return ["filecontents",fseg.file,fseg.seg]});
		 	fetchcontent(keys,db,Q,opts,cb);
		});
	}
};
var fetchfields=function(opts,db,cb1){
	var i,fields=opts.fields,fieldskey=[];
	if (typeof fields==="boolean") {
		fields=db.get('meta').toc;
	}

	if (typeof fields==="string") {
		fields=[fields];
	}
	if (!fields) {
		cb1();
		return;
	}

	for (i=0;i<fields.length;i+=1) {
		fieldskey.push(["fields",fields[i]]);
		fieldskey.push(["fields",fields[i]+"_vpos"]);
		fieldskey.push(["fields",fields[i]+"_len"]);
		fieldskey.push(["fields",fields[i]+"_depth"]);
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
						fetchfields(opts,db,function(a,b){
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
					var fres=fetch_res;

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

var excerpt2defaultoutput=function(excerpt,cb) {
	var out=[],i,ex,vpos,txtid;
	var sidsep=this.sidsep;
	//loadSegmentId

	var nfiles=excerpt.map(function(ex){return ex.file});

	this.loadSegmentId(nfiles,function(){
		for (i=0;i<excerpt.length;i+=1) {
			ex=excerpt[i];
			var uti=this.fileSeg2uti(ex);
			out.push({uti:uti,text:ex.text,hits:ex.realHits,vhits:ex.hits,vpos:ex.start});
		}

		cb(out);
	}.bind(this))
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
			excerpt2defaultoutput.call(res.engine,res.excerpt,function(out){
				cb(0,out);	
			})
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
			prevdepth=65535,inrange=false, fieldhits ,filesegs,vposs=[],uti;
			if (!rawresult||!rawresult.length) {
				matches=filterField(fields,regex,filterfunc);
				for (i=0;i<matches.length;i+=1) {
					item=matches[i];
					items.push({text:item.text , vpos:fieldsvpos[item.idx]});
				}
				cb(0,items,db);
			} else {
				var t=new Date();
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
		      	item={text: item, vpos: fieldhit[0], uti:db.vpos2uti(fieldhit[0]) , vhits:fieldhit};
		      	if (postfunc) {
		      		postfunc(item);
		      	}
		      	items.push(item);
		      }
		    }		
				cb(0,items,db);
			}
		});
	});
};
/*
*/

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

  var field=opts.field;

  if (typeof field==="boolean" && opts.field) {
  	field=db.get("meta").toc;	
  }

	if (field) {
		if (opts.ranges) {
			rawresult=trimResult(rawresult,opts.ranges);
		}
		groupByField(db,rawresult,field,opts.regex,filterfunc,null,cb);
	} else {
		//TODO , groupBySegments 
		if (db.get("meta").indexer===10) {
			indexer10.groupByUti(db,rawresult,opts.regex,filterfunc,cb);
		} else {
			throw "field is not specified";
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

var tryOpen=function(kdbid,opts,cb){
	if (typeof opts=="function") {
		cb=opts;
		opts={};
	}

	if (typeof window!=='undefined') {
		var nw=window.process!==undefined && window.process.__node_webkit;
		var protocol='';
		if (window.location) protocol=window.location.protocol;
		var socketio= window.io!==undefined;
 
		if ( (protocol==="http:" || protocol==="https:") && !socketio) {
			cb("http+local file mode");
			return;
		}

		if (protocol==="file:" && !nw) {
			cb("local file mode");
		}
	}

	kde.open(kdbid,opts,function(err,db){
		cb(err,db);
	});
};
var applyMarkups=function(text,opts,func) {
	var r=require("./highlight").applyMarkups.apply(this,arguments);
	return r;
}

var getMarkupPainter=function(db) {
	return {tokenize:db.analyzer.tokenize, isSkip:db.analyzer.isSkip, applyMarkups:applyMarkups};
}
var renderHits=function(text,hits,func){
	if (!text) return [];
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
			cb(0,db.vpos2uti(opts.vpos));
		}
	});
};

var uti2vpos=function(opts,cb){
	kde.open(opts.db,function(err,db){
		if (err) {
			cb(err);
		} else {
			cb(0,db.uti2vpos(opts.uti));
		}
	});
};



var sibling=function(opts,cb) {
	/*
	  specify opts.nfile 
	  or use uti format "nfile@sid"
	*/
	var uti=opts.uti,keys,segs,idx,nfile;

	kde.open(opts.db,function(err,db){
		if (err) {
			cb(err);
			return;
		}

		if (db.get("meta").indexer===10){
			indexer10.sibling(opts,cb);
			return;
		}

		if (typeof uti==="string") {
			var file_sid=uti.split(db.sidsep);
			nfile=parseInt(file_sid[0]);
		}		
		if ( opts.vpos !==undefined && opts.uti===undefined) {
			//uti=db.vpos2txtid(opts.vpos);
			nfile=db.fileSegFromVpos(opts.vpos).file;
		}

		if (isNaN(nfile)) {
			if (typeof opts.nfile === undefined) {
				cb("must specify file");
				return;
			} else {
				nfile=opts.nfile;
			}		
		}

		db.get(["segments",nfile],function(segs){
			if (!segs) {
				cb("invalid nfile: "+nfile);
				return;			
			}
			var offsets=db.getFileSegOffsets(nfile);
			if (uti) {
				idx=segs.indexOf(uti);
			} else {
				idx=bsearch(offsets,opts.vpos,true);
				if(idx>0) idx--;
			}
			
			cb(0,{sibling:segs,idx:idx,offsets:offsets});
		});

	});
};



var getFieldRange=function(opts,cb){
	var findField=function(array,item) {
		var i;
		for(i=0;i<array.length;i+=1){
			if (array[i]===item) {
				return i;
			}
		}
		return -1;
	};	

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

var iterateInner=function(db,action,opts,cb,context){
	var out=[], next=opts.uti,  count=opts.count||10;

	var nfile_sid=db.parseUti(opts.uti);
	var nfile=nfile_sid[0],sid=nfile_sid[1];
	var filename=nfile_sid[2];

	db.loadSegmentId([nfile_sid[0]],function(){
		var segments=this.get(["segments",nfile]),i;

		var now=segments.indexOf(sid);
		if (now==-1) {
			cb("segment not found in the file"+opts.uti);
			return;
		}

		for (i=1;i<count+1;i+=1) {
			if (action=="next") {
				if (now+i<segments.length) out.push(filename+db.sidsep+segments[now+i]);
				else break;
			} else {
				if (now-i>=0) out.unshift(filename+db.sidsep+segments[now-i]);
				else break;
			}
		}		

		opts.uti=out;
		fetch(opts,cb,context);
	});
};

var iterate=function(action,opts,cb,context) {
	if (typeof opts.uti!=="string") {
		cb("uti must be a string");
		return;
	}
	if (!opts.q) {
		kde.open(opts.db,function(err,db){
			if (!err) {
				if (db.get("meta").indexer===10) {
					indexer10.iterateInner(db,action,opts,cb,context);
					fetch(opts,cb,context);
				} else {
					iterateInner(db,action,opts,cb,context);					
				}
			}
			else {
				cb(err);
			}
		},context);
		return;
	}
	kse.search(opts.db,opts.q,opts,function(err,res){
		if (!err) {
			if (db.get("meta").indexer===10) {
				indexer10.iterateInner(db,action,opts,cb,context);
				fetch(opts,cb,context);
			}else {				
				iterateInner(res.engine,action,opts,cb,context);
			}
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
	iterate("next",opts,cb,context);
};

var prev=function(opts,cb,context) {
	iterate("prev",opts,cb,context);
};
var open=function(opts,cb){
	var db=opts.db;
	if (typeof opts=="string") db=opts; //accept open("dbname")
	kde.open(db,opts,cb);
}
var enumKdb=function(opts,cb,context) {
	if (typeof opts==="function") {
		cb=opts;
		opts={};
	}

	var taskqueue=[],out=[];
	var enumTask=function(dbname){
		taskqueue.push(function(err,data){
			if (!err && !(typeof data==='object' && data.empty)) {
				out.push([data.dbname,data.meta]);
			}
			kde.open(dbname,{metaonly:true},taskqueue.shift(0,data));
		});
	};

	kde.enumKdb(opts,function(kdbs){
		kdbs.forEach(enumTask);
		taskqueue.push(function(err,data){
			if (!err && !(typeof data==='object' && data.empty)) {
				out.push([data.dbname,data.meta]);
			}	
			cb(out);
		});
		taskqueue.shift()(0,{empty:true});
	});
}
var API={
	next:createTask(next),
	prev:createTask(prev),
	nextUti:createTask(nextUti),
	prevUti:createTask(prevUti),
	nextHit:createTask(nextHit),
	prevHit:createTask(prevHit),
	vpos2uti:createTask(vpos2uti),
	uti2vpos:createTask(uti2vpos),	
	toc:createTask(toc),
	fetch:createTask(fetch),
	sibling:createTask(sibling),
	excerpt:createTask(excerpt),
	filter:createTask(filter),
	enumKdb:createTask(enumKdb),
	getFieldRange:createTask(getFieldRange),
	search:createTask(search),	
	open:createTask(open),

	fillHits:fillHits,
	renderHits:renderHits,
	applyMarkups:applyMarkups,
	tryOpen:tryOpen,
	get:get,
	treenodehits:treenodehits,
	getMarkupPainter:getMarkupPainter
};
module.exports=API;