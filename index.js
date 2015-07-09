var kse=require("ksana-search");
var kde=require("ksana-database");
var bsearch=kde.bsearch;


var next=function(opts,cb,context) {
}
var prev=function(opts,cb,context) {
}

var filesegwithhits=function(res) {
	console.log(res)
	//{t:texts[i],d:depths[i], vpos:voffs[i]}
}
var toc=function(opts,cb,context) {
	var that=this;
	kse.search(opts.db,opts.q,{},function(err,res){
		if (err) {
			cb(err);
		} else {
			var tocname=res.engine.get("meta").toc;
			res.engine.getTOC({tocname:tocname},function(data){
				cb(0,{name:tocname,toc:data,hits:res.rawresult});
			});
		}
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

var hits2markup=function( Q,file,seg){
	var seg1=this.fileSegToAbsSeg(file,seg);
	var vpos=this.absSegToVpos(seg1-1);
	var vpos2=this.absSegToVpos(seg1);
	//console.log("rawresult",Q.rawresult,vpos,vpos2)
	var hits=kse.excerpt.hitInRange(Q,vpos,vpos2);
	for (var i=0;i<hits.length;i++) {
		hits[i][2]="HL"+hits[i][2];
	}
	return hits;
}
var fetch=function(opts,cb,context) {
	var that=this;
	kse.search(opts.db,opts.q,{},function(err,res){
		if (err) {
			cb(err);
		} else {
			var keys=txtids2key.call(res.engine,opts.uti);
			res.engine.get(keys,function(data){
				var out=[];
				for (var i=0;i<opts.uti.length;i++) {
					var hits=hits2markup.call(res.engine,res,keys[i][1],keys[i][2]);
					out.push({uti:opts.uti[i],text:data[i],hits:hits});
				}
				cb(0,out);
			});
		}
	});	
}
var excerpt=function(opts,cb,context) {
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
		var txtid=db.get("txtid");
		for (var i=0;i<opts.sentence.length;i++) {
			var q=opts.sentence.substr(i);
			out=out.concat(beginWith(q,txtid));
		}
		cb(0,out);
	});
}
var filter=function(opts,cb,context) {
}


var API={
	next:next,
	prev:prev,
	toc:toc,
	fetch:fetch,
	excerpt:excerpt,
	scan:scan,
	filter:filter	
}
module.exports=API;