var rangeOfTreeNode=function(toc,n) {
  //setting the ending vpos of a treenode
  //this value is fixed when hits is changed, but not saved in kdb
  if (typeof toc[n].end!=="undefined") return;

  if (n+1>=toc.length) {
    return;
  }
  var depth=toc[n].d , nextdepth=toc[n+1].d;
  if (n==toc.length-1 || n==0) {
      toc[n].end=Math.pow(2, 48);
      return;
  } else  if (nextdepth>depth){
    if (toc[n].n) {
    	if (toc[toc[n].n]) toc[n].end=toc[toc[n].n].vpos;
    	else toc[n].end= Number.MAX_VALUE;  
    } else { //last sibling
      var next=n+1;
      while (next<toc.length && toc[next].d>depth) next++;
      if (next==toc.length) toc[n].end=Math.pow(2,48);
      else toc[n].end=toc[next].voff;
    }
  } else { //same level or end of sibling
    toc[n].end=toc[n+1].vpos;
  }
}
var calculateHit=function(toc,hits,n) {

  var start=toc[n].vpos;
  var end=toc[n].end;
  if (n==0) {
    return hits.length;
  } else {
    var hitcount=0,firstvpos=start;
    for (var i=0;i<hits.length;i++) {
      if (hits[i]>=start && hits[i]<end) {
        if (hitcount==0) firstvpos=hits[i];
       hitcount++;
      }
    }
    return {hitcount:hitcount,firstvpos:firstvpos};
  }
}

var treenodehits=function(toc,hits,n) {
  if (!hits || !hits.length) return 0;

  if (toc.length<2) return 0 ;

  //need to clear toc[n].hit when hits is changed.
  //see index.js clearHits()
   if (!toc[n]) return 0;
  if (typeof toc[n].hit!=="undefined") return toc[n].hit;

  rangeOfTreeNode(toc,n);
  var res=calculateHit(toc,hits,n);
  toc[n].hit=res.hitcount;
  toc[n].firstvpos=res.firstvpos;
  return res.hitcount;
}

module.exports=treenodehits;