"use strict";
var rangeOfTreeNode=function(toc,n) {
  //setting the ending vpos of a treenode
  //this value is fixed when hits is changed, but not saved in kdb
  if (toc[n].end!==undefined) {
    return;
  }

  if (n+1>=toc.length) {
    return;
  }
  var depth=toc[n].d , nextdepth=toc[n+1].d ,next;
  if (n===toc.length-1 || n===0) {
      toc[n].end=Math.pow(2, 48);
      return;
  } 

  if (nextdepth>depth){
    if (toc[n].n) {
    	if (toc[toc[n].n]) {
        toc[n].end=toc[toc[n].n].vpos;
      } else {
        toc[n].end= Number.MAX_VALUE;  
      }
    } else { //last sibling
      next=n+1;
      while (next<toc.length && toc[next].d>depth) {
        next+=1;
      }
      if (next===toc.length) {
        toc[n].end=Math.pow(2,48);
      } else {
        toc[n].end=toc[next].voff;
      }
    }
  } else { //same level or end of sibling
    toc[n].end=toc[n+1].vpos;
  }
};

var calculateHit=function(toc,hits,n) {
  var i, start=toc[n].vpos, end=toc[n].end,hitcount=0,firstvpos=toc[n].vpos;
  if (n===0) {
    return hits.length;
  }
  for (i=0;i<hits.length;i+=1) {
    if (hits[i]>=start && hits[i]<end) {
      if (hitcount===0){
        firstvpos=hits[i];
      }
      hitcount+=1;
    }
  }
  return {hitcount:hitcount,firstvpos:firstvpos};
};

var treenodehits=function(toc,hits,n) {
  if (!hits || !hits.length) {
    return 0;
  }

  if (toc.length<2) {
    return 0 ;
  }

  //need to clear toc[n].hit when hits is changed.
  //see index.js clearHits()
   if (!toc[n]) {
    return 0;
  }

  if (toc[n].hit!==undefined) {
    return toc[n].hit;
  }

  rangeOfTreeNode(toc,n);
  var res=calculateHit(toc,hits,n);
  toc[n].hit=res.hitcount;
  toc[n].firstvpos=res.firstvpos;
  return res.hitcount;
};

module.exports=treenodehits;