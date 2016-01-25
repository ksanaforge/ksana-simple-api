/*
	combine hits and tags, for full text mode
*/
var applyMarkups=function(text,opts){
	var hits=opts.vhits||[];
	var markups=opts.markups;
	
	var hitclass=opts.hitclass||'hl';
	var output='',O=[],j=0,m=0,k,M;

	var tokens=opts.tokenize(text).tokens;
	var vpos=opts.vpos;
	var i=0,str=[];
	var hitstart=0,hitend=0;
	var tagn={},tagstart={},tagend={},tagclass="",lastclasses="";
	var para=[],classes;
	for (m in markups) {
		tagn[m]=0;
		tagstart[m]=0;
		tagend[m]=0;
	} 
	
	var breakat=[],start=0;

	while (i<tokens.length) {
		var skip=opts.isSkip(tokens[i]);		
		var token=tokens[i];
		if (opts.isBreaker) {
			if (opts.isBreaker( token, i,tokens)){
				breakat.push(i);
			}
		}
		i++;
	}
	
	i=0,nbreak=0;
	while (i<tokens.length) {
		var skip=opts.isSkip(tokens[i]);
		var hashit=false;
		
		var token=tokens[i];

		if (opts.isBreaker && para.length) {
			if (opts.isBreaker( token, i,tokens)){
				O.push(opts.createPara(para,O.length));
				para=[];
			}
		}
		if (i<tokens.length) {
			if (skip) {
				str.push(token);
			} else {
				classes="";	
				//check hit
				if (j<hits.length && vpos==hits[j][0]) {
					var nphrase=hits[j][2] % 10, width=hits[j][1];
					hitstart=hits[j][0];
					hitend=hitstart+width;
					j++;
				}
				if (vpos>=hitstart && vpos<hitend) {
					classes=hitclass+" "+hitclass+nphrase;
				}
				for (m in markups) {
					M=markups[m].vpos;
					k=tagn[m];
					if (k<M.length && vpos===M[k][0]) {
						var width=M[k][1];
						tagstart[m]=M[k][0];
						tagend[m]=tagstart[m]+width;
						tagclass=m+M[k][2];
						k++;
					}
					tagn[m]=k;
					if (vpos>=tagstart[m]&& vpos<tagend[m]) classes+=" "+tagclass;
				}

				if (classes!==lastclasses) {
					para.push(opts.createToken(str,{className:lastclasses,vpos:vpos,key:start}));
					lastclasses=classes;
					start=i;
					str=[token];
				} else {
					str.push(token);
				}
			}
		}

		if (nbreak<breakat.length && breakat[nbreak]===i) {
			para.push(opts.createToken(str,{className:classes,vpos:vpos,key:start}));
			O.push(opts.createPara(para,O.length));
			para=[];
			str=[];
			start=i;
			nbreak++;
		}

		if (!skip) vpos++;
		i++; 
	}

	if (str.length) {
		para.push(opts.createToken(str,{className:classes,vpos:vpos,key:start}));
		O.push(opts.createPara(para,O.length));	
	}

	return O;
}

module.exports={applyMarkups:applyMarkups}