/*
https://docs.google.com/document/d/1d4ntQ0JOIckth-QqvzGjyvgXNjAI3tKDIUbe2nEjT3w/edit?usp=sharing
*/
var ksa=require("..");
var assert=require("assert");
describe("api test",function(){

	it("scan test",function(done){
		ksa.scan({db:"sampledict",sentence:"菩提心是學佛根本",q:"內"},function(err,data){
			assert.deepEqual(data,["菩提","菩提心","學佛"]);
			done();
		});
	});

	it("toc test",function(done){
		ksa.toc({db:"sampledict",q:"內"},function(err,data){
			//assert.deepEqual(data,["菩提","菩提心","學佛"]);
			//console.log(data)
			assert(data.toc.length,9); // engine.segcount +2
			done();
		});
	});

  //fetch a given set of uti
	it("fetch test",function(done){
		ksa.fetch({db:"sampledict",q:"內",uti:["菩提","菩提心"]},function(err,data){
			//console.log(JSON.stringify(data))
			assert(data.length,2);
			done();
		});
	});

  //next is based on fetch, given a uti, fetch next batch of text.
  it("next uti",function(done){
  	var next=ksa.next({db:"sampledict",q:"內",uti:"b",count:2},function(err,data){
  		//console.log(JSON.stringify(data));
  		assert(data.length,2);
  		done();
  	});
  })

	//fetch only text with hits
	it("excerpt test",function(done){
		ksa.excerpt({db:"sampledict",q:"內"},function(err,data){
			//console.log(JSON.stringify(data));
			assert(data.length,2);
			done();
		});
	});

	it("excerpt start from test",function(done){
		ksa.excerpt({db:"sampledict",q:"菩提",from:"菩提"},function(err,data){
			//console.log(JSON.stringify(data));
			assert(data.length,1);
			done();
		});
	});	

	it("filter",function(done){
		ksa.filter({db:"sampledict",regex:"[a-z]"},function(err,data){
			//console.log(data)
			assert(data.length,4);
			done();
		});
	});
})