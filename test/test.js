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
			console.log(data)
			done();
		});
	});

	it("fetch test",function(done){
		ksa.fetch({db:"sampledict",q:"內",uti:["菩提","菩提心"]},function(err,data){
			console.log(JSON.stringify(data))
			done();
		});
	})
})