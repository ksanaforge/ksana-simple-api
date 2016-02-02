var taskqueue=[];
var running=false;

var runtask=function(){
	if (taskqueue.length===0) {
		running=false;
		return;
	}
	running=true;

	var task=taskqueue.pop();
	var func=task[0], opts=task[1], cb=task[2];

	func(opts,function(err,res,res2){
		cb(err,res,res2);
		setTimeout(runtask,0);
	});
}

var createTask=function(func) {
	return function(opts,cb){
		if (typeof opts==="function") {
			cb=opts;
			opts={};
		}
		taskqueue.unshift([func,opts,cb]);
		if (!running) setTimeout(runtask,0);
	}
}
module.exports={createTask:createTask};