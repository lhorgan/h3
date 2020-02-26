const readline = require('readline');
const fs = require('fs');
const os = require('os');
const { Worker } = require('worker_threads');
const lineByLine = require('n-readlines');
const NodeCache = require( "node-cache" );
const shuffle = require('shuffle-array');

const micro = require("microtime");
const TIME_TO_WAIT = 1000000;

var level = require('level');

class Earl {
    constructor(ifname, results_name, binSize, dbname) {
        this.dispatchedURLIndex = 0;
        this.processedURLIndex = 0;
        this.binSize = binSize; 
        this.urlsToWrite = [];
        this.results_name = results_name;
        this.accessLogs = new NodeCache();
        
        this.readstream = new lineByLine(ifname);
        this.urlCount = 0;
        this.allLinesRead = false;

        this.db = level(dbname);

        this.proxyConfig();
    }

    async go(ifname) {
        this.workers = this.makeWorkers();
        this.initialAssignWorkers();
        //console.log("workers assigned");
    }

    makeWorkers() {
        let workers = [];
        //os.cpus().length

        console.log("Utilizing " + os.cpus().length + " CPUs to make workers...");

        for(let i = 0; i < os.cpus().length; i++) {
            let worker = new Worker("./h.js", {});
            worker.on("message", (message) => {
                //console.log("we got a message in the main thread");
                this.handleWorkerMessage(message, worker);
            });
            workers.push(worker);
        }
        return workers;
    }

    handleWorkerMessage(message, worker) {
        //console.log(JSON.stringify(message));
        if(message["kind"] === "lastAccessed") {
            let domain = message["domain"];
            let proxy, time = this.getBestProxy(domain);

            this.getBestProxy(domain);

            worker.postMessage({kind: "lastAccessed", mid: message["mid"], time: time, proxy: proxy});
        }
        else if(message["kind"] === "updateAccessLogs") {
            let domain = message["domain"];
            let time = message["time"];
            let proxy = message["proxy"];
            this.accessLogs.set(domain + ":" + proxy, time, 5);
        }
        else if(message["kind"] === "writeURL") {
            if(this.processedURLIndex % 1 === 0) {
                console.log(message.url + " --< " + message.origURL + ": " + this.processedURLIndex);
            }
            this.urlsToWrite.push(message);
            if(this.urlsToWrite.length >= 50) {
                this.writeURLs();
            }

            this.processedURLIndex++;

            let [url, year] = this.getNextURL();
            if(url) {
                worker.postMessage({"url": url, "queue": false, "year": year});
                this.dispatchedURLIndex++;
            }
            else {
                console.log("PROCSSED " + this.processedURLIndex + ", READ " + this.urlCount);
                if(this.processedURLIndex === this.urlCount && this.allLinesRead) {
                    console.log("All URLs have been processed!");
                    for(let i = 0; i < this.workers.length; i++) {
                        this.workers[i].terminate();
                    }
                }
                this.writeURLs();
            }
        }
    }

    getBestProxy(domain) {
        let proxiesList = [];
        for(let i = 0; i < this.proxies.length; i++) {
	    //console.log("PROXY " + JSON.stringify(this.proxies[i]));
            if(this.proxies[i].state === "running") {
                proxiesList.push(this.proxies[i].ip);
            }
        }
	//console.log(JSON.stringify(proxiesList));


        let proxies = shuffle(proxiesList);
        let res = [];

        for(let i = 0; i < proxies.length; i++) {
            let time = this.accessLogs.get(domain + ":" + proxies[i]);
            if(!time) {
                time = 0;
            }
            res = [proxies[i], time];
            if((micro.now() - time) > TIME_TO_WAIT) {
                return res;
            }
        }

        return res;
    }

    async getNextURL() {
        let line = this.readstream.next();

        while(true) { // this'll just keep going 'til we find something that isn't in the database
            if(line) {
                this.urlCount++;
                line =  line.toString("utf-8");
                let [url, year] = line.trim().split("\t");

                let inDB = await db.get(url)
                                .then((value) => { return true })
                                .catch((err) => { return false });
                
                if(!inDB) {
                    if(!year) {
                        year = 2020;
                    }
                    return [url, year];
                }
            }
            else {
                if(this.allLinesRead === false) {
                    console.log("End of file reached!");
                }
                this.allLinesRead = true;
                return [null, null];
            }
        }
    }

    writeURLs() {
        //console.log("Writing a batch of URLs");
        let urlsCopy = JSON.parse(JSON.stringify(this.urlsToWrite)); // live with it
        this.urlsToWrite = [];
        let urlStr = "";
        for(let i = 0; i < urlsCopy.length; i++) {
            urlStr += urlsCopy[i].url + "\t" + 
                      urlsCopy[i].origURL + "\t" + 
                      urlsCopy[i].urlWithParams + "\t" + 
                      urlsCopy[i].year + "\t" + 
                      urlsCopy[i].size + "\t" + 
                      urlsCopy[i].error;
            if(urlsCopy[i].error) {
                //console.log("There has been an error, we are writing "  + urlsCopy[i].errorMessage);
                urlStr += "\t" + '"' + urlsCopy[i].errorMessage + '"';
            }
            urlStr += "\r\n";
        }

        var stream = fs.createWriteStream(this.results_name, {flags:'a'});
        stream.write(urlStr);
        stream.end();
    }

    initialAssignWorkers() {
        console.log("ASSINGING WORKERS");
        for(let i = 0; i < this.binSize; i++) {
            for(let j = 0; j < this.workers.length; j++) {
                let [url, year] = this.getNextURL(this.readstream);
                if(url) {
                    console.log("URL: " + url);
                    this.workers[j].postMessage({"url": url, "queue": false, "year": year});
                    this.dispatchedURLIndex++;
                }
                else {
                    //console.log("We have reached the end of the file");
                }
            }
        }

        for(let i = 0; i < this.workers.length; i++) {
            this.workers[i].postMessage({"go": true});
        }
    }

    proxyConfig() {
        getProxies((instances) => {
            console.log("here are the instances");
            console.log(instances);
            this.proxies = instances;
            
            this.go();

            this.proxyLoop(0);
        });
    }

    proxyLoop(idx) {
        this.proxies[idx].state = "stopping";
        setTimeout(() => {
            restartProxy(this.proxies[idx].id, () => {
                console.log("it has now been restarted");
                this.proxies[idx].state = "running";
		this.proxyLoop(++idx % this.proxies.length);
            });
        }, 60000);
    }
}

var AWS = require('aws-sdk');
// Set the region 
AWS.config.update({region: 'us-east-2'});

// Create EC2 service object
var ec2 = new AWS.EC2({apiVersion: '2016-11-15'});
//let e = new Earl("/media/luke/277eaea3-2185-4341-a594-d0fe5146d917/twitter_urls/todos/11226.tsv", "results/0.tsv", 50);
//let e = new Earl("../xaa", "../res.tsv", 100);

// Load the AWS SDK for Node.js

function getProxies(cb) {
    var params = {
        DryRun: false,
        Filters: [{Name: 'tag:Name', Values: ['squid*']}]
    };

    // Call EC2 to retrieve policy for selected bucket
    ec2.describeInstances(params, function(err, data) {
        let instances = [];

        if(err) {
            console.log("Error", err.stack);
        } 
        else {
            //console.log("Success", JSON.stringify(data));

            for(let i = 0; i < data.Reservations.length; i++) {
                for(let j = 0; j < data.Reservations[i].Instances.length; j++) {
                    let instance = data.Reservations[i].Instances[j];
                    let ip = instance.PrivateIpAddress;
                    let state = instance.State.Name;
                    let id = instance.InstanceId;
                    console.log(ip + " (" + id + "): " + state);
                    instances.push({"ip": ip, "state": state, "id": id});
                }
            }

            cb(instances);
        }
    });
}

function restartProxy(id, cb) {
    var params = {
        InstanceIds: [id],
        DryRun: false
    };

    ec2.stopInstances(params, function(err, data) {
        if(err) {
            console.log("Error", err);
            cb("error");
        } 
        else if (data) {
            console.log("Success", data.StoppingInstances);
            checkState(id, "stopped", (state) => {
                console.log("The instance is now stopped");
                startProxy(id, () => {
                    console.log("We're running!");
                    cb("success");
                });
            });
        }
    });
}

function startProxy(id, cb) {
    var params = {
        InstanceIds: [id],
        DryRun: false
    };

    ec2.startInstances(params, function(err, data) {
        if(err) {
            console.log("Error", err);
            cb("error");
        } 
        else if (data) {
            console.log("Success", data.StoppingInstances);
            checkState(id, "running", (state) => {
                console.log("The instance is now running again");
                cb("success");
            });
        }
    });
}

function checkState(id, state, cb) {
    var params = {
        InstanceIds: [id],
        DryRun: false
    };

    ec2.describeInstances(params, function(err, data) {
        if(err) {
            console.log("Error", err.stack);
        }
        else {
            for(let i = 0; i < data.Reservations.length; i++) {
                for(let j = 0; j < data.Reservations[i].Instances.length; j++) {
                    let instance = data.Reservations[i].Instances[j];
                    let currState = instance.State.Name;
                    if(currState === state) {
                        cb(state);
                    }
                    else {
                        setTimeout(() => {
                            checkState(id, state, cb);
                        }, 1000);
                    }
                    break;
                }
            }
        }
    });
}

