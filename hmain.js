const readline = require('readline');
const fs = require('fs');
const os = require('os');
const { Worker } = require('worker_threads');
const lineByLine = require('n-readlines');
const NodeCache = require( "node-cache" );

class Earl {
    constructor(ifname, results_name, binSize) {
        this.dispatchedURLIndex = 0;
        this.processedURLIndex = 0;
        this.binSize = binSize; 
        this.urlsToWrite = [];
        this.results_name = results_name;
        this.accessLogs = new NodeCache();
        
        this.readstream = new lineByLine(ifname);
        this.urlCount = 0;
        this.allLinesRead = false;

        this.go();
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
            let time = this.accessLogs.get(domain);
            if(!time) {
                time = 0;
            }
            worker.postMessage({kind: "lastAccessed", mid: message["mid"], time: time});
        }
        else if(message["kind"] === "updateAccessLogs") {
            let domain = message["domain"];
            let time = message["time"];
            this.accessLogs.set(domain, time, 5);
        }
        else if(message["kind"] === "writeURL") {
            console.log(message.url + " --< " + message.origURL + ": " + this.processedURLIndex);
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

    getNextURL() {
        let line = this.readstream.next();
        if(line) {
            this.urlCount++;
            line =  line.toString("utf-8");
            let [url, year] = line.trim().split("\t");
            return [url, year];
        }
        else {
            if(this.allLinesRead === false) {
                console.log("End of file reached!");
            }
            this.allLinesRead = true;
            return [null, null];
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
}

//let e = new Earl("/media/luke/277eaea3-2185-4341-a594-d0fe5146d917/twitter_urls/todos/11226.tsv", "results/0.tsv", 50);
let e = new Earl("../xaa", "results.tsv", 50);
