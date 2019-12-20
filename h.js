const micro = require("microtime");
const request = require("request");
const cheerio = require("cheerio");
const URL = require('url');
const difflib = require('difflib');

const TIME_TO_WAIT = 1000000;
const TIMEOUT = 10000;

const { Worker, isMainThread, parentPort } = require('worker_threads');

class UrlProcessor {
    constructor() {
        this.errors = {};
        this.maxRetries = 2;
        this.callbacks = {};
        this.completedNum = 0;
        this.queue = [];
    }

    addToQueue(entry) {
        this.queue.push(entry);
    }

    go() {
        for(let i = 0; i < this.queue.length; i++) {
            this.process(this.queue[i]);
        }
        this.queue = [];
    }

    // url --> original url, new url
    async followRedirects(entry) {
        //console.log("FOLLOWING REDIRECTS FOR " + entry.url + " orig url " + entry.origURL);
        let [resp, body] = await this.hitURL(entry.url, {
                                        method: "HEAD",
                                        followAllRedirects: false,
                                        followRedirect: false,
                                        timeout: TIMEOUT
                                    });

        let newURL = resp.headers.location;//resp.request.uri.href;
        if(resp.statusCode >= 300 && resp.statusCode < 400 && newURL) {
            //console.log("HERE IS OUR NEW URL " + newURL);
            let parsedNew = URL.parse(newURL);
            if(!parsedNew.hostname && parsedNew.path) {
                let path = parsedNew.path;
                if(path[0] !== "/") {
                    path = "/" + path;
                }
                let parsedOrig = URL.parse(resp.request.uri.href);
                newURL = parsedOrig.protocol + "//" + parsedOrig.hostname + path;
            }
            entry.url = newURL;
            return this.followRedirects(entry);
        }
        else { // hooray!
            return entry;
        }
    }

    delay(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    postMessage(data, callback) {
        data["mid"] = this.randomString();
        parentPort.postMessage(data);
        if(callback) {
            this.callbacks[data["mid"]] = callback;
        }
    }

    onMessage(data) {
        //console.log("THE MESSAGE FROM MAIN " + JSON.stringify(data));
        if("mid" in data && data["mid"] in this.callbacks) {
            //console.log("MID " + data["mid"]);
            this.callbacks[data["mid"]](data);
            delete this.callbacks[data["mid"]];
        }
    }

    randomString() {
        return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    }

    async process(entry) {
        try {
            if(!("origURL" in entry)) {
                entry.origURL = entry.url;
            }

            //console.log("Processing entry " + JSON.stringify(entry));

            /**
             * Follow redirects safely
             */
            entry = await this.followRedirects(entry).catch((err) => {
                throw(err);
            });
            entry.urlWithParams = entry.url;

            // //console.log("We have succesffully followed the redirects");
            
            /**
             * Read the contents of the page and parse the DOM with Cheerio
             */
            //console.log("READING CONTENT FOR " + entry.url);
            entry["text"] = await this.getContent(entry.url).catch((err) => {
                throw(err);
            });

            //console.log("We have successfully fetched the content");

            entry["tree"] = cheerio.load(entry["text"]);
            //console.log("TITLE " + entry["tree"]("title"));

            /**
             * Try to read the canonical URL from the page
             * If there is one, stop, we're done.
             * Otherwise, keep going.
             */
            let canURL = this.getCanonicalURL(entry);
            if(canURL) {
                entry["url"] = canURL;
            }
            else {
                /**
                 * Separate the query parameters from the URL using Node's "url" package
                 */
                let parsed = URL.parse(entry.url, {parseQueryString: true});
                entry["params"] = parsed.query;
                entry["url"] = entry.url.split("?")[0];

                /**
                 * Remove any unnecessary query params from the URL
                 */
                entry = await this.stripParams(entry);
            }

            //console.log("This entry is complete " + entry.url + ", " + this.completedNum++);
            this.postMessage({"kind": "writeURL", 
                              "url": entry.url, 
                              "origURL": entry.origURL,
                              "urlWithParams": entry.urlWithParams,
                              "year": entry.year,
                              "error": false});
        } catch(err) {
            //console.log(err);
            if(typeof(err) === "object") {
                err = err.toString();
            }
            //console.log("Error on " + entry.url);
            //console.log("\n");
            //console.log("\n\n" + err + "\n\n");
            if(!(entry.url in this.errors)) {
                this.errors[entry.url] = 0;
            }
            this.errors[entry.url] += 1;

            if(this.errors[entry.url] >= this.maxRetries) {
                //console.log("Max retry limit for " + entry.url + " exceeded.");
                //console.log("HERE IS THE ERROR WE ARE PASSING: " + err);
                // TODO: post a complete message for this URL, with error
                //console.log("so, error did occur");
                this.postMessage({"kind": "writeURL", 
                                  "url": entry.url, 
                                  "origURL": entry.origURL, 
                                  "urlWithParams": entry.urlWithParams,
                                  "year": entry.year,
                                  "error": true,
                                  "errorMessage": err});
                delete this.errors[entry.url];
            }
            else {
                // try again
                this.process(entry);
            }
        }
    }

    /**
     * Reads the canonical URL, if there is one
     */
    getCanonicalURL(entry) {
        let tree = entry.tree;
        let canURL = tree("meta[property='og:url']").attr("content");
        if(canURL) {
            return canURL;
        }
        
        canURL = tree("link[rel='canonical']").attr("href");
        if(canURL) {
            return canURL;
        }

        return null;
    }

    async stripParams(entry) {
        //console.log("\n");
        //console.log(JSON.stringify(entry.url));
        //console.log(JSON.stringify(entry.params));
        if(Object.keys(entry.params).length === 0) {
            return entry;
        }

        let parsedURL = URL.parse(entry.url, {parseQueryString: true});
        let params = entry["params"];
        let newParams = {};
        for(let i = 1; i < Object.keys(params).length; i++) {
            let paramName = Object.keys(params)[i];
            parsedURL.query[paramName] = params[paramName];
            newParams[paramName] = params[paramName];
        }
        let newURL = URL.format(parsedURL);
        //console.log("Here is the new URL we are trying " + newURL);
        let newText = await this.getContent(newURL);
        
        let a = {"text": entry.text, "tree": entry.tree};
        let b = {"text": newText, "tree": cheerio.load(newText)};

        // we removed one param - is the content the same?
        if(!this.isContentDifferent(a, b)) { // the content IS the same with the one removed param
            //console.log("REMOVING PARAM " + Object.keys(params)[0] + " did not result in a change");
            // so we just drop that param
            entry["params"] = newParams;
            this.logUselessParam(Object.keys(params)[0], parsedURL.host);
        }
        else {
            //console.log("REMOVING PARAM" + Object.keys(params)[0] + " CHANGED THE PAGE!!!");
            entry["params"] = newParams;
            parsedURL = URL.parse(entry.url, {parseQueryString: true});
            let removedParamName = Object.keys(params)[0];
            parsedURL.query[removedParamName] = params[removedParamName];
            entry["url"] = URL.format(parsedURL);
        }

        return this.stripParams(entry);
    }

    /**
     * todo, just there to note that some params aren't needed
     */
    logUselessParam(param, domain) {
        //let parsedURL = URL.parse(entry.url);
    }

    getDomain(url) {
        let parsedURL = URL.parse(url);
        return parsedURL.host;
    }

    // url --> time when last accessed
    async lastAccessed(url) {
        let domain = this.getDomain(url);

        //console.log("Hi, so, we are accessing a time for " + url);
        return new Promise((resolve) => {
            this.postMessage({"kind": "lastAccessed", "domain": domain}, (data) => {
                //console.log("Here is the time we read " + data.time);
                resolve(data.time);
            });
        });
    }

    updateAccessLogs(url) {
        let domain = this.getDomain(url);

        this.postMessage({"kind": "updateAccessLogs", "domain": domain, "time": micro.now()});
    }

    async hitURL(url, options) {
        let timeOfLastAccess = await this.lastAccessed(url);
        if(micro.now() - timeOfLastAccess > TIME_TO_WAIT) {
            //console.log("Fantastic, hitting URL");
            return new Promise((resolve, reject) => {
                this.updateAccessLogs(url);
                options["url"] = url;
                options["headers"] = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.75 Safari/537.36",
                                      'Connection': 'keep-alive', 'Accept-Encoding': 'gzip, deflate', 'Accept': '*/*'};
                let r = request(options, (err, resp, body) => {
                    if(err) {
                        //console.log(typeof(err.toString()));
                        //console.log(err.toString());
                        reject(err.toString());
                    }
                    else if(resp.statusCode >= 400) {
                        //console.log("WE HIT AN ERROR CODE");
                        reject("Status code: " + resp.statusCode);
                    }
                    else {
                        //console.log("RESOLVING URL, yay!")
                        resolve([resp, body]);
                    }
                });
                //console.log("HERE ARE OUR HEADERS ");
                //console.log(r.headers);
            });
        }
        else {
            //console.log("We must wait a bit because " + url + " has been accessed too recently.");
            let timeToDelay = Math.max(TIME_TO_WAIT - (micro.now() - timeOfLastAccess), 0);
            timeToDelay *= (1 + Math.random());

            //console.log("DELAYING " + timeToDelay / 1000 + " milliseconds");
            //await this.delay(timeToDelay / 1000);
            //console.log("Delay over");
            return this.hitURL(url, options);
        }
    }

    // url --> text of page at URL
    async getContent(url) {
        let [resp, body] = await this.hitURL(url, {timeout: TIMEOUT});

        return body;
    }

    // page, page --> is the text different? true/false
    isContentDifferent(a, b) {
        let aTitle = a.tree("title").text();
        let bTitle = b.tree("title").text();

        let s = new difflib.SequenceMatcher(null, a.text, b.text);
        let diff = s.quickRatio();

        //console.log("A " + aTitle + " B " + bTitle);

        return ((diff < 0.98 && aTitle != bTitle) || diff < 0.05);
    }
}

urlproc = new UrlProcessor();

parentPort.on('message', (message) => {
    ////console.log("We got a message " + JSON.stringify(message));
    if("mid" in message) {
        urlproc.onMessage(message);
    }
    else if("url" in message) {
        if(message.queue) {
            urlproc.addToQueue(message);
        }
        else {
            urlproc.process(message);
        }
    }
    else if("go" in message) {
        urlproc.go();
    }
});