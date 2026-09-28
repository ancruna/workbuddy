#!/usr/bin/env node
"use strict";
var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __esm = (fn, res) => function __init() {
  return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
};
var __commonJS = (cb, mod) => function __require() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __export = (target, all3) => {
  for (var name in all3)
    __defProp(target, name, { get: all3[name], enumerable: true });
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));
var __toCommonJS = (mod) => __copyProps(__defProp({}, "__esModule", { value: true }), mod);

// ../../node_modules/.pnpm/delayed-stream@1.0.0/node_modules/delayed-stream/lib/delayed_stream.js
var require_delayed_stream = __commonJS({
  "../../node_modules/.pnpm/delayed-stream@1.0.0/node_modules/delayed-stream/lib/delayed_stream.js"(exports2, module2) {
    var Stream = require("stream").Stream;
    var util3 = require("util");
    module2.exports = DelayedStream;
    function DelayedStream() {
      this.source = null;
      this.dataSize = 0;
      this.maxDataSize = 1024 * 1024;
      this.pauseStream = true;
      this._maxDataSizeExceeded = false;
      this._released = false;
      this._bufferedEvents = [];
    }
    util3.inherits(DelayedStream, Stream);
    DelayedStream.create = function(source, options) {
      var delayedStream = new this();
      options = options || {};
      for (var option in options) {
        delayedStream[option] = options[option];
      }
      delayedStream.source = source;
      var realEmit = source.emit;
      source.emit = function() {
        delayedStream._handleEmit(arguments);
        return realEmit.apply(source, arguments);
      };
      source.on("error", function() {
      });
      if (delayedStream.pauseStream) {
        source.pause();
      }
      return delayedStream;
    };
    Object.defineProperty(DelayedStream.prototype, "readable", {
      configurable: true,
      enumerable: true,
      get: function() {
        return this.source.readable;
      }
    });
    DelayedStream.prototype.setEncoding = function() {
      return this.source.setEncoding.apply(this.source, arguments);
    };
    DelayedStream.prototype.resume = function() {
      if (!this._released) {
        this.release();
      }
      this.source.resume();
    };
    DelayedStream.prototype.pause = function() {
      this.source.pause();
    };
    DelayedStream.prototype.release = function() {
      this._released = true;
      this._bufferedEvents.forEach(function(args) {
        this.emit.apply(this, args);
      }.bind(this));
      this._bufferedEvents = [];
    };
    DelayedStream.prototype.pipe = function() {
      var r = Stream.prototype.pipe.apply(this, arguments);
      this.resume();
      return r;
    };
    DelayedStream.prototype._handleEmit = function(args) {
      if (this._released) {
        this.emit.apply(this, args);
        return;
      }
      if (args[0] === "data") {
        this.dataSize += args[1].length;
        this._checkIfMaxDataSizeExceeded();
      }
      this._bufferedEvents.push(args);
    };
    DelayedStream.prototype._checkIfMaxDataSizeExceeded = function() {
      if (this._maxDataSizeExceeded) {
        return;
      }
      if (this.dataSize <= this.maxDataSize) {
        return;
      }
      this._maxDataSizeExceeded = true;
      var message = "DelayedStream#maxDataSize of " + this.maxDataSize + " bytes exceeded.";
      this.emit("error", new Error(message));
    };
  }
});

// ../../node_modules/.pnpm/combined-stream@1.0.8/node_modules/combined-stream/lib/combined_stream.js
var require_combined_stream = __commonJS({
  "../../node_modules/.pnpm/combined-stream@1.0.8/node_modules/combined-stream/lib/combined_stream.js"(exports2, module2) {
    var util3 = require("util");
    var Stream = require("stream").Stream;
    var DelayedStream = require_delayed_stream();
    module2.exports = CombinedStream;
    function CombinedStream() {
      this.writable = false;
      this.readable = true;
      this.dataSize = 0;
      this.maxDataSize = 2 * 1024 * 1024;
      this.pauseStreams = true;
      this._released = false;
      this._streams = [];
      this._currentStream = null;
      this._insideLoop = false;
      this._pendingNext = false;
    }
    util3.inherits(CombinedStream, Stream);
    CombinedStream.create = function(options) {
      var combinedStream = new this();
      options = options || {};
      for (var option in options) {
        combinedStream[option] = options[option];
      }
      return combinedStream;
    };
    CombinedStream.isStreamLike = function(stream4) {
      return typeof stream4 !== "function" && typeof stream4 !== "string" && typeof stream4 !== "boolean" && typeof stream4 !== "number" && !Buffer.isBuffer(stream4);
    };
    CombinedStream.prototype.append = function(stream4) {
      var isStreamLike = CombinedStream.isStreamLike(stream4);
      if (isStreamLike) {
        if (!(stream4 instanceof DelayedStream)) {
          var newStream = DelayedStream.create(stream4, {
            maxDataSize: Infinity,
            pauseStream: this.pauseStreams
          });
          stream4.on("data", this._checkDataSize.bind(this));
          stream4 = newStream;
        }
        this._handleErrors(stream4);
        if (this.pauseStreams) {
          stream4.pause();
        }
      }
      this._streams.push(stream4);
      return this;
    };
    CombinedStream.prototype.pipe = function(dest, options) {
      Stream.prototype.pipe.call(this, dest, options);
      this.resume();
      return dest;
    };
    CombinedStream.prototype._getNext = function() {
      this._currentStream = null;
      if (this._insideLoop) {
        this._pendingNext = true;
        return;
      }
      this._insideLoop = true;
      try {
        do {
          this._pendingNext = false;
          this._realGetNext();
        } while (this._pendingNext);
      } finally {
        this._insideLoop = false;
      }
    };
    CombinedStream.prototype._realGetNext = function() {
      var stream4 = this._streams.shift();
      if (typeof stream4 == "undefined") {
        this.end();
        return;
      }
      if (typeof stream4 !== "function") {
        this._pipeNext(stream4);
        return;
      }
      var getStream = stream4;
      getStream(function(stream5) {
        var isStreamLike = CombinedStream.isStreamLike(stream5);
        if (isStreamLike) {
          stream5.on("data", this._checkDataSize.bind(this));
          this._handleErrors(stream5);
        }
        this._pipeNext(stream5);
      }.bind(this));
    };
    CombinedStream.prototype._pipeNext = function(stream4) {
      this._currentStream = stream4;
      var isStreamLike = CombinedStream.isStreamLike(stream4);
      if (isStreamLike) {
        stream4.on("end", this._getNext.bind(this));
        stream4.pipe(this, { end: false });
        return;
      }
      var value = stream4;
      this.write(value);
      this._getNext();
    };
    CombinedStream.prototype._handleErrors = function(stream4) {
      var self2 = this;
      stream4.on("error", function(err) {
        self2._emitError(err);
      });
    };
    CombinedStream.prototype.write = function(data) {
      this.emit("data", data);
    };
    CombinedStream.prototype.pause = function() {
      if (!this.pauseStreams) {
        return;
      }
      if (this.pauseStreams && this._currentStream && typeof this._currentStream.pause == "function") this._currentStream.pause();
      this.emit("pause");
    };
    CombinedStream.prototype.resume = function() {
      if (!this._released) {
        this._released = true;
        this.writable = true;
        this._getNext();
      }
      if (this.pauseStreams && this._currentStream && typeof this._currentStream.resume == "function") this._currentStream.resume();
      this.emit("resume");
    };
    CombinedStream.prototype.end = function() {
      this._reset();
      this.emit("end");
    };
    CombinedStream.prototype.destroy = function() {
      this._reset();
      this.emit("close");
    };
    CombinedStream.prototype._reset = function() {
      this.writable = false;
      this._streams = [];
      this._currentStream = null;
    };
    CombinedStream.prototype._checkDataSize = function() {
      this._updateDataSize();
      if (this.dataSize <= this.maxDataSize) {
        return;
      }
      var message = "DelayedStream#maxDataSize of " + this.maxDataSize + " bytes exceeded.";
      this._emitError(new Error(message));
    };
    CombinedStream.prototype._updateDataSize = function() {
      this.dataSize = 0;
      var self2 = this;
      this._streams.forEach(function(stream4) {
        if (!stream4.dataSize) {
          return;
        }
        self2.dataSize += stream4.dataSize;
      });
      if (this._currentStream && this._currentStream.dataSize) {
        this.dataSize += this._currentStream.dataSize;
      }
    };
    CombinedStream.prototype._emitError = function(err) {
      this._reset();
      this.emit("error", err);
    };
  }
});

// ../../node_modules/.pnpm/mime-db@1.52.0/node_modules/mime-db/db.json
var require_db = __commonJS({
  "../../node_modules/.pnpm/mime-db@1.52.0/node_modules/mime-db/db.json"(exports2, module2) {
    module2.exports = {
      "application/1d-interleaved-parityfec": {
        source: "iana"
      },
      "application/3gpdash-qoe-report+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/3gpp-ims+xml": {
        source: "iana",
        compressible: true
      },
      "application/3gpphal+json": {
        source: "iana",
        compressible: true
      },
      "application/3gpphalforms+json": {
        source: "iana",
        compressible: true
      },
      "application/a2l": {
        source: "iana"
      },
      "application/ace+cbor": {
        source: "iana"
      },
      "application/activemessage": {
        source: "iana"
      },
      "application/activity+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-costmap+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-costmapfilter+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-directory+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-endpointcost+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-endpointcostparams+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-endpointprop+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-endpointpropparams+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-error+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-networkmap+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-networkmapfilter+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-updatestreamcontrol+json": {
        source: "iana",
        compressible: true
      },
      "application/alto-updatestreamparams+json": {
        source: "iana",
        compressible: true
      },
      "application/aml": {
        source: "iana"
      },
      "application/andrew-inset": {
        source: "iana",
        extensions: ["ez"]
      },
      "application/applefile": {
        source: "iana"
      },
      "application/applixware": {
        source: "apache",
        extensions: ["aw"]
      },
      "application/at+jwt": {
        source: "iana"
      },
      "application/atf": {
        source: "iana"
      },
      "application/atfx": {
        source: "iana"
      },
      "application/atom+xml": {
        source: "iana",
        compressible: true,
        extensions: ["atom"]
      },
      "application/atomcat+xml": {
        source: "iana",
        compressible: true,
        extensions: ["atomcat"]
      },
      "application/atomdeleted+xml": {
        source: "iana",
        compressible: true,
        extensions: ["atomdeleted"]
      },
      "application/atomicmail": {
        source: "iana"
      },
      "application/atomsvc+xml": {
        source: "iana",
        compressible: true,
        extensions: ["atomsvc"]
      },
      "application/atsc-dwd+xml": {
        source: "iana",
        compressible: true,
        extensions: ["dwd"]
      },
      "application/atsc-dynamic-event-message": {
        source: "iana"
      },
      "application/atsc-held+xml": {
        source: "iana",
        compressible: true,
        extensions: ["held"]
      },
      "application/atsc-rdt+json": {
        source: "iana",
        compressible: true
      },
      "application/atsc-rsat+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rsat"]
      },
      "application/atxml": {
        source: "iana"
      },
      "application/auth-policy+xml": {
        source: "iana",
        compressible: true
      },
      "application/bacnet-xdd+zip": {
        source: "iana",
        compressible: false
      },
      "application/batch-smtp": {
        source: "iana"
      },
      "application/bdoc": {
        compressible: false,
        extensions: ["bdoc"]
      },
      "application/beep+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/calendar+json": {
        source: "iana",
        compressible: true
      },
      "application/calendar+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xcs"]
      },
      "application/call-completion": {
        source: "iana"
      },
      "application/cals-1840": {
        source: "iana"
      },
      "application/captive+json": {
        source: "iana",
        compressible: true
      },
      "application/cbor": {
        source: "iana"
      },
      "application/cbor-seq": {
        source: "iana"
      },
      "application/cccex": {
        source: "iana"
      },
      "application/ccmp+xml": {
        source: "iana",
        compressible: true
      },
      "application/ccxml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["ccxml"]
      },
      "application/cdfx+xml": {
        source: "iana",
        compressible: true,
        extensions: ["cdfx"]
      },
      "application/cdmi-capability": {
        source: "iana",
        extensions: ["cdmia"]
      },
      "application/cdmi-container": {
        source: "iana",
        extensions: ["cdmic"]
      },
      "application/cdmi-domain": {
        source: "iana",
        extensions: ["cdmid"]
      },
      "application/cdmi-object": {
        source: "iana",
        extensions: ["cdmio"]
      },
      "application/cdmi-queue": {
        source: "iana",
        extensions: ["cdmiq"]
      },
      "application/cdni": {
        source: "iana"
      },
      "application/cea": {
        source: "iana"
      },
      "application/cea-2018+xml": {
        source: "iana",
        compressible: true
      },
      "application/cellml+xml": {
        source: "iana",
        compressible: true
      },
      "application/cfw": {
        source: "iana"
      },
      "application/city+json": {
        source: "iana",
        compressible: true
      },
      "application/clr": {
        source: "iana"
      },
      "application/clue+xml": {
        source: "iana",
        compressible: true
      },
      "application/clue_info+xml": {
        source: "iana",
        compressible: true
      },
      "application/cms": {
        source: "iana"
      },
      "application/cnrp+xml": {
        source: "iana",
        compressible: true
      },
      "application/coap-group+json": {
        source: "iana",
        compressible: true
      },
      "application/coap-payload": {
        source: "iana"
      },
      "application/commonground": {
        source: "iana"
      },
      "application/conference-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/cose": {
        source: "iana"
      },
      "application/cose-key": {
        source: "iana"
      },
      "application/cose-key-set": {
        source: "iana"
      },
      "application/cpl+xml": {
        source: "iana",
        compressible: true,
        extensions: ["cpl"]
      },
      "application/csrattrs": {
        source: "iana"
      },
      "application/csta+xml": {
        source: "iana",
        compressible: true
      },
      "application/cstadata+xml": {
        source: "iana",
        compressible: true
      },
      "application/csvm+json": {
        source: "iana",
        compressible: true
      },
      "application/cu-seeme": {
        source: "apache",
        extensions: ["cu"]
      },
      "application/cwt": {
        source: "iana"
      },
      "application/cybercash": {
        source: "iana"
      },
      "application/dart": {
        compressible: true
      },
      "application/dash+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mpd"]
      },
      "application/dash-patch+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mpp"]
      },
      "application/dashdelta": {
        source: "iana"
      },
      "application/davmount+xml": {
        source: "iana",
        compressible: true,
        extensions: ["davmount"]
      },
      "application/dca-rft": {
        source: "iana"
      },
      "application/dcd": {
        source: "iana"
      },
      "application/dec-dx": {
        source: "iana"
      },
      "application/dialog-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/dicom": {
        source: "iana"
      },
      "application/dicom+json": {
        source: "iana",
        compressible: true
      },
      "application/dicom+xml": {
        source: "iana",
        compressible: true
      },
      "application/dii": {
        source: "iana"
      },
      "application/dit": {
        source: "iana"
      },
      "application/dns": {
        source: "iana"
      },
      "application/dns+json": {
        source: "iana",
        compressible: true
      },
      "application/dns-message": {
        source: "iana"
      },
      "application/docbook+xml": {
        source: "apache",
        compressible: true,
        extensions: ["dbk"]
      },
      "application/dots+cbor": {
        source: "iana"
      },
      "application/dskpp+xml": {
        source: "iana",
        compressible: true
      },
      "application/dssc+der": {
        source: "iana",
        extensions: ["dssc"]
      },
      "application/dssc+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xdssc"]
      },
      "application/dvcs": {
        source: "iana"
      },
      "application/ecmascript": {
        source: "iana",
        compressible: true,
        extensions: ["es", "ecma"]
      },
      "application/edi-consent": {
        source: "iana"
      },
      "application/edi-x12": {
        source: "iana",
        compressible: false
      },
      "application/edifact": {
        source: "iana",
        compressible: false
      },
      "application/efi": {
        source: "iana"
      },
      "application/elm+json": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/elm+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.cap+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/emergencycalldata.comment+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.control+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.deviceinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.ecall.msd": {
        source: "iana"
      },
      "application/emergencycalldata.providerinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.serviceinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.subscriberinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/emergencycalldata.veds+xml": {
        source: "iana",
        compressible: true
      },
      "application/emma+xml": {
        source: "iana",
        compressible: true,
        extensions: ["emma"]
      },
      "application/emotionml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["emotionml"]
      },
      "application/encaprtp": {
        source: "iana"
      },
      "application/epp+xml": {
        source: "iana",
        compressible: true
      },
      "application/epub+zip": {
        source: "iana",
        compressible: false,
        extensions: ["epub"]
      },
      "application/eshop": {
        source: "iana"
      },
      "application/exi": {
        source: "iana",
        extensions: ["exi"]
      },
      "application/expect-ct-report+json": {
        source: "iana",
        compressible: true
      },
      "application/express": {
        source: "iana",
        extensions: ["exp"]
      },
      "application/fastinfoset": {
        source: "iana"
      },
      "application/fastsoap": {
        source: "iana"
      },
      "application/fdt+xml": {
        source: "iana",
        compressible: true,
        extensions: ["fdt"]
      },
      "application/fhir+json": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/fhir+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/fido.trusted-apps+json": {
        compressible: true
      },
      "application/fits": {
        source: "iana"
      },
      "application/flexfec": {
        source: "iana"
      },
      "application/font-sfnt": {
        source: "iana"
      },
      "application/font-tdpfr": {
        source: "iana",
        extensions: ["pfr"]
      },
      "application/font-woff": {
        source: "iana",
        compressible: false
      },
      "application/framework-attributes+xml": {
        source: "iana",
        compressible: true
      },
      "application/geo+json": {
        source: "iana",
        compressible: true,
        extensions: ["geojson"]
      },
      "application/geo+json-seq": {
        source: "iana"
      },
      "application/geopackage+sqlite3": {
        source: "iana"
      },
      "application/geoxacml+xml": {
        source: "iana",
        compressible: true
      },
      "application/gltf-buffer": {
        source: "iana"
      },
      "application/gml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["gml"]
      },
      "application/gpx+xml": {
        source: "apache",
        compressible: true,
        extensions: ["gpx"]
      },
      "application/gxf": {
        source: "apache",
        extensions: ["gxf"]
      },
      "application/gzip": {
        source: "iana",
        compressible: false,
        extensions: ["gz"]
      },
      "application/h224": {
        source: "iana"
      },
      "application/held+xml": {
        source: "iana",
        compressible: true
      },
      "application/hjson": {
        extensions: ["hjson"]
      },
      "application/http": {
        source: "iana"
      },
      "application/hyperstudio": {
        source: "iana",
        extensions: ["stk"]
      },
      "application/ibe-key-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/ibe-pkg-reply+xml": {
        source: "iana",
        compressible: true
      },
      "application/ibe-pp-data": {
        source: "iana"
      },
      "application/iges": {
        source: "iana"
      },
      "application/im-iscomposing+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/index": {
        source: "iana"
      },
      "application/index.cmd": {
        source: "iana"
      },
      "application/index.obj": {
        source: "iana"
      },
      "application/index.response": {
        source: "iana"
      },
      "application/index.vnd": {
        source: "iana"
      },
      "application/inkml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["ink", "inkml"]
      },
      "application/iotp": {
        source: "iana"
      },
      "application/ipfix": {
        source: "iana",
        extensions: ["ipfix"]
      },
      "application/ipp": {
        source: "iana"
      },
      "application/isup": {
        source: "iana"
      },
      "application/its+xml": {
        source: "iana",
        compressible: true,
        extensions: ["its"]
      },
      "application/java-archive": {
        source: "apache",
        compressible: false,
        extensions: ["jar", "war", "ear"]
      },
      "application/java-serialized-object": {
        source: "apache",
        compressible: false,
        extensions: ["ser"]
      },
      "application/java-vm": {
        source: "apache",
        compressible: false,
        extensions: ["class"]
      },
      "application/javascript": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["js", "mjs"]
      },
      "application/jf2feed+json": {
        source: "iana",
        compressible: true
      },
      "application/jose": {
        source: "iana"
      },
      "application/jose+json": {
        source: "iana",
        compressible: true
      },
      "application/jrd+json": {
        source: "iana",
        compressible: true
      },
      "application/jscalendar+json": {
        source: "iana",
        compressible: true
      },
      "application/json": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["json", "map"]
      },
      "application/json-patch+json": {
        source: "iana",
        compressible: true
      },
      "application/json-seq": {
        source: "iana"
      },
      "application/json5": {
        extensions: ["json5"]
      },
      "application/jsonml+json": {
        source: "apache",
        compressible: true,
        extensions: ["jsonml"]
      },
      "application/jwk+json": {
        source: "iana",
        compressible: true
      },
      "application/jwk-set+json": {
        source: "iana",
        compressible: true
      },
      "application/jwt": {
        source: "iana"
      },
      "application/kpml-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/kpml-response+xml": {
        source: "iana",
        compressible: true
      },
      "application/ld+json": {
        source: "iana",
        compressible: true,
        extensions: ["jsonld"]
      },
      "application/lgr+xml": {
        source: "iana",
        compressible: true,
        extensions: ["lgr"]
      },
      "application/link-format": {
        source: "iana"
      },
      "application/load-control+xml": {
        source: "iana",
        compressible: true
      },
      "application/lost+xml": {
        source: "iana",
        compressible: true,
        extensions: ["lostxml"]
      },
      "application/lostsync+xml": {
        source: "iana",
        compressible: true
      },
      "application/lpf+zip": {
        source: "iana",
        compressible: false
      },
      "application/lxf": {
        source: "iana"
      },
      "application/mac-binhex40": {
        source: "iana",
        extensions: ["hqx"]
      },
      "application/mac-compactpro": {
        source: "apache",
        extensions: ["cpt"]
      },
      "application/macwriteii": {
        source: "iana"
      },
      "application/mads+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mads"]
      },
      "application/manifest+json": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["webmanifest"]
      },
      "application/marc": {
        source: "iana",
        extensions: ["mrc"]
      },
      "application/marcxml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mrcx"]
      },
      "application/mathematica": {
        source: "iana",
        extensions: ["ma", "nb", "mb"]
      },
      "application/mathml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mathml"]
      },
      "application/mathml-content+xml": {
        source: "iana",
        compressible: true
      },
      "application/mathml-presentation+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-associated-procedure-description+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-deregister+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-envelope+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-msk+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-msk-response+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-protection-description+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-reception-report+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-register+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-register-response+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-schedule+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbms-user-service-description+xml": {
        source: "iana",
        compressible: true
      },
      "application/mbox": {
        source: "iana",
        extensions: ["mbox"]
      },
      "application/media-policy-dataset+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mpf"]
      },
      "application/media_control+xml": {
        source: "iana",
        compressible: true
      },
      "application/mediaservercontrol+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mscml"]
      },
      "application/merge-patch+json": {
        source: "iana",
        compressible: true
      },
      "application/metalink+xml": {
        source: "apache",
        compressible: true,
        extensions: ["metalink"]
      },
      "application/metalink4+xml": {
        source: "iana",
        compressible: true,
        extensions: ["meta4"]
      },
      "application/mets+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mets"]
      },
      "application/mf4": {
        source: "iana"
      },
      "application/mikey": {
        source: "iana"
      },
      "application/mipc": {
        source: "iana"
      },
      "application/missing-blocks+cbor-seq": {
        source: "iana"
      },
      "application/mmt-aei+xml": {
        source: "iana",
        compressible: true,
        extensions: ["maei"]
      },
      "application/mmt-usd+xml": {
        source: "iana",
        compressible: true,
        extensions: ["musd"]
      },
      "application/mods+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mods"]
      },
      "application/moss-keys": {
        source: "iana"
      },
      "application/moss-signature": {
        source: "iana"
      },
      "application/mosskey-data": {
        source: "iana"
      },
      "application/mosskey-request": {
        source: "iana"
      },
      "application/mp21": {
        source: "iana",
        extensions: ["m21", "mp21"]
      },
      "application/mp4": {
        source: "iana",
        extensions: ["mp4s", "m4p"]
      },
      "application/mpeg4-generic": {
        source: "iana"
      },
      "application/mpeg4-iod": {
        source: "iana"
      },
      "application/mpeg4-iod-xmt": {
        source: "iana"
      },
      "application/mrb-consumer+xml": {
        source: "iana",
        compressible: true
      },
      "application/mrb-publish+xml": {
        source: "iana",
        compressible: true
      },
      "application/msc-ivr+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/msc-mixer+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/msword": {
        source: "iana",
        compressible: false,
        extensions: ["doc", "dot"]
      },
      "application/mud+json": {
        source: "iana",
        compressible: true
      },
      "application/multipart-core": {
        source: "iana"
      },
      "application/mxf": {
        source: "iana",
        extensions: ["mxf"]
      },
      "application/n-quads": {
        source: "iana",
        extensions: ["nq"]
      },
      "application/n-triples": {
        source: "iana",
        extensions: ["nt"]
      },
      "application/nasdata": {
        source: "iana"
      },
      "application/news-checkgroups": {
        source: "iana",
        charset: "US-ASCII"
      },
      "application/news-groupinfo": {
        source: "iana",
        charset: "US-ASCII"
      },
      "application/news-transmission": {
        source: "iana"
      },
      "application/nlsml+xml": {
        source: "iana",
        compressible: true
      },
      "application/node": {
        source: "iana",
        extensions: ["cjs"]
      },
      "application/nss": {
        source: "iana"
      },
      "application/oauth-authz-req+jwt": {
        source: "iana"
      },
      "application/oblivious-dns-message": {
        source: "iana"
      },
      "application/ocsp-request": {
        source: "iana"
      },
      "application/ocsp-response": {
        source: "iana"
      },
      "application/octet-stream": {
        source: "iana",
        compressible: false,
        extensions: ["bin", "dms", "lrf", "mar", "so", "dist", "distz", "pkg", "bpk", "dump", "elc", "deploy", "exe", "dll", "deb", "dmg", "iso", "img", "msi", "msp", "msm", "buffer"]
      },
      "application/oda": {
        source: "iana",
        extensions: ["oda"]
      },
      "application/odm+xml": {
        source: "iana",
        compressible: true
      },
      "application/odx": {
        source: "iana"
      },
      "application/oebps-package+xml": {
        source: "iana",
        compressible: true,
        extensions: ["opf"]
      },
      "application/ogg": {
        source: "iana",
        compressible: false,
        extensions: ["ogx"]
      },
      "application/omdoc+xml": {
        source: "apache",
        compressible: true,
        extensions: ["omdoc"]
      },
      "application/onenote": {
        source: "apache",
        extensions: ["onetoc", "onetoc2", "onetmp", "onepkg"]
      },
      "application/opc-nodeset+xml": {
        source: "iana",
        compressible: true
      },
      "application/oscore": {
        source: "iana"
      },
      "application/oxps": {
        source: "iana",
        extensions: ["oxps"]
      },
      "application/p21": {
        source: "iana"
      },
      "application/p21+zip": {
        source: "iana",
        compressible: false
      },
      "application/p2p-overlay+xml": {
        source: "iana",
        compressible: true,
        extensions: ["relo"]
      },
      "application/parityfec": {
        source: "iana"
      },
      "application/passport": {
        source: "iana"
      },
      "application/patch-ops-error+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xer"]
      },
      "application/pdf": {
        source: "iana",
        compressible: false,
        extensions: ["pdf"]
      },
      "application/pdx": {
        source: "iana"
      },
      "application/pem-certificate-chain": {
        source: "iana"
      },
      "application/pgp-encrypted": {
        source: "iana",
        compressible: false,
        extensions: ["pgp"]
      },
      "application/pgp-keys": {
        source: "iana",
        extensions: ["asc"]
      },
      "application/pgp-signature": {
        source: "iana",
        extensions: ["asc", "sig"]
      },
      "application/pics-rules": {
        source: "apache",
        extensions: ["prf"]
      },
      "application/pidf+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/pidf-diff+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/pkcs10": {
        source: "iana",
        extensions: ["p10"]
      },
      "application/pkcs12": {
        source: "iana"
      },
      "application/pkcs7-mime": {
        source: "iana",
        extensions: ["p7m", "p7c"]
      },
      "application/pkcs7-signature": {
        source: "iana",
        extensions: ["p7s"]
      },
      "application/pkcs8": {
        source: "iana",
        extensions: ["p8"]
      },
      "application/pkcs8-encrypted": {
        source: "iana"
      },
      "application/pkix-attr-cert": {
        source: "iana",
        extensions: ["ac"]
      },
      "application/pkix-cert": {
        source: "iana",
        extensions: ["cer"]
      },
      "application/pkix-crl": {
        source: "iana",
        extensions: ["crl"]
      },
      "application/pkix-pkipath": {
        source: "iana",
        extensions: ["pkipath"]
      },
      "application/pkixcmp": {
        source: "iana",
        extensions: ["pki"]
      },
      "application/pls+xml": {
        source: "iana",
        compressible: true,
        extensions: ["pls"]
      },
      "application/poc-settings+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/postscript": {
        source: "iana",
        compressible: true,
        extensions: ["ai", "eps", "ps"]
      },
      "application/ppsp-tracker+json": {
        source: "iana",
        compressible: true
      },
      "application/problem+json": {
        source: "iana",
        compressible: true
      },
      "application/problem+xml": {
        source: "iana",
        compressible: true
      },
      "application/provenance+xml": {
        source: "iana",
        compressible: true,
        extensions: ["provx"]
      },
      "application/prs.alvestrand.titrax-sheet": {
        source: "iana"
      },
      "application/prs.cww": {
        source: "iana",
        extensions: ["cww"]
      },
      "application/prs.cyn": {
        source: "iana",
        charset: "7-BIT"
      },
      "application/prs.hpub+zip": {
        source: "iana",
        compressible: false
      },
      "application/prs.nprend": {
        source: "iana"
      },
      "application/prs.plucker": {
        source: "iana"
      },
      "application/prs.rdf-xml-crypt": {
        source: "iana"
      },
      "application/prs.xsf+xml": {
        source: "iana",
        compressible: true
      },
      "application/pskc+xml": {
        source: "iana",
        compressible: true,
        extensions: ["pskcxml"]
      },
      "application/pvd+json": {
        source: "iana",
        compressible: true
      },
      "application/qsig": {
        source: "iana"
      },
      "application/raml+yaml": {
        compressible: true,
        extensions: ["raml"]
      },
      "application/raptorfec": {
        source: "iana"
      },
      "application/rdap+json": {
        source: "iana",
        compressible: true
      },
      "application/rdf+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rdf", "owl"]
      },
      "application/reginfo+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rif"]
      },
      "application/relax-ng-compact-syntax": {
        source: "iana",
        extensions: ["rnc"]
      },
      "application/remote-printing": {
        source: "iana"
      },
      "application/reputon+json": {
        source: "iana",
        compressible: true
      },
      "application/resource-lists+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rl"]
      },
      "application/resource-lists-diff+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rld"]
      },
      "application/rfc+xml": {
        source: "iana",
        compressible: true
      },
      "application/riscos": {
        source: "iana"
      },
      "application/rlmi+xml": {
        source: "iana",
        compressible: true
      },
      "application/rls-services+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rs"]
      },
      "application/route-apd+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rapd"]
      },
      "application/route-s-tsid+xml": {
        source: "iana",
        compressible: true,
        extensions: ["sls"]
      },
      "application/route-usd+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rusd"]
      },
      "application/rpki-ghostbusters": {
        source: "iana",
        extensions: ["gbr"]
      },
      "application/rpki-manifest": {
        source: "iana",
        extensions: ["mft"]
      },
      "application/rpki-publication": {
        source: "iana"
      },
      "application/rpki-roa": {
        source: "iana",
        extensions: ["roa"]
      },
      "application/rpki-updown": {
        source: "iana"
      },
      "application/rsd+xml": {
        source: "apache",
        compressible: true,
        extensions: ["rsd"]
      },
      "application/rss+xml": {
        source: "apache",
        compressible: true,
        extensions: ["rss"]
      },
      "application/rtf": {
        source: "iana",
        compressible: true,
        extensions: ["rtf"]
      },
      "application/rtploopback": {
        source: "iana"
      },
      "application/rtx": {
        source: "iana"
      },
      "application/samlassertion+xml": {
        source: "iana",
        compressible: true
      },
      "application/samlmetadata+xml": {
        source: "iana",
        compressible: true
      },
      "application/sarif+json": {
        source: "iana",
        compressible: true
      },
      "application/sarif-external-properties+json": {
        source: "iana",
        compressible: true
      },
      "application/sbe": {
        source: "iana"
      },
      "application/sbml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["sbml"]
      },
      "application/scaip+xml": {
        source: "iana",
        compressible: true
      },
      "application/scim+json": {
        source: "iana",
        compressible: true
      },
      "application/scvp-cv-request": {
        source: "iana",
        extensions: ["scq"]
      },
      "application/scvp-cv-response": {
        source: "iana",
        extensions: ["scs"]
      },
      "application/scvp-vp-request": {
        source: "iana",
        extensions: ["spq"]
      },
      "application/scvp-vp-response": {
        source: "iana",
        extensions: ["spp"]
      },
      "application/sdp": {
        source: "iana",
        extensions: ["sdp"]
      },
      "application/secevent+jwt": {
        source: "iana"
      },
      "application/senml+cbor": {
        source: "iana"
      },
      "application/senml+json": {
        source: "iana",
        compressible: true
      },
      "application/senml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["senmlx"]
      },
      "application/senml-etch+cbor": {
        source: "iana"
      },
      "application/senml-etch+json": {
        source: "iana",
        compressible: true
      },
      "application/senml-exi": {
        source: "iana"
      },
      "application/sensml+cbor": {
        source: "iana"
      },
      "application/sensml+json": {
        source: "iana",
        compressible: true
      },
      "application/sensml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["sensmlx"]
      },
      "application/sensml-exi": {
        source: "iana"
      },
      "application/sep+xml": {
        source: "iana",
        compressible: true
      },
      "application/sep-exi": {
        source: "iana"
      },
      "application/session-info": {
        source: "iana"
      },
      "application/set-payment": {
        source: "iana"
      },
      "application/set-payment-initiation": {
        source: "iana",
        extensions: ["setpay"]
      },
      "application/set-registration": {
        source: "iana"
      },
      "application/set-registration-initiation": {
        source: "iana",
        extensions: ["setreg"]
      },
      "application/sgml": {
        source: "iana"
      },
      "application/sgml-open-catalog": {
        source: "iana"
      },
      "application/shf+xml": {
        source: "iana",
        compressible: true,
        extensions: ["shf"]
      },
      "application/sieve": {
        source: "iana",
        extensions: ["siv", "sieve"]
      },
      "application/simple-filter+xml": {
        source: "iana",
        compressible: true
      },
      "application/simple-message-summary": {
        source: "iana"
      },
      "application/simplesymbolcontainer": {
        source: "iana"
      },
      "application/sipc": {
        source: "iana"
      },
      "application/slate": {
        source: "iana"
      },
      "application/smil": {
        source: "iana"
      },
      "application/smil+xml": {
        source: "iana",
        compressible: true,
        extensions: ["smi", "smil"]
      },
      "application/smpte336m": {
        source: "iana"
      },
      "application/soap+fastinfoset": {
        source: "iana"
      },
      "application/soap+xml": {
        source: "iana",
        compressible: true
      },
      "application/sparql-query": {
        source: "iana",
        extensions: ["rq"]
      },
      "application/sparql-results+xml": {
        source: "iana",
        compressible: true,
        extensions: ["srx"]
      },
      "application/spdx+json": {
        source: "iana",
        compressible: true
      },
      "application/spirits-event+xml": {
        source: "iana",
        compressible: true
      },
      "application/sql": {
        source: "iana"
      },
      "application/srgs": {
        source: "iana",
        extensions: ["gram"]
      },
      "application/srgs+xml": {
        source: "iana",
        compressible: true,
        extensions: ["grxml"]
      },
      "application/sru+xml": {
        source: "iana",
        compressible: true,
        extensions: ["sru"]
      },
      "application/ssdl+xml": {
        source: "apache",
        compressible: true,
        extensions: ["ssdl"]
      },
      "application/ssml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["ssml"]
      },
      "application/stix+json": {
        source: "iana",
        compressible: true
      },
      "application/swid+xml": {
        source: "iana",
        compressible: true,
        extensions: ["swidtag"]
      },
      "application/tamp-apex-update": {
        source: "iana"
      },
      "application/tamp-apex-update-confirm": {
        source: "iana"
      },
      "application/tamp-community-update": {
        source: "iana"
      },
      "application/tamp-community-update-confirm": {
        source: "iana"
      },
      "application/tamp-error": {
        source: "iana"
      },
      "application/tamp-sequence-adjust": {
        source: "iana"
      },
      "application/tamp-sequence-adjust-confirm": {
        source: "iana"
      },
      "application/tamp-status-query": {
        source: "iana"
      },
      "application/tamp-status-response": {
        source: "iana"
      },
      "application/tamp-update": {
        source: "iana"
      },
      "application/tamp-update-confirm": {
        source: "iana"
      },
      "application/tar": {
        compressible: true
      },
      "application/taxii+json": {
        source: "iana",
        compressible: true
      },
      "application/td+json": {
        source: "iana",
        compressible: true
      },
      "application/tei+xml": {
        source: "iana",
        compressible: true,
        extensions: ["tei", "teicorpus"]
      },
      "application/tetra_isi": {
        source: "iana"
      },
      "application/thraud+xml": {
        source: "iana",
        compressible: true,
        extensions: ["tfi"]
      },
      "application/timestamp-query": {
        source: "iana"
      },
      "application/timestamp-reply": {
        source: "iana"
      },
      "application/timestamped-data": {
        source: "iana",
        extensions: ["tsd"]
      },
      "application/tlsrpt+gzip": {
        source: "iana"
      },
      "application/tlsrpt+json": {
        source: "iana",
        compressible: true
      },
      "application/tnauthlist": {
        source: "iana"
      },
      "application/token-introspection+jwt": {
        source: "iana"
      },
      "application/toml": {
        compressible: true,
        extensions: ["toml"]
      },
      "application/trickle-ice-sdpfrag": {
        source: "iana"
      },
      "application/trig": {
        source: "iana",
        extensions: ["trig"]
      },
      "application/ttml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["ttml"]
      },
      "application/tve-trigger": {
        source: "iana"
      },
      "application/tzif": {
        source: "iana"
      },
      "application/tzif-leap": {
        source: "iana"
      },
      "application/ubjson": {
        compressible: false,
        extensions: ["ubj"]
      },
      "application/ulpfec": {
        source: "iana"
      },
      "application/urc-grpsheet+xml": {
        source: "iana",
        compressible: true
      },
      "application/urc-ressheet+xml": {
        source: "iana",
        compressible: true,
        extensions: ["rsheet"]
      },
      "application/urc-targetdesc+xml": {
        source: "iana",
        compressible: true,
        extensions: ["td"]
      },
      "application/urc-uisocketdesc+xml": {
        source: "iana",
        compressible: true
      },
      "application/vcard+json": {
        source: "iana",
        compressible: true
      },
      "application/vcard+xml": {
        source: "iana",
        compressible: true
      },
      "application/vemmi": {
        source: "iana"
      },
      "application/vividence.scriptfile": {
        source: "apache"
      },
      "application/vnd.1000minds.decision-model+xml": {
        source: "iana",
        compressible: true,
        extensions: ["1km"]
      },
      "application/vnd.3gpp-prose+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp-prose-pc3ch+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp-v2x-local-service-information": {
        source: "iana"
      },
      "application/vnd.3gpp.5gnas": {
        source: "iana"
      },
      "application/vnd.3gpp.access-transfer-events+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.bsf+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.gmop+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.gtpc": {
        source: "iana"
      },
      "application/vnd.3gpp.interworking-data": {
        source: "iana"
      },
      "application/vnd.3gpp.lpp": {
        source: "iana"
      },
      "application/vnd.3gpp.mc-signalling-ear": {
        source: "iana"
      },
      "application/vnd.3gpp.mcdata-affiliation-command+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcdata-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcdata-payload": {
        source: "iana"
      },
      "application/vnd.3gpp.mcdata-service-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcdata-signalling": {
        source: "iana"
      },
      "application/vnd.3gpp.mcdata-ue-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcdata-user-profile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-affiliation-command+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-floor-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-location-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-mbms-usage-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-service-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-signed+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-ue-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-ue-init-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcptt-user-profile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-affiliation-command+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-affiliation-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-location-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-mbms-usage-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-service-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-transmission-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-ue-config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mcvideo-user-profile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.mid-call+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.ngap": {
        source: "iana"
      },
      "application/vnd.3gpp.pfcp": {
        source: "iana"
      },
      "application/vnd.3gpp.pic-bw-large": {
        source: "iana",
        extensions: ["plb"]
      },
      "application/vnd.3gpp.pic-bw-small": {
        source: "iana",
        extensions: ["psb"]
      },
      "application/vnd.3gpp.pic-bw-var": {
        source: "iana",
        extensions: ["pvb"]
      },
      "application/vnd.3gpp.s1ap": {
        source: "iana"
      },
      "application/vnd.3gpp.sms": {
        source: "iana"
      },
      "application/vnd.3gpp.sms+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.srvcc-ext+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.srvcc-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.state-and-event-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp.ussd+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp2.bcmcsinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.3gpp2.sms": {
        source: "iana"
      },
      "application/vnd.3gpp2.tcap": {
        source: "iana",
        extensions: ["tcap"]
      },
      "application/vnd.3lightssoftware.imagescal": {
        source: "iana"
      },
      "application/vnd.3m.post-it-notes": {
        source: "iana",
        extensions: ["pwn"]
      },
      "application/vnd.accpac.simply.aso": {
        source: "iana",
        extensions: ["aso"]
      },
      "application/vnd.accpac.simply.imp": {
        source: "iana",
        extensions: ["imp"]
      },
      "application/vnd.acucobol": {
        source: "iana",
        extensions: ["acu"]
      },
      "application/vnd.acucorp": {
        source: "iana",
        extensions: ["atc", "acutc"]
      },
      "application/vnd.adobe.air-application-installer-package+zip": {
        source: "apache",
        compressible: false,
        extensions: ["air"]
      },
      "application/vnd.adobe.flash.movie": {
        source: "iana"
      },
      "application/vnd.adobe.formscentral.fcdt": {
        source: "iana",
        extensions: ["fcdt"]
      },
      "application/vnd.adobe.fxp": {
        source: "iana",
        extensions: ["fxp", "fxpl"]
      },
      "application/vnd.adobe.partial-upload": {
        source: "iana"
      },
      "application/vnd.adobe.xdp+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xdp"]
      },
      "application/vnd.adobe.xfdf": {
        source: "iana",
        extensions: ["xfdf"]
      },
      "application/vnd.aether.imp": {
        source: "iana"
      },
      "application/vnd.afpc.afplinedata": {
        source: "iana"
      },
      "application/vnd.afpc.afplinedata-pagedef": {
        source: "iana"
      },
      "application/vnd.afpc.cmoca-cmresource": {
        source: "iana"
      },
      "application/vnd.afpc.foca-charset": {
        source: "iana"
      },
      "application/vnd.afpc.foca-codedfont": {
        source: "iana"
      },
      "application/vnd.afpc.foca-codepage": {
        source: "iana"
      },
      "application/vnd.afpc.modca": {
        source: "iana"
      },
      "application/vnd.afpc.modca-cmtable": {
        source: "iana"
      },
      "application/vnd.afpc.modca-formdef": {
        source: "iana"
      },
      "application/vnd.afpc.modca-mediummap": {
        source: "iana"
      },
      "application/vnd.afpc.modca-objectcontainer": {
        source: "iana"
      },
      "application/vnd.afpc.modca-overlay": {
        source: "iana"
      },
      "application/vnd.afpc.modca-pagesegment": {
        source: "iana"
      },
      "application/vnd.age": {
        source: "iana",
        extensions: ["age"]
      },
      "application/vnd.ah-barcode": {
        source: "iana"
      },
      "application/vnd.ahead.space": {
        source: "iana",
        extensions: ["ahead"]
      },
      "application/vnd.airzip.filesecure.azf": {
        source: "iana",
        extensions: ["azf"]
      },
      "application/vnd.airzip.filesecure.azs": {
        source: "iana",
        extensions: ["azs"]
      },
      "application/vnd.amadeus+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.amazon.ebook": {
        source: "apache",
        extensions: ["azw"]
      },
      "application/vnd.amazon.mobi8-ebook": {
        source: "iana"
      },
      "application/vnd.americandynamics.acc": {
        source: "iana",
        extensions: ["acc"]
      },
      "application/vnd.amiga.ami": {
        source: "iana",
        extensions: ["ami"]
      },
      "application/vnd.amundsen.maze+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.android.ota": {
        source: "iana"
      },
      "application/vnd.android.package-archive": {
        source: "apache",
        compressible: false,
        extensions: ["apk"]
      },
      "application/vnd.anki": {
        source: "iana"
      },
      "application/vnd.anser-web-certificate-issue-initiation": {
        source: "iana",
        extensions: ["cii"]
      },
      "application/vnd.anser-web-funds-transfer-initiation": {
        source: "apache",
        extensions: ["fti"]
      },
      "application/vnd.antix.game-component": {
        source: "iana",
        extensions: ["atx"]
      },
      "application/vnd.apache.arrow.file": {
        source: "iana"
      },
      "application/vnd.apache.arrow.stream": {
        source: "iana"
      },
      "application/vnd.apache.thrift.binary": {
        source: "iana"
      },
      "application/vnd.apache.thrift.compact": {
        source: "iana"
      },
      "application/vnd.apache.thrift.json": {
        source: "iana"
      },
      "application/vnd.api+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.aplextor.warrp+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.apothekende.reservation+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.apple.installer+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mpkg"]
      },
      "application/vnd.apple.keynote": {
        source: "iana",
        extensions: ["key"]
      },
      "application/vnd.apple.mpegurl": {
        source: "iana",
        extensions: ["m3u8"]
      },
      "application/vnd.apple.numbers": {
        source: "iana",
        extensions: ["numbers"]
      },
      "application/vnd.apple.pages": {
        source: "iana",
        extensions: ["pages"]
      },
      "application/vnd.apple.pkpass": {
        compressible: false,
        extensions: ["pkpass"]
      },
      "application/vnd.arastra.swi": {
        source: "iana"
      },
      "application/vnd.aristanetworks.swi": {
        source: "iana",
        extensions: ["swi"]
      },
      "application/vnd.artisan+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.artsquare": {
        source: "iana"
      },
      "application/vnd.astraea-software.iota": {
        source: "iana",
        extensions: ["iota"]
      },
      "application/vnd.audiograph": {
        source: "iana",
        extensions: ["aep"]
      },
      "application/vnd.autopackage": {
        source: "iana"
      },
      "application/vnd.avalon+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.avistar+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.balsamiq.bmml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["bmml"]
      },
      "application/vnd.balsamiq.bmpr": {
        source: "iana"
      },
      "application/vnd.banana-accounting": {
        source: "iana"
      },
      "application/vnd.bbf.usp.error": {
        source: "iana"
      },
      "application/vnd.bbf.usp.msg": {
        source: "iana"
      },
      "application/vnd.bbf.usp.msg+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.bekitzur-stech+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.bint.med-content": {
        source: "iana"
      },
      "application/vnd.biopax.rdf+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.blink-idb-value-wrapper": {
        source: "iana"
      },
      "application/vnd.blueice.multipass": {
        source: "iana",
        extensions: ["mpm"]
      },
      "application/vnd.bluetooth.ep.oob": {
        source: "iana"
      },
      "application/vnd.bluetooth.le.oob": {
        source: "iana"
      },
      "application/vnd.bmi": {
        source: "iana",
        extensions: ["bmi"]
      },
      "application/vnd.bpf": {
        source: "iana"
      },
      "application/vnd.bpf3": {
        source: "iana"
      },
      "application/vnd.businessobjects": {
        source: "iana",
        extensions: ["rep"]
      },
      "application/vnd.byu.uapi+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cab-jscript": {
        source: "iana"
      },
      "application/vnd.canon-cpdl": {
        source: "iana"
      },
      "application/vnd.canon-lips": {
        source: "iana"
      },
      "application/vnd.capasystems-pg+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cendio.thinlinc.clientconf": {
        source: "iana"
      },
      "application/vnd.century-systems.tcp_stream": {
        source: "iana"
      },
      "application/vnd.chemdraw+xml": {
        source: "iana",
        compressible: true,
        extensions: ["cdxml"]
      },
      "application/vnd.chess-pgn": {
        source: "iana"
      },
      "application/vnd.chipnuts.karaoke-mmd": {
        source: "iana",
        extensions: ["mmd"]
      },
      "application/vnd.ciedi": {
        source: "iana"
      },
      "application/vnd.cinderella": {
        source: "iana",
        extensions: ["cdy"]
      },
      "application/vnd.cirpack.isdn-ext": {
        source: "iana"
      },
      "application/vnd.citationstyles.style+xml": {
        source: "iana",
        compressible: true,
        extensions: ["csl"]
      },
      "application/vnd.claymore": {
        source: "iana",
        extensions: ["cla"]
      },
      "application/vnd.cloanto.rp9": {
        source: "iana",
        extensions: ["rp9"]
      },
      "application/vnd.clonk.c4group": {
        source: "iana",
        extensions: ["c4g", "c4d", "c4f", "c4p", "c4u"]
      },
      "application/vnd.cluetrust.cartomobile-config": {
        source: "iana",
        extensions: ["c11amc"]
      },
      "application/vnd.cluetrust.cartomobile-config-pkg": {
        source: "iana",
        extensions: ["c11amz"]
      },
      "application/vnd.coffeescript": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.document": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.document-template": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.presentation": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.presentation-template": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.spreadsheet": {
        source: "iana"
      },
      "application/vnd.collabio.xodocuments.spreadsheet-template": {
        source: "iana"
      },
      "application/vnd.collection+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.collection.doc+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.collection.next+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.comicbook+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.comicbook-rar": {
        source: "iana"
      },
      "application/vnd.commerce-battelle": {
        source: "iana"
      },
      "application/vnd.commonspace": {
        source: "iana",
        extensions: ["csp"]
      },
      "application/vnd.contact.cmsg": {
        source: "iana",
        extensions: ["cdbcmsg"]
      },
      "application/vnd.coreos.ignition+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cosmocaller": {
        source: "iana",
        extensions: ["cmc"]
      },
      "application/vnd.crick.clicker": {
        source: "iana",
        extensions: ["clkx"]
      },
      "application/vnd.crick.clicker.keyboard": {
        source: "iana",
        extensions: ["clkk"]
      },
      "application/vnd.crick.clicker.palette": {
        source: "iana",
        extensions: ["clkp"]
      },
      "application/vnd.crick.clicker.template": {
        source: "iana",
        extensions: ["clkt"]
      },
      "application/vnd.crick.clicker.wordbank": {
        source: "iana",
        extensions: ["clkw"]
      },
      "application/vnd.criticaltools.wbs+xml": {
        source: "iana",
        compressible: true,
        extensions: ["wbs"]
      },
      "application/vnd.cryptii.pipe+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.crypto-shade-file": {
        source: "iana"
      },
      "application/vnd.cryptomator.encrypted": {
        source: "iana"
      },
      "application/vnd.cryptomator.vault": {
        source: "iana"
      },
      "application/vnd.ctc-posml": {
        source: "iana",
        extensions: ["pml"]
      },
      "application/vnd.ctct.ws+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cups-pdf": {
        source: "iana"
      },
      "application/vnd.cups-postscript": {
        source: "iana"
      },
      "application/vnd.cups-ppd": {
        source: "iana",
        extensions: ["ppd"]
      },
      "application/vnd.cups-raster": {
        source: "iana"
      },
      "application/vnd.cups-raw": {
        source: "iana"
      },
      "application/vnd.curl": {
        source: "iana"
      },
      "application/vnd.curl.car": {
        source: "apache",
        extensions: ["car"]
      },
      "application/vnd.curl.pcurl": {
        source: "apache",
        extensions: ["pcurl"]
      },
      "application/vnd.cyan.dean.root+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cybank": {
        source: "iana"
      },
      "application/vnd.cyclonedx+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.cyclonedx+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.d2l.coursepackage1p0+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.d3m-dataset": {
        source: "iana"
      },
      "application/vnd.d3m-problem": {
        source: "iana"
      },
      "application/vnd.dart": {
        source: "iana",
        compressible: true,
        extensions: ["dart"]
      },
      "application/vnd.data-vision.rdz": {
        source: "iana",
        extensions: ["rdz"]
      },
      "application/vnd.datapackage+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dataresource+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dbf": {
        source: "iana",
        extensions: ["dbf"]
      },
      "application/vnd.debian.binary-package": {
        source: "iana"
      },
      "application/vnd.dece.data": {
        source: "iana",
        extensions: ["uvf", "uvvf", "uvd", "uvvd"]
      },
      "application/vnd.dece.ttml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["uvt", "uvvt"]
      },
      "application/vnd.dece.unspecified": {
        source: "iana",
        extensions: ["uvx", "uvvx"]
      },
      "application/vnd.dece.zip": {
        source: "iana",
        extensions: ["uvz", "uvvz"]
      },
      "application/vnd.denovo.fcselayout-link": {
        source: "iana",
        extensions: ["fe_launch"]
      },
      "application/vnd.desmume.movie": {
        source: "iana"
      },
      "application/vnd.dir-bi.plate-dl-nosuffix": {
        source: "iana"
      },
      "application/vnd.dm.delegation+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dna": {
        source: "iana",
        extensions: ["dna"]
      },
      "application/vnd.document+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dolby.mlp": {
        source: "apache",
        extensions: ["mlp"]
      },
      "application/vnd.dolby.mobile.1": {
        source: "iana"
      },
      "application/vnd.dolby.mobile.2": {
        source: "iana"
      },
      "application/vnd.doremir.scorecloud-binary-document": {
        source: "iana"
      },
      "application/vnd.dpgraph": {
        source: "iana",
        extensions: ["dpg"]
      },
      "application/vnd.dreamfactory": {
        source: "iana",
        extensions: ["dfac"]
      },
      "application/vnd.drive+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ds-keypoint": {
        source: "apache",
        extensions: ["kpxx"]
      },
      "application/vnd.dtg.local": {
        source: "iana"
      },
      "application/vnd.dtg.local.flash": {
        source: "iana"
      },
      "application/vnd.dtg.local.html": {
        source: "iana"
      },
      "application/vnd.dvb.ait": {
        source: "iana",
        extensions: ["ait"]
      },
      "application/vnd.dvb.dvbisl+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.dvbj": {
        source: "iana"
      },
      "application/vnd.dvb.esgcontainer": {
        source: "iana"
      },
      "application/vnd.dvb.ipdcdftnotifaccess": {
        source: "iana"
      },
      "application/vnd.dvb.ipdcesgaccess": {
        source: "iana"
      },
      "application/vnd.dvb.ipdcesgaccess2": {
        source: "iana"
      },
      "application/vnd.dvb.ipdcesgpdd": {
        source: "iana"
      },
      "application/vnd.dvb.ipdcroaming": {
        source: "iana"
      },
      "application/vnd.dvb.iptv.alfec-base": {
        source: "iana"
      },
      "application/vnd.dvb.iptv.alfec-enhancement": {
        source: "iana"
      },
      "application/vnd.dvb.notif-aggregate-root+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-container+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-generic+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-ia-msglist+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-ia-registration-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-ia-registration-response+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.notif-init+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.dvb.pfr": {
        source: "iana"
      },
      "application/vnd.dvb.service": {
        source: "iana",
        extensions: ["svc"]
      },
      "application/vnd.dxr": {
        source: "iana"
      },
      "application/vnd.dynageo": {
        source: "iana",
        extensions: ["geo"]
      },
      "application/vnd.dzr": {
        source: "iana"
      },
      "application/vnd.easykaraoke.cdgdownload": {
        source: "iana"
      },
      "application/vnd.ecdis-update": {
        source: "iana"
      },
      "application/vnd.ecip.rlp": {
        source: "iana"
      },
      "application/vnd.eclipse.ditto+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ecowin.chart": {
        source: "iana",
        extensions: ["mag"]
      },
      "application/vnd.ecowin.filerequest": {
        source: "iana"
      },
      "application/vnd.ecowin.fileupdate": {
        source: "iana"
      },
      "application/vnd.ecowin.series": {
        source: "iana"
      },
      "application/vnd.ecowin.seriesrequest": {
        source: "iana"
      },
      "application/vnd.ecowin.seriesupdate": {
        source: "iana"
      },
      "application/vnd.efi.img": {
        source: "iana"
      },
      "application/vnd.efi.iso": {
        source: "iana"
      },
      "application/vnd.emclient.accessrequest+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.enliven": {
        source: "iana",
        extensions: ["nml"]
      },
      "application/vnd.enphase.envoy": {
        source: "iana"
      },
      "application/vnd.eprints.data+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.epson.esf": {
        source: "iana",
        extensions: ["esf"]
      },
      "application/vnd.epson.msf": {
        source: "iana",
        extensions: ["msf"]
      },
      "application/vnd.epson.quickanime": {
        source: "iana",
        extensions: ["qam"]
      },
      "application/vnd.epson.salt": {
        source: "iana",
        extensions: ["slt"]
      },
      "application/vnd.epson.ssf": {
        source: "iana",
        extensions: ["ssf"]
      },
      "application/vnd.ericsson.quickcall": {
        source: "iana"
      },
      "application/vnd.espass-espass+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.eszigno3+xml": {
        source: "iana",
        compressible: true,
        extensions: ["es3", "et3"]
      },
      "application/vnd.etsi.aoc+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.asic-e+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.etsi.asic-s+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.etsi.cug+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvcommand+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvdiscovery+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvprofile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvsad-bc+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvsad-cod+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvsad-npvr+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvservice+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvsync+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.iptvueprofile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.mcid+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.mheg5": {
        source: "iana"
      },
      "application/vnd.etsi.overload-control-policy-dataset+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.pstn+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.sci+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.simservs+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.timestamp-token": {
        source: "iana"
      },
      "application/vnd.etsi.tsl+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.etsi.tsl.der": {
        source: "iana"
      },
      "application/vnd.eu.kasparian.car+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.eudora.data": {
        source: "iana"
      },
      "application/vnd.evolv.ecig.profile": {
        source: "iana"
      },
      "application/vnd.evolv.ecig.settings": {
        source: "iana"
      },
      "application/vnd.evolv.ecig.theme": {
        source: "iana"
      },
      "application/vnd.exstream-empower+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.exstream-package": {
        source: "iana"
      },
      "application/vnd.ezpix-album": {
        source: "iana",
        extensions: ["ez2"]
      },
      "application/vnd.ezpix-package": {
        source: "iana",
        extensions: ["ez3"]
      },
      "application/vnd.f-secure.mobile": {
        source: "iana"
      },
      "application/vnd.familysearch.gedcom+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.fastcopy-disk-image": {
        source: "iana"
      },
      "application/vnd.fdf": {
        source: "iana",
        extensions: ["fdf"]
      },
      "application/vnd.fdsn.mseed": {
        source: "iana",
        extensions: ["mseed"]
      },
      "application/vnd.fdsn.seed": {
        source: "iana",
        extensions: ["seed", "dataless"]
      },
      "application/vnd.ffsns": {
        source: "iana"
      },
      "application/vnd.ficlab.flb+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.filmit.zfc": {
        source: "iana"
      },
      "application/vnd.fints": {
        source: "iana"
      },
      "application/vnd.firemonkeys.cloudcell": {
        source: "iana"
      },
      "application/vnd.flographit": {
        source: "iana",
        extensions: ["gph"]
      },
      "application/vnd.fluxtime.clip": {
        source: "iana",
        extensions: ["ftc"]
      },
      "application/vnd.font-fontforge-sfd": {
        source: "iana"
      },
      "application/vnd.framemaker": {
        source: "iana",
        extensions: ["fm", "frame", "maker", "book"]
      },
      "application/vnd.frogans.fnc": {
        source: "iana",
        extensions: ["fnc"]
      },
      "application/vnd.frogans.ltf": {
        source: "iana",
        extensions: ["ltf"]
      },
      "application/vnd.fsc.weblaunch": {
        source: "iana",
        extensions: ["fsc"]
      },
      "application/vnd.fujifilm.fb.docuworks": {
        source: "iana"
      },
      "application/vnd.fujifilm.fb.docuworks.binder": {
        source: "iana"
      },
      "application/vnd.fujifilm.fb.docuworks.container": {
        source: "iana"
      },
      "application/vnd.fujifilm.fb.jfi+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.fujitsu.oasys": {
        source: "iana",
        extensions: ["oas"]
      },
      "application/vnd.fujitsu.oasys2": {
        source: "iana",
        extensions: ["oa2"]
      },
      "application/vnd.fujitsu.oasys3": {
        source: "iana",
        extensions: ["oa3"]
      },
      "application/vnd.fujitsu.oasysgp": {
        source: "iana",
        extensions: ["fg5"]
      },
      "application/vnd.fujitsu.oasysprs": {
        source: "iana",
        extensions: ["bh2"]
      },
      "application/vnd.fujixerox.art-ex": {
        source: "iana"
      },
      "application/vnd.fujixerox.art4": {
        source: "iana"
      },
      "application/vnd.fujixerox.ddd": {
        source: "iana",
        extensions: ["ddd"]
      },
      "application/vnd.fujixerox.docuworks": {
        source: "iana",
        extensions: ["xdw"]
      },
      "application/vnd.fujixerox.docuworks.binder": {
        source: "iana",
        extensions: ["xbd"]
      },
      "application/vnd.fujixerox.docuworks.container": {
        source: "iana"
      },
      "application/vnd.fujixerox.hbpl": {
        source: "iana"
      },
      "application/vnd.fut-misnet": {
        source: "iana"
      },
      "application/vnd.futoin+cbor": {
        source: "iana"
      },
      "application/vnd.futoin+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.fuzzysheet": {
        source: "iana",
        extensions: ["fzs"]
      },
      "application/vnd.genomatix.tuxedo": {
        source: "iana",
        extensions: ["txd"]
      },
      "application/vnd.gentics.grd+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.geo+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.geocube+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.geogebra.file": {
        source: "iana",
        extensions: ["ggb"]
      },
      "application/vnd.geogebra.slides": {
        source: "iana"
      },
      "application/vnd.geogebra.tool": {
        source: "iana",
        extensions: ["ggt"]
      },
      "application/vnd.geometry-explorer": {
        source: "iana",
        extensions: ["gex", "gre"]
      },
      "application/vnd.geonext": {
        source: "iana",
        extensions: ["gxt"]
      },
      "application/vnd.geoplan": {
        source: "iana",
        extensions: ["g2w"]
      },
      "application/vnd.geospace": {
        source: "iana",
        extensions: ["g3w"]
      },
      "application/vnd.gerber": {
        source: "iana"
      },
      "application/vnd.globalplatform.card-content-mgt": {
        source: "iana"
      },
      "application/vnd.globalplatform.card-content-mgt-response": {
        source: "iana"
      },
      "application/vnd.gmx": {
        source: "iana",
        extensions: ["gmx"]
      },
      "application/vnd.google-apps.document": {
        compressible: false,
        extensions: ["gdoc"]
      },
      "application/vnd.google-apps.presentation": {
        compressible: false,
        extensions: ["gslides"]
      },
      "application/vnd.google-apps.spreadsheet": {
        compressible: false,
        extensions: ["gsheet"]
      },
      "application/vnd.google-earth.kml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["kml"]
      },
      "application/vnd.google-earth.kmz": {
        source: "iana",
        compressible: false,
        extensions: ["kmz"]
      },
      "application/vnd.gov.sk.e-form+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.gov.sk.e-form+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.gov.sk.xmldatacontainer+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.grafeq": {
        source: "iana",
        extensions: ["gqf", "gqs"]
      },
      "application/vnd.gridmp": {
        source: "iana"
      },
      "application/vnd.groove-account": {
        source: "iana",
        extensions: ["gac"]
      },
      "application/vnd.groove-help": {
        source: "iana",
        extensions: ["ghf"]
      },
      "application/vnd.groove-identity-message": {
        source: "iana",
        extensions: ["gim"]
      },
      "application/vnd.groove-injector": {
        source: "iana",
        extensions: ["grv"]
      },
      "application/vnd.groove-tool-message": {
        source: "iana",
        extensions: ["gtm"]
      },
      "application/vnd.groove-tool-template": {
        source: "iana",
        extensions: ["tpl"]
      },
      "application/vnd.groove-vcard": {
        source: "iana",
        extensions: ["vcg"]
      },
      "application/vnd.hal+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hal+xml": {
        source: "iana",
        compressible: true,
        extensions: ["hal"]
      },
      "application/vnd.handheld-entertainment+xml": {
        source: "iana",
        compressible: true,
        extensions: ["zmm"]
      },
      "application/vnd.hbci": {
        source: "iana",
        extensions: ["hbci"]
      },
      "application/vnd.hc+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hcl-bireports": {
        source: "iana"
      },
      "application/vnd.hdt": {
        source: "iana"
      },
      "application/vnd.heroku+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hhe.lesson-player": {
        source: "iana",
        extensions: ["les"]
      },
      "application/vnd.hl7cda+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.hl7v2+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.hp-hpgl": {
        source: "iana",
        extensions: ["hpgl"]
      },
      "application/vnd.hp-hpid": {
        source: "iana",
        extensions: ["hpid"]
      },
      "application/vnd.hp-hps": {
        source: "iana",
        extensions: ["hps"]
      },
      "application/vnd.hp-jlyt": {
        source: "iana",
        extensions: ["jlt"]
      },
      "application/vnd.hp-pcl": {
        source: "iana",
        extensions: ["pcl"]
      },
      "application/vnd.hp-pclxl": {
        source: "iana",
        extensions: ["pclxl"]
      },
      "application/vnd.httphone": {
        source: "iana"
      },
      "application/vnd.hydrostatix.sof-data": {
        source: "iana",
        extensions: ["sfd-hdstx"]
      },
      "application/vnd.hyper+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hyper-item+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hyperdrive+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.hzn-3d-crossword": {
        source: "iana"
      },
      "application/vnd.ibm.afplinedata": {
        source: "iana"
      },
      "application/vnd.ibm.electronic-media": {
        source: "iana"
      },
      "application/vnd.ibm.minipay": {
        source: "iana",
        extensions: ["mpy"]
      },
      "application/vnd.ibm.modcap": {
        source: "iana",
        extensions: ["afp", "listafp", "list3820"]
      },
      "application/vnd.ibm.rights-management": {
        source: "iana",
        extensions: ["irm"]
      },
      "application/vnd.ibm.secure-container": {
        source: "iana",
        extensions: ["sc"]
      },
      "application/vnd.iccprofile": {
        source: "iana",
        extensions: ["icc", "icm"]
      },
      "application/vnd.ieee.1905": {
        source: "iana"
      },
      "application/vnd.igloader": {
        source: "iana",
        extensions: ["igl"]
      },
      "application/vnd.imagemeter.folder+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.imagemeter.image+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.immervision-ivp": {
        source: "iana",
        extensions: ["ivp"]
      },
      "application/vnd.immervision-ivu": {
        source: "iana",
        extensions: ["ivu"]
      },
      "application/vnd.ims.imsccv1p1": {
        source: "iana"
      },
      "application/vnd.ims.imsccv1p2": {
        source: "iana"
      },
      "application/vnd.ims.imsccv1p3": {
        source: "iana"
      },
      "application/vnd.ims.lis.v2.result+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ims.lti.v2.toolconsumerprofile+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ims.lti.v2.toolproxy+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ims.lti.v2.toolproxy.id+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ims.lti.v2.toolsettings+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ims.lti.v2.toolsettings.simple+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.informedcontrol.rms+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.informix-visionary": {
        source: "iana"
      },
      "application/vnd.infotech.project": {
        source: "iana"
      },
      "application/vnd.infotech.project+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.innopath.wamp.notification": {
        source: "iana"
      },
      "application/vnd.insors.igm": {
        source: "iana",
        extensions: ["igm"]
      },
      "application/vnd.intercon.formnet": {
        source: "iana",
        extensions: ["xpw", "xpx"]
      },
      "application/vnd.intergeo": {
        source: "iana",
        extensions: ["i2g"]
      },
      "application/vnd.intertrust.digibox": {
        source: "iana"
      },
      "application/vnd.intertrust.nncp": {
        source: "iana"
      },
      "application/vnd.intu.qbo": {
        source: "iana",
        extensions: ["qbo"]
      },
      "application/vnd.intu.qfx": {
        source: "iana",
        extensions: ["qfx"]
      },
      "application/vnd.iptc.g2.catalogitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.conceptitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.knowledgeitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.newsitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.newsmessage+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.packageitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.iptc.g2.planningitem+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ipunplugged.rcprofile": {
        source: "iana",
        extensions: ["rcprofile"]
      },
      "application/vnd.irepository.package+xml": {
        source: "iana",
        compressible: true,
        extensions: ["irp"]
      },
      "application/vnd.is-xpr": {
        source: "iana",
        extensions: ["xpr"]
      },
      "application/vnd.isac.fcs": {
        source: "iana",
        extensions: ["fcs"]
      },
      "application/vnd.iso11783-10+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.jam": {
        source: "iana",
        extensions: ["jam"]
      },
      "application/vnd.japannet-directory-service": {
        source: "iana"
      },
      "application/vnd.japannet-jpnstore-wakeup": {
        source: "iana"
      },
      "application/vnd.japannet-payment-wakeup": {
        source: "iana"
      },
      "application/vnd.japannet-registration": {
        source: "iana"
      },
      "application/vnd.japannet-registration-wakeup": {
        source: "iana"
      },
      "application/vnd.japannet-setstore-wakeup": {
        source: "iana"
      },
      "application/vnd.japannet-verification": {
        source: "iana"
      },
      "application/vnd.japannet-verification-wakeup": {
        source: "iana"
      },
      "application/vnd.jcp.javame.midlet-rms": {
        source: "iana",
        extensions: ["rms"]
      },
      "application/vnd.jisp": {
        source: "iana",
        extensions: ["jisp"]
      },
      "application/vnd.joost.joda-archive": {
        source: "iana",
        extensions: ["joda"]
      },
      "application/vnd.jsk.isdn-ngn": {
        source: "iana"
      },
      "application/vnd.kahootz": {
        source: "iana",
        extensions: ["ktz", "ktr"]
      },
      "application/vnd.kde.karbon": {
        source: "iana",
        extensions: ["karbon"]
      },
      "application/vnd.kde.kchart": {
        source: "iana",
        extensions: ["chrt"]
      },
      "application/vnd.kde.kformula": {
        source: "iana",
        extensions: ["kfo"]
      },
      "application/vnd.kde.kivio": {
        source: "iana",
        extensions: ["flw"]
      },
      "application/vnd.kde.kontour": {
        source: "iana",
        extensions: ["kon"]
      },
      "application/vnd.kde.kpresenter": {
        source: "iana",
        extensions: ["kpr", "kpt"]
      },
      "application/vnd.kde.kspread": {
        source: "iana",
        extensions: ["ksp"]
      },
      "application/vnd.kde.kword": {
        source: "iana",
        extensions: ["kwd", "kwt"]
      },
      "application/vnd.kenameaapp": {
        source: "iana",
        extensions: ["htke"]
      },
      "application/vnd.kidspiration": {
        source: "iana",
        extensions: ["kia"]
      },
      "application/vnd.kinar": {
        source: "iana",
        extensions: ["kne", "knp"]
      },
      "application/vnd.koan": {
        source: "iana",
        extensions: ["skp", "skd", "skt", "skm"]
      },
      "application/vnd.kodak-descriptor": {
        source: "iana",
        extensions: ["sse"]
      },
      "application/vnd.las": {
        source: "iana"
      },
      "application/vnd.las.las+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.las.las+xml": {
        source: "iana",
        compressible: true,
        extensions: ["lasxml"]
      },
      "application/vnd.laszip": {
        source: "iana"
      },
      "application/vnd.leap+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.liberty-request+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.llamagraphics.life-balance.desktop": {
        source: "iana",
        extensions: ["lbd"]
      },
      "application/vnd.llamagraphics.life-balance.exchange+xml": {
        source: "iana",
        compressible: true,
        extensions: ["lbe"]
      },
      "application/vnd.logipipe.circuit+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.loom": {
        source: "iana"
      },
      "application/vnd.lotus-1-2-3": {
        source: "iana",
        extensions: ["123"]
      },
      "application/vnd.lotus-approach": {
        source: "iana",
        extensions: ["apr"]
      },
      "application/vnd.lotus-freelance": {
        source: "iana",
        extensions: ["pre"]
      },
      "application/vnd.lotus-notes": {
        source: "iana",
        extensions: ["nsf"]
      },
      "application/vnd.lotus-organizer": {
        source: "iana",
        extensions: ["org"]
      },
      "application/vnd.lotus-screencam": {
        source: "iana",
        extensions: ["scm"]
      },
      "application/vnd.lotus-wordpro": {
        source: "iana",
        extensions: ["lwp"]
      },
      "application/vnd.macports.portpkg": {
        source: "iana",
        extensions: ["portpkg"]
      },
      "application/vnd.mapbox-vector-tile": {
        source: "iana",
        extensions: ["mvt"]
      },
      "application/vnd.marlin.drm.actiontoken+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.marlin.drm.conftoken+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.marlin.drm.license+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.marlin.drm.mdcf": {
        source: "iana"
      },
      "application/vnd.mason+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.maxar.archive.3tz+zip": {
        source: "iana",
        compressible: false
      },
      "application/vnd.maxmind.maxmind-db": {
        source: "iana"
      },
      "application/vnd.mcd": {
        source: "iana",
        extensions: ["mcd"]
      },
      "application/vnd.medcalcdata": {
        source: "iana",
        extensions: ["mc1"]
      },
      "application/vnd.mediastation.cdkey": {
        source: "iana",
        extensions: ["cdkey"]
      },
      "application/vnd.meridian-slingshot": {
        source: "iana"
      },
      "application/vnd.mfer": {
        source: "iana",
        extensions: ["mwf"]
      },
      "application/vnd.mfmp": {
        source: "iana",
        extensions: ["mfm"]
      },
      "application/vnd.micro+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.micrografx.flo": {
        source: "iana",
        extensions: ["flo"]
      },
      "application/vnd.micrografx.igx": {
        source: "iana",
        extensions: ["igx"]
      },
      "application/vnd.microsoft.portable-executable": {
        source: "iana"
      },
      "application/vnd.microsoft.windows.thumbnail-cache": {
        source: "iana"
      },
      "application/vnd.miele+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.mif": {
        source: "iana",
        extensions: ["mif"]
      },
      "application/vnd.minisoft-hp3000-save": {
        source: "iana"
      },
      "application/vnd.mitsubishi.misty-guard.trustweb": {
        source: "iana"
      },
      "application/vnd.mobius.daf": {
        source: "iana",
        extensions: ["daf"]
      },
      "application/vnd.mobius.dis": {
        source: "iana",
        extensions: ["dis"]
      },
      "application/vnd.mobius.mbk": {
        source: "iana",
        extensions: ["mbk"]
      },
      "application/vnd.mobius.mqy": {
        source: "iana",
        extensions: ["mqy"]
      },
      "application/vnd.mobius.msl": {
        source: "iana",
        extensions: ["msl"]
      },
      "application/vnd.mobius.plc": {
        source: "iana",
        extensions: ["plc"]
      },
      "application/vnd.mobius.txf": {
        source: "iana",
        extensions: ["txf"]
      },
      "application/vnd.mophun.application": {
        source: "iana",
        extensions: ["mpn"]
      },
      "application/vnd.mophun.certificate": {
        source: "iana",
        extensions: ["mpc"]
      },
      "application/vnd.motorola.flexsuite": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.adsi": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.fis": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.gotap": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.kmr": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.ttc": {
        source: "iana"
      },
      "application/vnd.motorola.flexsuite.wem": {
        source: "iana"
      },
      "application/vnd.motorola.iprm": {
        source: "iana"
      },
      "application/vnd.mozilla.xul+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xul"]
      },
      "application/vnd.ms-3mfdocument": {
        source: "iana"
      },
      "application/vnd.ms-artgalry": {
        source: "iana",
        extensions: ["cil"]
      },
      "application/vnd.ms-asf": {
        source: "iana"
      },
      "application/vnd.ms-cab-compressed": {
        source: "iana",
        extensions: ["cab"]
      },
      "application/vnd.ms-color.iccprofile": {
        source: "apache"
      },
      "application/vnd.ms-excel": {
        source: "iana",
        compressible: false,
        extensions: ["xls", "xlm", "xla", "xlc", "xlt", "xlw"]
      },
      "application/vnd.ms-excel.addin.macroenabled.12": {
        source: "iana",
        extensions: ["xlam"]
      },
      "application/vnd.ms-excel.sheet.binary.macroenabled.12": {
        source: "iana",
        extensions: ["xlsb"]
      },
      "application/vnd.ms-excel.sheet.macroenabled.12": {
        source: "iana",
        extensions: ["xlsm"]
      },
      "application/vnd.ms-excel.template.macroenabled.12": {
        source: "iana",
        extensions: ["xltm"]
      },
      "application/vnd.ms-fontobject": {
        source: "iana",
        compressible: true,
        extensions: ["eot"]
      },
      "application/vnd.ms-htmlhelp": {
        source: "iana",
        extensions: ["chm"]
      },
      "application/vnd.ms-ims": {
        source: "iana",
        extensions: ["ims"]
      },
      "application/vnd.ms-lrm": {
        source: "iana",
        extensions: ["lrm"]
      },
      "application/vnd.ms-office.activex+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ms-officetheme": {
        source: "iana",
        extensions: ["thmx"]
      },
      "application/vnd.ms-opentype": {
        source: "apache",
        compressible: true
      },
      "application/vnd.ms-outlook": {
        compressible: false,
        extensions: ["msg"]
      },
      "application/vnd.ms-package.obfuscated-opentype": {
        source: "apache"
      },
      "application/vnd.ms-pki.seccat": {
        source: "apache",
        extensions: ["cat"]
      },
      "application/vnd.ms-pki.stl": {
        source: "apache",
        extensions: ["stl"]
      },
      "application/vnd.ms-playready.initiator+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ms-powerpoint": {
        source: "iana",
        compressible: false,
        extensions: ["ppt", "pps", "pot"]
      },
      "application/vnd.ms-powerpoint.addin.macroenabled.12": {
        source: "iana",
        extensions: ["ppam"]
      },
      "application/vnd.ms-powerpoint.presentation.macroenabled.12": {
        source: "iana",
        extensions: ["pptm"]
      },
      "application/vnd.ms-powerpoint.slide.macroenabled.12": {
        source: "iana",
        extensions: ["sldm"]
      },
      "application/vnd.ms-powerpoint.slideshow.macroenabled.12": {
        source: "iana",
        extensions: ["ppsm"]
      },
      "application/vnd.ms-powerpoint.template.macroenabled.12": {
        source: "iana",
        extensions: ["potm"]
      },
      "application/vnd.ms-printdevicecapabilities+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ms-printing.printticket+xml": {
        source: "apache",
        compressible: true
      },
      "application/vnd.ms-printschematicket+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ms-project": {
        source: "iana",
        extensions: ["mpp", "mpt"]
      },
      "application/vnd.ms-tnef": {
        source: "iana"
      },
      "application/vnd.ms-windows.devicepairing": {
        source: "iana"
      },
      "application/vnd.ms-windows.nwprinting.oob": {
        source: "iana"
      },
      "application/vnd.ms-windows.printerpairing": {
        source: "iana"
      },
      "application/vnd.ms-windows.wsd.oob": {
        source: "iana"
      },
      "application/vnd.ms-wmdrm.lic-chlg-req": {
        source: "iana"
      },
      "application/vnd.ms-wmdrm.lic-resp": {
        source: "iana"
      },
      "application/vnd.ms-wmdrm.meter-chlg-req": {
        source: "iana"
      },
      "application/vnd.ms-wmdrm.meter-resp": {
        source: "iana"
      },
      "application/vnd.ms-word.document.macroenabled.12": {
        source: "iana",
        extensions: ["docm"]
      },
      "application/vnd.ms-word.template.macroenabled.12": {
        source: "iana",
        extensions: ["dotm"]
      },
      "application/vnd.ms-works": {
        source: "iana",
        extensions: ["wps", "wks", "wcm", "wdb"]
      },
      "application/vnd.ms-wpl": {
        source: "iana",
        extensions: ["wpl"]
      },
      "application/vnd.ms-xpsdocument": {
        source: "iana",
        compressible: false,
        extensions: ["xps"]
      },
      "application/vnd.msa-disk-image": {
        source: "iana"
      },
      "application/vnd.mseq": {
        source: "iana",
        extensions: ["mseq"]
      },
      "application/vnd.msign": {
        source: "iana"
      },
      "application/vnd.multiad.creator": {
        source: "iana"
      },
      "application/vnd.multiad.creator.cif": {
        source: "iana"
      },
      "application/vnd.music-niff": {
        source: "iana"
      },
      "application/vnd.musician": {
        source: "iana",
        extensions: ["mus"]
      },
      "application/vnd.muvee.style": {
        source: "iana",
        extensions: ["msty"]
      },
      "application/vnd.mynfc": {
        source: "iana",
        extensions: ["taglet"]
      },
      "application/vnd.nacamar.ybrid+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.ncd.control": {
        source: "iana"
      },
      "application/vnd.ncd.reference": {
        source: "iana"
      },
      "application/vnd.nearst.inv+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nebumind.line": {
        source: "iana"
      },
      "application/vnd.nervana": {
        source: "iana"
      },
      "application/vnd.netfpx": {
        source: "iana"
      },
      "application/vnd.neurolanguage.nlu": {
        source: "iana",
        extensions: ["nlu"]
      },
      "application/vnd.nimn": {
        source: "iana"
      },
      "application/vnd.nintendo.nitro.rom": {
        source: "iana"
      },
      "application/vnd.nintendo.snes.rom": {
        source: "iana"
      },
      "application/vnd.nitf": {
        source: "iana",
        extensions: ["ntf", "nitf"]
      },
      "application/vnd.noblenet-directory": {
        source: "iana",
        extensions: ["nnd"]
      },
      "application/vnd.noblenet-sealer": {
        source: "iana",
        extensions: ["nns"]
      },
      "application/vnd.noblenet-web": {
        source: "iana",
        extensions: ["nnw"]
      },
      "application/vnd.nokia.catalogs": {
        source: "iana"
      },
      "application/vnd.nokia.conml+wbxml": {
        source: "iana"
      },
      "application/vnd.nokia.conml+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nokia.iptv.config+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nokia.isds-radio-presets": {
        source: "iana"
      },
      "application/vnd.nokia.landmark+wbxml": {
        source: "iana"
      },
      "application/vnd.nokia.landmark+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nokia.landmarkcollection+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nokia.n-gage.ac+xml": {
        source: "iana",
        compressible: true,
        extensions: ["ac"]
      },
      "application/vnd.nokia.n-gage.data": {
        source: "iana",
        extensions: ["ngdat"]
      },
      "application/vnd.nokia.n-gage.symbian.install": {
        source: "iana",
        extensions: ["n-gage"]
      },
      "application/vnd.nokia.ncd": {
        source: "iana"
      },
      "application/vnd.nokia.pcd+wbxml": {
        source: "iana"
      },
      "application/vnd.nokia.pcd+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.nokia.radio-preset": {
        source: "iana",
        extensions: ["rpst"]
      },
      "application/vnd.nokia.radio-presets": {
        source: "iana",
        extensions: ["rpss"]
      },
      "application/vnd.novadigm.edm": {
        source: "iana",
        extensions: ["edm"]
      },
      "application/vnd.novadigm.edx": {
        source: "iana",
        extensions: ["edx"]
      },
      "application/vnd.novadigm.ext": {
        source: "iana",
        extensions: ["ext"]
      },
      "application/vnd.ntt-local.content-share": {
        source: "iana"
      },
      "application/vnd.ntt-local.file-transfer": {
        source: "iana"
      },
      "application/vnd.ntt-local.ogw_remote-access": {
        source: "iana"
      },
      "application/vnd.ntt-local.sip-ta_remote": {
        source: "iana"
      },
      "application/vnd.ntt-local.sip-ta_tcp_stream": {
        source: "iana"
      },
      "application/vnd.oasis.opendocument.chart": {
        source: "iana",
        extensions: ["odc"]
      },
      "application/vnd.oasis.opendocument.chart-template": {
        source: "iana",
        extensions: ["otc"]
      },
      "application/vnd.oasis.opendocument.database": {
        source: "iana",
        extensions: ["odb"]
      },
      "application/vnd.oasis.opendocument.formula": {
        source: "iana",
        extensions: ["odf"]
      },
      "application/vnd.oasis.opendocument.formula-template": {
        source: "iana",
        extensions: ["odft"]
      },
      "application/vnd.oasis.opendocument.graphics": {
        source: "iana",
        compressible: false,
        extensions: ["odg"]
      },
      "application/vnd.oasis.opendocument.graphics-template": {
        source: "iana",
        extensions: ["otg"]
      },
      "application/vnd.oasis.opendocument.image": {
        source: "iana",
        extensions: ["odi"]
      },
      "application/vnd.oasis.opendocument.image-template": {
        source: "iana",
        extensions: ["oti"]
      },
      "application/vnd.oasis.opendocument.presentation": {
        source: "iana",
        compressible: false,
        extensions: ["odp"]
      },
      "application/vnd.oasis.opendocument.presentation-template": {
        source: "iana",
        extensions: ["otp"]
      },
      "application/vnd.oasis.opendocument.spreadsheet": {
        source: "iana",
        compressible: false,
        extensions: ["ods"]
      },
      "application/vnd.oasis.opendocument.spreadsheet-template": {
        source: "iana",
        extensions: ["ots"]
      },
      "application/vnd.oasis.opendocument.text": {
        source: "iana",
        compressible: false,
        extensions: ["odt"]
      },
      "application/vnd.oasis.opendocument.text-master": {
        source: "iana",
        extensions: ["odm"]
      },
      "application/vnd.oasis.opendocument.text-template": {
        source: "iana",
        extensions: ["ott"]
      },
      "application/vnd.oasis.opendocument.text-web": {
        source: "iana",
        extensions: ["oth"]
      },
      "application/vnd.obn": {
        source: "iana"
      },
      "application/vnd.ocf+cbor": {
        source: "iana"
      },
      "application/vnd.oci.image.manifest.v1+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oftn.l10n+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.contentaccessdownload+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.contentaccessstreaming+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.cspg-hexbinary": {
        source: "iana"
      },
      "application/vnd.oipf.dae.svg+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.dae.xhtml+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.mippvcontrolmessage+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.pae.gem": {
        source: "iana"
      },
      "application/vnd.oipf.spdiscovery+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.spdlist+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.ueprofile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oipf.userprofile+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.olpc-sugar": {
        source: "iana",
        extensions: ["xo"]
      },
      "application/vnd.oma-scws-config": {
        source: "iana"
      },
      "application/vnd.oma-scws-http-request": {
        source: "iana"
      },
      "application/vnd.oma-scws-http-response": {
        source: "iana"
      },
      "application/vnd.oma.bcast.associated-procedure-parameter+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.drm-trigger+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.imd+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.ltkm": {
        source: "iana"
      },
      "application/vnd.oma.bcast.notification+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.provisioningtrigger": {
        source: "iana"
      },
      "application/vnd.oma.bcast.sgboot": {
        source: "iana"
      },
      "application/vnd.oma.bcast.sgdd+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.sgdu": {
        source: "iana"
      },
      "application/vnd.oma.bcast.simple-symbol-container": {
        source: "iana"
      },
      "application/vnd.oma.bcast.smartcard-trigger+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.sprov+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.bcast.stkm": {
        source: "iana"
      },
      "application/vnd.oma.cab-address-book+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.cab-feature-handler+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.cab-pcc+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.cab-subs-invite+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.cab-user-prefs+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.dcd": {
        source: "iana"
      },
      "application/vnd.oma.dcdc": {
        source: "iana"
      },
      "application/vnd.oma.dd2+xml": {
        source: "iana",
        compressible: true,
        extensions: ["dd2"]
      },
      "application/vnd.oma.drm.risd+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.group-usage-list+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.lwm2m+cbor": {
        source: "iana"
      },
      "application/vnd.oma.lwm2m+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.lwm2m+tlv": {
        source: "iana"
      },
      "application/vnd.oma.pal+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.poc.detailed-progress-report+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.poc.final-report+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.poc.groups+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.poc.invocation-descriptor+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.poc.optimized-progress-report+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.push": {
        source: "iana"
      },
      "application/vnd.oma.scidm.messages+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oma.xcap-directory+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.omads-email+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.omads-file+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.omads-folder+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.omaloc-supl-init": {
        source: "iana"
      },
      "application/vnd.onepager": {
        source: "iana"
      },
      "application/vnd.onepagertamp": {
        source: "iana"
      },
      "application/vnd.onepagertamx": {
        source: "iana"
      },
      "application/vnd.onepagertat": {
        source: "iana"
      },
      "application/vnd.onepagertatp": {
        source: "iana"
      },
      "application/vnd.onepagertatx": {
        source: "iana"
      },
      "application/vnd.openblox.game+xml": {
        source: "iana",
        compressible: true,
        extensions: ["obgx"]
      },
      "application/vnd.openblox.game-binary": {
        source: "iana"
      },
      "application/vnd.openeye.oeb": {
        source: "iana"
      },
      "application/vnd.openofficeorg.extension": {
        source: "apache",
        extensions: ["oxt"]
      },
      "application/vnd.openstreetmap.data+xml": {
        source: "iana",
        compressible: true,
        extensions: ["osm"]
      },
      "application/vnd.opentimestamps.ots": {
        source: "iana"
      },
      "application/vnd.openxmlformats-officedocument.custom-properties+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.customxmlproperties+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawing+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.chart+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.chartshapes+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.diagramcolors+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.diagramdata+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.diagramlayout+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.drawingml.diagramstyle+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.extended-properties+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.commentauthors+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.comments+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.handoutmaster+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.notesmaster+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.notesslide+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.presentation": {
        source: "iana",
        compressible: false,
        extensions: ["pptx"]
      },
      "application/vnd.openxmlformats-officedocument.presentationml.presentation.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.presprops+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slide": {
        source: "iana",
        extensions: ["sldx"]
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slide+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slidelayout+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slidemaster+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slideshow": {
        source: "iana",
        extensions: ["ppsx"]
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slideshow.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.slideupdateinfo+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.tablestyles+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.tags+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.template": {
        source: "iana",
        extensions: ["potx"]
      },
      "application/vnd.openxmlformats-officedocument.presentationml.template.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.presentationml.viewprops+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.calcchain+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.chartsheet+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.comments+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.connections+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.dialogsheet+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.externallink+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotcachedefinition+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.pivotcacherecords+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.pivottable+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.querytable+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.revisionheaders+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.revisionlog+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedstrings+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": {
        source: "iana",
        compressible: false,
        extensions: ["xlsx"]
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheetmetadata+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.table+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.tablesinglecells+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.template": {
        source: "iana",
        extensions: ["xltx"]
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.template.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.usernames+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.volatiledependencies+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.theme+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.themeoverride+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.vmldrawing": {
        source: "iana"
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.comments+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document": {
        source: "iana",
        compressible: false,
        extensions: ["docx"]
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document.glossary+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.endnotes+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.fonttable+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.footer+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.footnotes+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.numbering+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.settings+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.styles+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.template": {
        source: "iana",
        extensions: ["dotx"]
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.template.main+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-officedocument.wordprocessingml.websettings+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-package.core-properties+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-package.digital-signature-xmlsignature+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.openxmlformats-package.relationships+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oracle.resource+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.orange.indata": {
        source: "iana"
      },
      "application/vnd.osa.netdeploy": {
        source: "iana"
      },
      "application/vnd.osgeo.mapguide.package": {
        source: "iana",
        extensions: ["mgp"]
      },
      "application/vnd.osgi.bundle": {
        source: "iana"
      },
      "application/vnd.osgi.dp": {
        source: "iana",
        extensions: ["dp"]
      },
      "application/vnd.osgi.subsystem": {
        source: "iana",
        extensions: ["esa"]
      },
      "application/vnd.otps.ct-kip+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.oxli.countgraph": {
        source: "iana"
      },
      "application/vnd.pagerduty+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.palm": {
        source: "iana",
        extensions: ["pdb", "pqa", "oprc"]
      },
      "application/vnd.panoply": {
        source: "iana"
      },
      "application/vnd.paos.xml": {
        source: "iana"
      },
      "application/vnd.patentdive": {
        source: "iana"
      },
      "application/vnd.patientecommsdoc": {
        source: "iana"
      },
      "application/vnd.pawaafile": {
        source: "iana",
        extensions: ["paw"]
      },
      "application/vnd.pcos": {
        source: "iana"
      },
      "application/vnd.pg.format": {
        source: "iana",
        extensions: ["str"]
      },
      "application/vnd.pg.osasli": {
        source: "iana",
        extensions: ["ei6"]
      },
      "application/vnd.piaccess.application-licence": {
        source: "iana"
      },
      "application/vnd.picsel": {
        source: "iana",
        extensions: ["efif"]
      },
      "application/vnd.pmi.widget": {
        source: "iana",
        extensions: ["wg"]
      },
      "application/vnd.poc.group-advertisement+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.pocketlearn": {
        source: "iana",
        extensions: ["plf"]
      },
      "application/vnd.powerbuilder6": {
        source: "iana",
        extensions: ["pbd"]
      },
      "application/vnd.powerbuilder6-s": {
        source: "iana"
      },
      "application/vnd.powerbuilder7": {
        source: "iana"
      },
      "application/vnd.powerbuilder7-s": {
        source: "iana"
      },
      "application/vnd.powerbuilder75": {
        source: "iana"
      },
      "application/vnd.powerbuilder75-s": {
        source: "iana"
      },
      "application/vnd.preminet": {
        source: "iana"
      },
      "application/vnd.previewsystems.box": {
        source: "iana",
        extensions: ["box"]
      },
      "application/vnd.proteus.magazine": {
        source: "iana",
        extensions: ["mgz"]
      },
      "application/vnd.psfs": {
        source: "iana"
      },
      "application/vnd.publishare-delta-tree": {
        source: "iana",
        extensions: ["qps"]
      },
      "application/vnd.pvi.ptid1": {
        source: "iana",
        extensions: ["ptid"]
      },
      "application/vnd.pwg-multiplexed": {
        source: "iana"
      },
      "application/vnd.pwg-xhtml-print+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.qualcomm.brew-app-res": {
        source: "iana"
      },
      "application/vnd.quarantainenet": {
        source: "iana"
      },
      "application/vnd.quark.quarkxpress": {
        source: "iana",
        extensions: ["qxd", "qxt", "qwd", "qwt", "qxl", "qxb"]
      },
      "application/vnd.quobject-quoxdocument": {
        source: "iana"
      },
      "application/vnd.radisys.moml+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-audit+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-audit-conf+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-audit-conn+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-audit-dialog+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-audit-stream+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-conf+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-base+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-fax-detect+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-fax-sendrecv+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-group+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-speech+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.radisys.msml-dialog-transform+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.rainstor.data": {
        source: "iana"
      },
      "application/vnd.rapid": {
        source: "iana"
      },
      "application/vnd.rar": {
        source: "iana",
        extensions: ["rar"]
      },
      "application/vnd.realvnc.bed": {
        source: "iana",
        extensions: ["bed"]
      },
      "application/vnd.recordare.musicxml": {
        source: "iana",
        extensions: ["mxl"]
      },
      "application/vnd.recordare.musicxml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["musicxml"]
      },
      "application/vnd.renlearn.rlprint": {
        source: "iana"
      },
      "application/vnd.resilient.logic": {
        source: "iana"
      },
      "application/vnd.restful+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.rig.cryptonote": {
        source: "iana",
        extensions: ["cryptonote"]
      },
      "application/vnd.rim.cod": {
        source: "apache",
        extensions: ["cod"]
      },
      "application/vnd.rn-realmedia": {
        source: "apache",
        extensions: ["rm"]
      },
      "application/vnd.rn-realmedia-vbr": {
        source: "apache",
        extensions: ["rmvb"]
      },
      "application/vnd.route66.link66+xml": {
        source: "iana",
        compressible: true,
        extensions: ["link66"]
      },
      "application/vnd.rs-274x": {
        source: "iana"
      },
      "application/vnd.ruckus.download": {
        source: "iana"
      },
      "application/vnd.s3sms": {
        source: "iana"
      },
      "application/vnd.sailingtracker.track": {
        source: "iana",
        extensions: ["st"]
      },
      "application/vnd.sar": {
        source: "iana"
      },
      "application/vnd.sbm.cid": {
        source: "iana"
      },
      "application/vnd.sbm.mid2": {
        source: "iana"
      },
      "application/vnd.scribus": {
        source: "iana"
      },
      "application/vnd.sealed.3df": {
        source: "iana"
      },
      "application/vnd.sealed.csf": {
        source: "iana"
      },
      "application/vnd.sealed.doc": {
        source: "iana"
      },
      "application/vnd.sealed.eml": {
        source: "iana"
      },
      "application/vnd.sealed.mht": {
        source: "iana"
      },
      "application/vnd.sealed.net": {
        source: "iana"
      },
      "application/vnd.sealed.ppt": {
        source: "iana"
      },
      "application/vnd.sealed.tiff": {
        source: "iana"
      },
      "application/vnd.sealed.xls": {
        source: "iana"
      },
      "application/vnd.sealedmedia.softseal.html": {
        source: "iana"
      },
      "application/vnd.sealedmedia.softseal.pdf": {
        source: "iana"
      },
      "application/vnd.seemail": {
        source: "iana",
        extensions: ["see"]
      },
      "application/vnd.seis+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.sema": {
        source: "iana",
        extensions: ["sema"]
      },
      "application/vnd.semd": {
        source: "iana",
        extensions: ["semd"]
      },
      "application/vnd.semf": {
        source: "iana",
        extensions: ["semf"]
      },
      "application/vnd.shade-save-file": {
        source: "iana"
      },
      "application/vnd.shana.informed.formdata": {
        source: "iana",
        extensions: ["ifm"]
      },
      "application/vnd.shana.informed.formtemplate": {
        source: "iana",
        extensions: ["itp"]
      },
      "application/vnd.shana.informed.interchange": {
        source: "iana",
        extensions: ["iif"]
      },
      "application/vnd.shana.informed.package": {
        source: "iana",
        extensions: ["ipk"]
      },
      "application/vnd.shootproof+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.shopkick+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.shp": {
        source: "iana"
      },
      "application/vnd.shx": {
        source: "iana"
      },
      "application/vnd.sigrok.session": {
        source: "iana"
      },
      "application/vnd.simtech-mindmapper": {
        source: "iana",
        extensions: ["twd", "twds"]
      },
      "application/vnd.siren+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.smaf": {
        source: "iana",
        extensions: ["mmf"]
      },
      "application/vnd.smart.notebook": {
        source: "iana"
      },
      "application/vnd.smart.teacher": {
        source: "iana",
        extensions: ["teacher"]
      },
      "application/vnd.snesdev-page-table": {
        source: "iana"
      },
      "application/vnd.software602.filler.form+xml": {
        source: "iana",
        compressible: true,
        extensions: ["fo"]
      },
      "application/vnd.software602.filler.form-xml-zip": {
        source: "iana"
      },
      "application/vnd.solent.sdkm+xml": {
        source: "iana",
        compressible: true,
        extensions: ["sdkm", "sdkd"]
      },
      "application/vnd.spotfire.dxp": {
        source: "iana",
        extensions: ["dxp"]
      },
      "application/vnd.spotfire.sfs": {
        source: "iana",
        extensions: ["sfs"]
      },
      "application/vnd.sqlite3": {
        source: "iana"
      },
      "application/vnd.sss-cod": {
        source: "iana"
      },
      "application/vnd.sss-dtf": {
        source: "iana"
      },
      "application/vnd.sss-ntf": {
        source: "iana"
      },
      "application/vnd.stardivision.calc": {
        source: "apache",
        extensions: ["sdc"]
      },
      "application/vnd.stardivision.draw": {
        source: "apache",
        extensions: ["sda"]
      },
      "application/vnd.stardivision.impress": {
        source: "apache",
        extensions: ["sdd"]
      },
      "application/vnd.stardivision.math": {
        source: "apache",
        extensions: ["smf"]
      },
      "application/vnd.stardivision.writer": {
        source: "apache",
        extensions: ["sdw", "vor"]
      },
      "application/vnd.stardivision.writer-global": {
        source: "apache",
        extensions: ["sgl"]
      },
      "application/vnd.stepmania.package": {
        source: "iana",
        extensions: ["smzip"]
      },
      "application/vnd.stepmania.stepchart": {
        source: "iana",
        extensions: ["sm"]
      },
      "application/vnd.street-stream": {
        source: "iana"
      },
      "application/vnd.sun.wadl+xml": {
        source: "iana",
        compressible: true,
        extensions: ["wadl"]
      },
      "application/vnd.sun.xml.calc": {
        source: "apache",
        extensions: ["sxc"]
      },
      "application/vnd.sun.xml.calc.template": {
        source: "apache",
        extensions: ["stc"]
      },
      "application/vnd.sun.xml.draw": {
        source: "apache",
        extensions: ["sxd"]
      },
      "application/vnd.sun.xml.draw.template": {
        source: "apache",
        extensions: ["std"]
      },
      "application/vnd.sun.xml.impress": {
        source: "apache",
        extensions: ["sxi"]
      },
      "application/vnd.sun.xml.impress.template": {
        source: "apache",
        extensions: ["sti"]
      },
      "application/vnd.sun.xml.math": {
        source: "apache",
        extensions: ["sxm"]
      },
      "application/vnd.sun.xml.writer": {
        source: "apache",
        extensions: ["sxw"]
      },
      "application/vnd.sun.xml.writer.global": {
        source: "apache",
        extensions: ["sxg"]
      },
      "application/vnd.sun.xml.writer.template": {
        source: "apache",
        extensions: ["stw"]
      },
      "application/vnd.sus-calendar": {
        source: "iana",
        extensions: ["sus", "susp"]
      },
      "application/vnd.svd": {
        source: "iana",
        extensions: ["svd"]
      },
      "application/vnd.swiftview-ics": {
        source: "iana"
      },
      "application/vnd.sycle+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.syft+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.symbian.install": {
        source: "apache",
        extensions: ["sis", "sisx"]
      },
      "application/vnd.syncml+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["xsm"]
      },
      "application/vnd.syncml.dm+wbxml": {
        source: "iana",
        charset: "UTF-8",
        extensions: ["bdm"]
      },
      "application/vnd.syncml.dm+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["xdm"]
      },
      "application/vnd.syncml.dm.notification": {
        source: "iana"
      },
      "application/vnd.syncml.dmddf+wbxml": {
        source: "iana"
      },
      "application/vnd.syncml.dmddf+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["ddf"]
      },
      "application/vnd.syncml.dmtnds+wbxml": {
        source: "iana"
      },
      "application/vnd.syncml.dmtnds+xml": {
        source: "iana",
        charset: "UTF-8",
        compressible: true
      },
      "application/vnd.syncml.ds.notification": {
        source: "iana"
      },
      "application/vnd.tableschema+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.tao.intent-module-archive": {
        source: "iana",
        extensions: ["tao"]
      },
      "application/vnd.tcpdump.pcap": {
        source: "iana",
        extensions: ["pcap", "cap", "dmp"]
      },
      "application/vnd.think-cell.ppttc+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.tmd.mediaflex.api+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.tml": {
        source: "iana"
      },
      "application/vnd.tmobile-livetv": {
        source: "iana",
        extensions: ["tmo"]
      },
      "application/vnd.tri.onesource": {
        source: "iana"
      },
      "application/vnd.trid.tpt": {
        source: "iana",
        extensions: ["tpt"]
      },
      "application/vnd.triscape.mxs": {
        source: "iana",
        extensions: ["mxs"]
      },
      "application/vnd.trueapp": {
        source: "iana",
        extensions: ["tra"]
      },
      "application/vnd.truedoc": {
        source: "iana"
      },
      "application/vnd.ubisoft.webplayer": {
        source: "iana"
      },
      "application/vnd.ufdl": {
        source: "iana",
        extensions: ["ufd", "ufdl"]
      },
      "application/vnd.uiq.theme": {
        source: "iana",
        extensions: ["utz"]
      },
      "application/vnd.umajin": {
        source: "iana",
        extensions: ["umj"]
      },
      "application/vnd.unity": {
        source: "iana",
        extensions: ["unityweb"]
      },
      "application/vnd.uoml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["uoml"]
      },
      "application/vnd.uplanet.alert": {
        source: "iana"
      },
      "application/vnd.uplanet.alert-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.bearer-choice": {
        source: "iana"
      },
      "application/vnd.uplanet.bearer-choice-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.cacheop": {
        source: "iana"
      },
      "application/vnd.uplanet.cacheop-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.channel": {
        source: "iana"
      },
      "application/vnd.uplanet.channel-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.list": {
        source: "iana"
      },
      "application/vnd.uplanet.list-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.listcmd": {
        source: "iana"
      },
      "application/vnd.uplanet.listcmd-wbxml": {
        source: "iana"
      },
      "application/vnd.uplanet.signal": {
        source: "iana"
      },
      "application/vnd.uri-map": {
        source: "iana"
      },
      "application/vnd.valve.source.material": {
        source: "iana"
      },
      "application/vnd.vcx": {
        source: "iana",
        extensions: ["vcx"]
      },
      "application/vnd.vd-study": {
        source: "iana"
      },
      "application/vnd.vectorworks": {
        source: "iana"
      },
      "application/vnd.vel+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.verimatrix.vcas": {
        source: "iana"
      },
      "application/vnd.veritone.aion+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.veryant.thin": {
        source: "iana"
      },
      "application/vnd.ves.encrypted": {
        source: "iana"
      },
      "application/vnd.vidsoft.vidconference": {
        source: "iana"
      },
      "application/vnd.visio": {
        source: "iana",
        extensions: ["vsd", "vst", "vss", "vsw"]
      },
      "application/vnd.visionary": {
        source: "iana",
        extensions: ["vis"]
      },
      "application/vnd.vividence.scriptfile": {
        source: "iana"
      },
      "application/vnd.vsf": {
        source: "iana",
        extensions: ["vsf"]
      },
      "application/vnd.wap.sic": {
        source: "iana"
      },
      "application/vnd.wap.slc": {
        source: "iana"
      },
      "application/vnd.wap.wbxml": {
        source: "iana",
        charset: "UTF-8",
        extensions: ["wbxml"]
      },
      "application/vnd.wap.wmlc": {
        source: "iana",
        extensions: ["wmlc"]
      },
      "application/vnd.wap.wmlscriptc": {
        source: "iana",
        extensions: ["wmlsc"]
      },
      "application/vnd.webturbo": {
        source: "iana",
        extensions: ["wtb"]
      },
      "application/vnd.wfa.dpp": {
        source: "iana"
      },
      "application/vnd.wfa.p2p": {
        source: "iana"
      },
      "application/vnd.wfa.wsc": {
        source: "iana"
      },
      "application/vnd.windows.devicepairing": {
        source: "iana"
      },
      "application/vnd.wmc": {
        source: "iana"
      },
      "application/vnd.wmf.bootstrap": {
        source: "iana"
      },
      "application/vnd.wolfram.mathematica": {
        source: "iana"
      },
      "application/vnd.wolfram.mathematica.package": {
        source: "iana"
      },
      "application/vnd.wolfram.player": {
        source: "iana",
        extensions: ["nbp"]
      },
      "application/vnd.wordperfect": {
        source: "iana",
        extensions: ["wpd"]
      },
      "application/vnd.wqd": {
        source: "iana",
        extensions: ["wqd"]
      },
      "application/vnd.wrq-hp3000-labelled": {
        source: "iana"
      },
      "application/vnd.wt.stf": {
        source: "iana",
        extensions: ["stf"]
      },
      "application/vnd.wv.csp+wbxml": {
        source: "iana"
      },
      "application/vnd.wv.csp+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.wv.ssp+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.xacml+json": {
        source: "iana",
        compressible: true
      },
      "application/vnd.xara": {
        source: "iana",
        extensions: ["xar"]
      },
      "application/vnd.xfdl": {
        source: "iana",
        extensions: ["xfdl"]
      },
      "application/vnd.xfdl.webform": {
        source: "iana"
      },
      "application/vnd.xmi+xml": {
        source: "iana",
        compressible: true
      },
      "application/vnd.xmpie.cpkg": {
        source: "iana"
      },
      "application/vnd.xmpie.dpkg": {
        source: "iana"
      },
      "application/vnd.xmpie.plan": {
        source: "iana"
      },
      "application/vnd.xmpie.ppkg": {
        source: "iana"
      },
      "application/vnd.xmpie.xlim": {
        source: "iana"
      },
      "application/vnd.yamaha.hv-dic": {
        source: "iana",
        extensions: ["hvd"]
      },
      "application/vnd.yamaha.hv-script": {
        source: "iana",
        extensions: ["hvs"]
      },
      "application/vnd.yamaha.hv-voice": {
        source: "iana",
        extensions: ["hvp"]
      },
      "application/vnd.yamaha.openscoreformat": {
        source: "iana",
        extensions: ["osf"]
      },
      "application/vnd.yamaha.openscoreformat.osfpvg+xml": {
        source: "iana",
        compressible: true,
        extensions: ["osfpvg"]
      },
      "application/vnd.yamaha.remote-setup": {
        source: "iana"
      },
      "application/vnd.yamaha.smaf-audio": {
        source: "iana",
        extensions: ["saf"]
      },
      "application/vnd.yamaha.smaf-phrase": {
        source: "iana",
        extensions: ["spf"]
      },
      "application/vnd.yamaha.through-ngn": {
        source: "iana"
      },
      "application/vnd.yamaha.tunnel-udpencap": {
        source: "iana"
      },
      "application/vnd.yaoweme": {
        source: "iana"
      },
      "application/vnd.yellowriver-custom-menu": {
        source: "iana",
        extensions: ["cmp"]
      },
      "application/vnd.youtube.yt": {
        source: "iana"
      },
      "application/vnd.zul": {
        source: "iana",
        extensions: ["zir", "zirz"]
      },
      "application/vnd.zzazz.deck+xml": {
        source: "iana",
        compressible: true,
        extensions: ["zaz"]
      },
      "application/voicexml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["vxml"]
      },
      "application/voucher-cms+json": {
        source: "iana",
        compressible: true
      },
      "application/vq-rtcpxr": {
        source: "iana"
      },
      "application/wasm": {
        source: "iana",
        compressible: true,
        extensions: ["wasm"]
      },
      "application/watcherinfo+xml": {
        source: "iana",
        compressible: true,
        extensions: ["wif"]
      },
      "application/webpush-options+json": {
        source: "iana",
        compressible: true
      },
      "application/whoispp-query": {
        source: "iana"
      },
      "application/whoispp-response": {
        source: "iana"
      },
      "application/widget": {
        source: "iana",
        extensions: ["wgt"]
      },
      "application/winhlp": {
        source: "apache",
        extensions: ["hlp"]
      },
      "application/wita": {
        source: "iana"
      },
      "application/wordperfect5.1": {
        source: "iana"
      },
      "application/wsdl+xml": {
        source: "iana",
        compressible: true,
        extensions: ["wsdl"]
      },
      "application/wspolicy+xml": {
        source: "iana",
        compressible: true,
        extensions: ["wspolicy"]
      },
      "application/x-7z-compressed": {
        source: "apache",
        compressible: false,
        extensions: ["7z"]
      },
      "application/x-abiword": {
        source: "apache",
        extensions: ["abw"]
      },
      "application/x-ace-compressed": {
        source: "apache",
        extensions: ["ace"]
      },
      "application/x-amf": {
        source: "apache"
      },
      "application/x-apple-diskimage": {
        source: "apache",
        extensions: ["dmg"]
      },
      "application/x-arj": {
        compressible: false,
        extensions: ["arj"]
      },
      "application/x-authorware-bin": {
        source: "apache",
        extensions: ["aab", "x32", "u32", "vox"]
      },
      "application/x-authorware-map": {
        source: "apache",
        extensions: ["aam"]
      },
      "application/x-authorware-seg": {
        source: "apache",
        extensions: ["aas"]
      },
      "application/x-bcpio": {
        source: "apache",
        extensions: ["bcpio"]
      },
      "application/x-bdoc": {
        compressible: false,
        extensions: ["bdoc"]
      },
      "application/x-bittorrent": {
        source: "apache",
        extensions: ["torrent"]
      },
      "application/x-blorb": {
        source: "apache",
        extensions: ["blb", "blorb"]
      },
      "application/x-bzip": {
        source: "apache",
        compressible: false,
        extensions: ["bz"]
      },
      "application/x-bzip2": {
        source: "apache",
        compressible: false,
        extensions: ["bz2", "boz"]
      },
      "application/x-cbr": {
        source: "apache",
        extensions: ["cbr", "cba", "cbt", "cbz", "cb7"]
      },
      "application/x-cdlink": {
        source: "apache",
        extensions: ["vcd"]
      },
      "application/x-cfs-compressed": {
        source: "apache",
        extensions: ["cfs"]
      },
      "application/x-chat": {
        source: "apache",
        extensions: ["chat"]
      },
      "application/x-chess-pgn": {
        source: "apache",
        extensions: ["pgn"]
      },
      "application/x-chrome-extension": {
        extensions: ["crx"]
      },
      "application/x-cocoa": {
        source: "nginx",
        extensions: ["cco"]
      },
      "application/x-compress": {
        source: "apache"
      },
      "application/x-conference": {
        source: "apache",
        extensions: ["nsc"]
      },
      "application/x-cpio": {
        source: "apache",
        extensions: ["cpio"]
      },
      "application/x-csh": {
        source: "apache",
        extensions: ["csh"]
      },
      "application/x-deb": {
        compressible: false
      },
      "application/x-debian-package": {
        source: "apache",
        extensions: ["deb", "udeb"]
      },
      "application/x-dgc-compressed": {
        source: "apache",
        extensions: ["dgc"]
      },
      "application/x-director": {
        source: "apache",
        extensions: ["dir", "dcr", "dxr", "cst", "cct", "cxt", "w3d", "fgd", "swa"]
      },
      "application/x-doom": {
        source: "apache",
        extensions: ["wad"]
      },
      "application/x-dtbncx+xml": {
        source: "apache",
        compressible: true,
        extensions: ["ncx"]
      },
      "application/x-dtbook+xml": {
        source: "apache",
        compressible: true,
        extensions: ["dtb"]
      },
      "application/x-dtbresource+xml": {
        source: "apache",
        compressible: true,
        extensions: ["res"]
      },
      "application/x-dvi": {
        source: "apache",
        compressible: false,
        extensions: ["dvi"]
      },
      "application/x-envoy": {
        source: "apache",
        extensions: ["evy"]
      },
      "application/x-eva": {
        source: "apache",
        extensions: ["eva"]
      },
      "application/x-font-bdf": {
        source: "apache",
        extensions: ["bdf"]
      },
      "application/x-font-dos": {
        source: "apache"
      },
      "application/x-font-framemaker": {
        source: "apache"
      },
      "application/x-font-ghostscript": {
        source: "apache",
        extensions: ["gsf"]
      },
      "application/x-font-libgrx": {
        source: "apache"
      },
      "application/x-font-linux-psf": {
        source: "apache",
        extensions: ["psf"]
      },
      "application/x-font-pcf": {
        source: "apache",
        extensions: ["pcf"]
      },
      "application/x-font-snf": {
        source: "apache",
        extensions: ["snf"]
      },
      "application/x-font-speedo": {
        source: "apache"
      },
      "application/x-font-sunos-news": {
        source: "apache"
      },
      "application/x-font-type1": {
        source: "apache",
        extensions: ["pfa", "pfb", "pfm", "afm"]
      },
      "application/x-font-vfont": {
        source: "apache"
      },
      "application/x-freearc": {
        source: "apache",
        extensions: ["arc"]
      },
      "application/x-futuresplash": {
        source: "apache",
        extensions: ["spl"]
      },
      "application/x-gca-compressed": {
        source: "apache",
        extensions: ["gca"]
      },
      "application/x-glulx": {
        source: "apache",
        extensions: ["ulx"]
      },
      "application/x-gnumeric": {
        source: "apache",
        extensions: ["gnumeric"]
      },
      "application/x-gramps-xml": {
        source: "apache",
        extensions: ["gramps"]
      },
      "application/x-gtar": {
        source: "apache",
        extensions: ["gtar"]
      },
      "application/x-gzip": {
        source: "apache"
      },
      "application/x-hdf": {
        source: "apache",
        extensions: ["hdf"]
      },
      "application/x-httpd-php": {
        compressible: true,
        extensions: ["php"]
      },
      "application/x-install-instructions": {
        source: "apache",
        extensions: ["install"]
      },
      "application/x-iso9660-image": {
        source: "apache",
        extensions: ["iso"]
      },
      "application/x-iwork-keynote-sffkey": {
        extensions: ["key"]
      },
      "application/x-iwork-numbers-sffnumbers": {
        extensions: ["numbers"]
      },
      "application/x-iwork-pages-sffpages": {
        extensions: ["pages"]
      },
      "application/x-java-archive-diff": {
        source: "nginx",
        extensions: ["jardiff"]
      },
      "application/x-java-jnlp-file": {
        source: "apache",
        compressible: false,
        extensions: ["jnlp"]
      },
      "application/x-javascript": {
        compressible: true
      },
      "application/x-keepass2": {
        extensions: ["kdbx"]
      },
      "application/x-latex": {
        source: "apache",
        compressible: false,
        extensions: ["latex"]
      },
      "application/x-lua-bytecode": {
        extensions: ["luac"]
      },
      "application/x-lzh-compressed": {
        source: "apache",
        extensions: ["lzh", "lha"]
      },
      "application/x-makeself": {
        source: "nginx",
        extensions: ["run"]
      },
      "application/x-mie": {
        source: "apache",
        extensions: ["mie"]
      },
      "application/x-mobipocket-ebook": {
        source: "apache",
        extensions: ["prc", "mobi"]
      },
      "application/x-mpegurl": {
        compressible: false
      },
      "application/x-ms-application": {
        source: "apache",
        extensions: ["application"]
      },
      "application/x-ms-shortcut": {
        source: "apache",
        extensions: ["lnk"]
      },
      "application/x-ms-wmd": {
        source: "apache",
        extensions: ["wmd"]
      },
      "application/x-ms-wmz": {
        source: "apache",
        extensions: ["wmz"]
      },
      "application/x-ms-xbap": {
        source: "apache",
        extensions: ["xbap"]
      },
      "application/x-msaccess": {
        source: "apache",
        extensions: ["mdb"]
      },
      "application/x-msbinder": {
        source: "apache",
        extensions: ["obd"]
      },
      "application/x-mscardfile": {
        source: "apache",
        extensions: ["crd"]
      },
      "application/x-msclip": {
        source: "apache",
        extensions: ["clp"]
      },
      "application/x-msdos-program": {
        extensions: ["exe"]
      },
      "application/x-msdownload": {
        source: "apache",
        extensions: ["exe", "dll", "com", "bat", "msi"]
      },
      "application/x-msmediaview": {
        source: "apache",
        extensions: ["mvb", "m13", "m14"]
      },
      "application/x-msmetafile": {
        source: "apache",
        extensions: ["wmf", "wmz", "emf", "emz"]
      },
      "application/x-msmoney": {
        source: "apache",
        extensions: ["mny"]
      },
      "application/x-mspublisher": {
        source: "apache",
        extensions: ["pub"]
      },
      "application/x-msschedule": {
        source: "apache",
        extensions: ["scd"]
      },
      "application/x-msterminal": {
        source: "apache",
        extensions: ["trm"]
      },
      "application/x-mswrite": {
        source: "apache",
        extensions: ["wri"]
      },
      "application/x-netcdf": {
        source: "apache",
        extensions: ["nc", "cdf"]
      },
      "application/x-ns-proxy-autoconfig": {
        compressible: true,
        extensions: ["pac"]
      },
      "application/x-nzb": {
        source: "apache",
        extensions: ["nzb"]
      },
      "application/x-perl": {
        source: "nginx",
        extensions: ["pl", "pm"]
      },
      "application/x-pilot": {
        source: "nginx",
        extensions: ["prc", "pdb"]
      },
      "application/x-pkcs12": {
        source: "apache",
        compressible: false,
        extensions: ["p12", "pfx"]
      },
      "application/x-pkcs7-certificates": {
        source: "apache",
        extensions: ["p7b", "spc"]
      },
      "application/x-pkcs7-certreqresp": {
        source: "apache",
        extensions: ["p7r"]
      },
      "application/x-pki-message": {
        source: "iana"
      },
      "application/x-rar-compressed": {
        source: "apache",
        compressible: false,
        extensions: ["rar"]
      },
      "application/x-redhat-package-manager": {
        source: "nginx",
        extensions: ["rpm"]
      },
      "application/x-research-info-systems": {
        source: "apache",
        extensions: ["ris"]
      },
      "application/x-sea": {
        source: "nginx",
        extensions: ["sea"]
      },
      "application/x-sh": {
        source: "apache",
        compressible: true,
        extensions: ["sh"]
      },
      "application/x-shar": {
        source: "apache",
        extensions: ["shar"]
      },
      "application/x-shockwave-flash": {
        source: "apache",
        compressible: false,
        extensions: ["swf"]
      },
      "application/x-silverlight-app": {
        source: "apache",
        extensions: ["xap"]
      },
      "application/x-sql": {
        source: "apache",
        extensions: ["sql"]
      },
      "application/x-stuffit": {
        source: "apache",
        compressible: false,
        extensions: ["sit"]
      },
      "application/x-stuffitx": {
        source: "apache",
        extensions: ["sitx"]
      },
      "application/x-subrip": {
        source: "apache",
        extensions: ["srt"]
      },
      "application/x-sv4cpio": {
        source: "apache",
        extensions: ["sv4cpio"]
      },
      "application/x-sv4crc": {
        source: "apache",
        extensions: ["sv4crc"]
      },
      "application/x-t3vm-image": {
        source: "apache",
        extensions: ["t3"]
      },
      "application/x-tads": {
        source: "apache",
        extensions: ["gam"]
      },
      "application/x-tar": {
        source: "apache",
        compressible: true,
        extensions: ["tar"]
      },
      "application/x-tcl": {
        source: "apache",
        extensions: ["tcl", "tk"]
      },
      "application/x-tex": {
        source: "apache",
        extensions: ["tex"]
      },
      "application/x-tex-tfm": {
        source: "apache",
        extensions: ["tfm"]
      },
      "application/x-texinfo": {
        source: "apache",
        extensions: ["texinfo", "texi"]
      },
      "application/x-tgif": {
        source: "apache",
        extensions: ["obj"]
      },
      "application/x-ustar": {
        source: "apache",
        extensions: ["ustar"]
      },
      "application/x-virtualbox-hdd": {
        compressible: true,
        extensions: ["hdd"]
      },
      "application/x-virtualbox-ova": {
        compressible: true,
        extensions: ["ova"]
      },
      "application/x-virtualbox-ovf": {
        compressible: true,
        extensions: ["ovf"]
      },
      "application/x-virtualbox-vbox": {
        compressible: true,
        extensions: ["vbox"]
      },
      "application/x-virtualbox-vbox-extpack": {
        compressible: false,
        extensions: ["vbox-extpack"]
      },
      "application/x-virtualbox-vdi": {
        compressible: true,
        extensions: ["vdi"]
      },
      "application/x-virtualbox-vhd": {
        compressible: true,
        extensions: ["vhd"]
      },
      "application/x-virtualbox-vmdk": {
        compressible: true,
        extensions: ["vmdk"]
      },
      "application/x-wais-source": {
        source: "apache",
        extensions: ["src"]
      },
      "application/x-web-app-manifest+json": {
        compressible: true,
        extensions: ["webapp"]
      },
      "application/x-www-form-urlencoded": {
        source: "iana",
        compressible: true
      },
      "application/x-x509-ca-cert": {
        source: "iana",
        extensions: ["der", "crt", "pem"]
      },
      "application/x-x509-ca-ra-cert": {
        source: "iana"
      },
      "application/x-x509-next-ca-cert": {
        source: "iana"
      },
      "application/x-xfig": {
        source: "apache",
        extensions: ["fig"]
      },
      "application/x-xliff+xml": {
        source: "apache",
        compressible: true,
        extensions: ["xlf"]
      },
      "application/x-xpinstall": {
        source: "apache",
        compressible: false,
        extensions: ["xpi"]
      },
      "application/x-xz": {
        source: "apache",
        extensions: ["xz"]
      },
      "application/x-zmachine": {
        source: "apache",
        extensions: ["z1", "z2", "z3", "z4", "z5", "z6", "z7", "z8"]
      },
      "application/x400-bp": {
        source: "iana"
      },
      "application/xacml+xml": {
        source: "iana",
        compressible: true
      },
      "application/xaml+xml": {
        source: "apache",
        compressible: true,
        extensions: ["xaml"]
      },
      "application/xcap-att+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xav"]
      },
      "application/xcap-caps+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xca"]
      },
      "application/xcap-diff+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xdf"]
      },
      "application/xcap-el+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xel"]
      },
      "application/xcap-error+xml": {
        source: "iana",
        compressible: true
      },
      "application/xcap-ns+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xns"]
      },
      "application/xcon-conference-info+xml": {
        source: "iana",
        compressible: true
      },
      "application/xcon-conference-info-diff+xml": {
        source: "iana",
        compressible: true
      },
      "application/xenc+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xenc"]
      },
      "application/xhtml+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xhtml", "xht"]
      },
      "application/xhtml-voice+xml": {
        source: "apache",
        compressible: true
      },
      "application/xliff+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xlf"]
      },
      "application/xml": {
        source: "iana",
        compressible: true,
        extensions: ["xml", "xsl", "xsd", "rng"]
      },
      "application/xml-dtd": {
        source: "iana",
        compressible: true,
        extensions: ["dtd"]
      },
      "application/xml-external-parsed-entity": {
        source: "iana"
      },
      "application/xml-patch+xml": {
        source: "iana",
        compressible: true
      },
      "application/xmpp+xml": {
        source: "iana",
        compressible: true
      },
      "application/xop+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xop"]
      },
      "application/xproc+xml": {
        source: "apache",
        compressible: true,
        extensions: ["xpl"]
      },
      "application/xslt+xml": {
        source: "iana",
        compressible: true,
        extensions: ["xsl", "xslt"]
      },
      "application/xspf+xml": {
        source: "apache",
        compressible: true,
        extensions: ["xspf"]
      },
      "application/xv+xml": {
        source: "iana",
        compressible: true,
        extensions: ["mxml", "xhvml", "xvml", "xvm"]
      },
      "application/yang": {
        source: "iana",
        extensions: ["yang"]
      },
      "application/yang-data+json": {
        source: "iana",
        compressible: true
      },
      "application/yang-data+xml": {
        source: "iana",
        compressible: true
      },
      "application/yang-patch+json": {
        source: "iana",
        compressible: true
      },
      "application/yang-patch+xml": {
        source: "iana",
        compressible: true
      },
      "application/yin+xml": {
        source: "iana",
        compressible: true,
        extensions: ["yin"]
      },
      "application/zip": {
        source: "iana",
        compressible: false,
        extensions: ["zip"]
      },
      "application/zlib": {
        source: "iana"
      },
      "application/zstd": {
        source: "iana"
      },
      "audio/1d-interleaved-parityfec": {
        source: "iana"
      },
      "audio/32kadpcm": {
        source: "iana"
      },
      "audio/3gpp": {
        source: "iana",
        compressible: false,
        extensions: ["3gpp"]
      },
      "audio/3gpp2": {
        source: "iana"
      },
      "audio/aac": {
        source: "iana"
      },
      "audio/ac3": {
        source: "iana"
      },
      "audio/adpcm": {
        source: "apache",
        extensions: ["adp"]
      },
      "audio/amr": {
        source: "iana",
        extensions: ["amr"]
      },
      "audio/amr-wb": {
        source: "iana"
      },
      "audio/amr-wb+": {
        source: "iana"
      },
      "audio/aptx": {
        source: "iana"
      },
      "audio/asc": {
        source: "iana"
      },
      "audio/atrac-advanced-lossless": {
        source: "iana"
      },
      "audio/atrac-x": {
        source: "iana"
      },
      "audio/atrac3": {
        source: "iana"
      },
      "audio/basic": {
        source: "iana",
        compressible: false,
        extensions: ["au", "snd"]
      },
      "audio/bv16": {
        source: "iana"
      },
      "audio/bv32": {
        source: "iana"
      },
      "audio/clearmode": {
        source: "iana"
      },
      "audio/cn": {
        source: "iana"
      },
      "audio/dat12": {
        source: "iana"
      },
      "audio/dls": {
        source: "iana"
      },
      "audio/dsr-es201108": {
        source: "iana"
      },
      "audio/dsr-es202050": {
        source: "iana"
      },
      "audio/dsr-es202211": {
        source: "iana"
      },
      "audio/dsr-es202212": {
        source: "iana"
      },
      "audio/dv": {
        source: "iana"
      },
      "audio/dvi4": {
        source: "iana"
      },
      "audio/eac3": {
        source: "iana"
      },
      "audio/encaprtp": {
        source: "iana"
      },
      "audio/evrc": {
        source: "iana"
      },
      "audio/evrc-qcp": {
        source: "iana"
      },
      "audio/evrc0": {
        source: "iana"
      },
      "audio/evrc1": {
        source: "iana"
      },
      "audio/evrcb": {
        source: "iana"
      },
      "audio/evrcb0": {
        source: "iana"
      },
      "audio/evrcb1": {
        source: "iana"
      },
      "audio/evrcnw": {
        source: "iana"
      },
      "audio/evrcnw0": {
        source: "iana"
      },
      "audio/evrcnw1": {
        source: "iana"
      },
      "audio/evrcwb": {
        source: "iana"
      },
      "audio/evrcwb0": {
        source: "iana"
      },
      "audio/evrcwb1": {
        source: "iana"
      },
      "audio/evs": {
        source: "iana"
      },
      "audio/flexfec": {
        source: "iana"
      },
      "audio/fwdred": {
        source: "iana"
      },
      "audio/g711-0": {
        source: "iana"
      },
      "audio/g719": {
        source: "iana"
      },
      "audio/g722": {
        source: "iana"
      },
      "audio/g7221": {
        source: "iana"
      },
      "audio/g723": {
        source: "iana"
      },
      "audio/g726-16": {
        source: "iana"
      },
      "audio/g726-24": {
        source: "iana"
      },
      "audio/g726-32": {
        source: "iana"
      },
      "audio/g726-40": {
        source: "iana"
      },
      "audio/g728": {
        source: "iana"
      },
      "audio/g729": {
        source: "iana"
      },
      "audio/g7291": {
        source: "iana"
      },
      "audio/g729d": {
        source: "iana"
      },
      "audio/g729e": {
        source: "iana"
      },
      "audio/gsm": {
        source: "iana"
      },
      "audio/gsm-efr": {
        source: "iana"
      },
      "audio/gsm-hr-08": {
        source: "iana"
      },
      "audio/ilbc": {
        source: "iana"
      },
      "audio/ip-mr_v2.5": {
        source: "iana"
      },
      "audio/isac": {
        source: "apache"
      },
      "audio/l16": {
        source: "iana"
      },
      "audio/l20": {
        source: "iana"
      },
      "audio/l24": {
        source: "iana",
        compressible: false
      },
      "audio/l8": {
        source: "iana"
      },
      "audio/lpc": {
        source: "iana"
      },
      "audio/melp": {
        source: "iana"
      },
      "audio/melp1200": {
        source: "iana"
      },
      "audio/melp2400": {
        source: "iana"
      },
      "audio/melp600": {
        source: "iana"
      },
      "audio/mhas": {
        source: "iana"
      },
      "audio/midi": {
        source: "apache",
        extensions: ["mid", "midi", "kar", "rmi"]
      },
      "audio/mobile-xmf": {
        source: "iana",
        extensions: ["mxmf"]
      },
      "audio/mp3": {
        compressible: false,
        extensions: ["mp3"]
      },
      "audio/mp4": {
        source: "iana",
        compressible: false,
        extensions: ["m4a", "mp4a"]
      },
      "audio/mp4a-latm": {
        source: "iana"
      },
      "audio/mpa": {
        source: "iana"
      },
      "audio/mpa-robust": {
        source: "iana"
      },
      "audio/mpeg": {
        source: "iana",
        compressible: false,
        extensions: ["mpga", "mp2", "mp2a", "mp3", "m2a", "m3a"]
      },
      "audio/mpeg4-generic": {
        source: "iana"
      },
      "audio/musepack": {
        source: "apache"
      },
      "audio/ogg": {
        source: "iana",
        compressible: false,
        extensions: ["oga", "ogg", "spx", "opus"]
      },
      "audio/opus": {
        source: "iana"
      },
      "audio/parityfec": {
        source: "iana"
      },
      "audio/pcma": {
        source: "iana"
      },
      "audio/pcma-wb": {
        source: "iana"
      },
      "audio/pcmu": {
        source: "iana"
      },
      "audio/pcmu-wb": {
        source: "iana"
      },
      "audio/prs.sid": {
        source: "iana"
      },
      "audio/qcelp": {
        source: "iana"
      },
      "audio/raptorfec": {
        source: "iana"
      },
      "audio/red": {
        source: "iana"
      },
      "audio/rtp-enc-aescm128": {
        source: "iana"
      },
      "audio/rtp-midi": {
        source: "iana"
      },
      "audio/rtploopback": {
        source: "iana"
      },
      "audio/rtx": {
        source: "iana"
      },
      "audio/s3m": {
        source: "apache",
        extensions: ["s3m"]
      },
      "audio/scip": {
        source: "iana"
      },
      "audio/silk": {
        source: "apache",
        extensions: ["sil"]
      },
      "audio/smv": {
        source: "iana"
      },
      "audio/smv-qcp": {
        source: "iana"
      },
      "audio/smv0": {
        source: "iana"
      },
      "audio/sofa": {
        source: "iana"
      },
      "audio/sp-midi": {
        source: "iana"
      },
      "audio/speex": {
        source: "iana"
      },
      "audio/t140c": {
        source: "iana"
      },
      "audio/t38": {
        source: "iana"
      },
      "audio/telephone-event": {
        source: "iana"
      },
      "audio/tetra_acelp": {
        source: "iana"
      },
      "audio/tetra_acelp_bb": {
        source: "iana"
      },
      "audio/tone": {
        source: "iana"
      },
      "audio/tsvcis": {
        source: "iana"
      },
      "audio/uemclip": {
        source: "iana"
      },
      "audio/ulpfec": {
        source: "iana"
      },
      "audio/usac": {
        source: "iana"
      },
      "audio/vdvi": {
        source: "iana"
      },
      "audio/vmr-wb": {
        source: "iana"
      },
      "audio/vnd.3gpp.iufp": {
        source: "iana"
      },
      "audio/vnd.4sb": {
        source: "iana"
      },
      "audio/vnd.audiokoz": {
        source: "iana"
      },
      "audio/vnd.celp": {
        source: "iana"
      },
      "audio/vnd.cisco.nse": {
        source: "iana"
      },
      "audio/vnd.cmles.radio-events": {
        source: "iana"
      },
      "audio/vnd.cns.anp1": {
        source: "iana"
      },
      "audio/vnd.cns.inf1": {
        source: "iana"
      },
      "audio/vnd.dece.audio": {
        source: "iana",
        extensions: ["uva", "uvva"]
      },
      "audio/vnd.digital-winds": {
        source: "iana",
        extensions: ["eol"]
      },
      "audio/vnd.dlna.adts": {
        source: "iana"
      },
      "audio/vnd.dolby.heaac.1": {
        source: "iana"
      },
      "audio/vnd.dolby.heaac.2": {
        source: "iana"
      },
      "audio/vnd.dolby.mlp": {
        source: "iana"
      },
      "audio/vnd.dolby.mps": {
        source: "iana"
      },
      "audio/vnd.dolby.pl2": {
        source: "iana"
      },
      "audio/vnd.dolby.pl2x": {
        source: "iana"
      },
      "audio/vnd.dolby.pl2z": {
        source: "iana"
      },
      "audio/vnd.dolby.pulse.1": {
        source: "iana"
      },
      "audio/vnd.dra": {
        source: "iana",
        extensions: ["dra"]
      },
      "audio/vnd.dts": {
        source: "iana",
        extensions: ["dts"]
      },
      "audio/vnd.dts.hd": {
        source: "iana",
        extensions: ["dtshd"]
      },
      "audio/vnd.dts.uhd": {
        source: "iana"
      },
      "audio/vnd.dvb.file": {
        source: "iana"
      },
      "audio/vnd.everad.plj": {
        source: "iana"
      },
      "audio/vnd.hns.audio": {
        source: "iana"
      },
      "audio/vnd.lucent.voice": {
        source: "iana",
        extensions: ["lvp"]
      },
      "audio/vnd.ms-playready.media.pya": {
        source: "iana",
        extensions: ["pya"]
      },
      "audio/vnd.nokia.mobile-xmf": {
        source: "iana"
      },
      "audio/vnd.nortel.vbk": {
        source: "iana"
      },
      "audio/vnd.nuera.ecelp4800": {
        source: "iana",
        extensions: ["ecelp4800"]
      },
      "audio/vnd.nuera.ecelp7470": {
        source: "iana",
        extensions: ["ecelp7470"]
      },
      "audio/vnd.nuera.ecelp9600": {
        source: "iana",
        extensions: ["ecelp9600"]
      },
      "audio/vnd.octel.sbc": {
        source: "iana"
      },
      "audio/vnd.presonus.multitrack": {
        source: "iana"
      },
      "audio/vnd.qcelp": {
        source: "iana"
      },
      "audio/vnd.rhetorex.32kadpcm": {
        source: "iana"
      },
      "audio/vnd.rip": {
        source: "iana",
        extensions: ["rip"]
      },
      "audio/vnd.rn-realaudio": {
        compressible: false
      },
      "audio/vnd.sealedmedia.softseal.mpeg": {
        source: "iana"
      },
      "audio/vnd.vmx.cvsd": {
        source: "iana"
      },
      "audio/vnd.wave": {
        compressible: false
      },
      "audio/vorbis": {
        source: "iana",
        compressible: false
      },
      "audio/vorbis-config": {
        source: "iana"
      },
      "audio/wav": {
        compressible: false,
        extensions: ["wav"]
      },
      "audio/wave": {
        compressible: false,
        extensions: ["wav"]
      },
      "audio/webm": {
        source: "apache",
        compressible: false,
        extensions: ["weba"]
      },
      "audio/x-aac": {
        source: "apache",
        compressible: false,
        extensions: ["aac"]
      },
      "audio/x-aiff": {
        source: "apache",
        extensions: ["aif", "aiff", "aifc"]
      },
      "audio/x-caf": {
        source: "apache",
        compressible: false,
        extensions: ["caf"]
      },
      "audio/x-flac": {
        source: "apache",
        extensions: ["flac"]
      },
      "audio/x-m4a": {
        source: "nginx",
        extensions: ["m4a"]
      },
      "audio/x-matroska": {
        source: "apache",
        extensions: ["mka"]
      },
      "audio/x-mpegurl": {
        source: "apache",
        extensions: ["m3u"]
      },
      "audio/x-ms-wax": {
        source: "apache",
        extensions: ["wax"]
      },
      "audio/x-ms-wma": {
        source: "apache",
        extensions: ["wma"]
      },
      "audio/x-pn-realaudio": {
        source: "apache",
        extensions: ["ram", "ra"]
      },
      "audio/x-pn-realaudio-plugin": {
        source: "apache",
        extensions: ["rmp"]
      },
      "audio/x-realaudio": {
        source: "nginx",
        extensions: ["ra"]
      },
      "audio/x-tta": {
        source: "apache"
      },
      "audio/x-wav": {
        source: "apache",
        extensions: ["wav"]
      },
      "audio/xm": {
        source: "apache",
        extensions: ["xm"]
      },
      "chemical/x-cdx": {
        source: "apache",
        extensions: ["cdx"]
      },
      "chemical/x-cif": {
        source: "apache",
        extensions: ["cif"]
      },
      "chemical/x-cmdf": {
        source: "apache",
        extensions: ["cmdf"]
      },
      "chemical/x-cml": {
        source: "apache",
        extensions: ["cml"]
      },
      "chemical/x-csml": {
        source: "apache",
        extensions: ["csml"]
      },
      "chemical/x-pdb": {
        source: "apache"
      },
      "chemical/x-xyz": {
        source: "apache",
        extensions: ["xyz"]
      },
      "font/collection": {
        source: "iana",
        extensions: ["ttc"]
      },
      "font/otf": {
        source: "iana",
        compressible: true,
        extensions: ["otf"]
      },
      "font/sfnt": {
        source: "iana"
      },
      "font/ttf": {
        source: "iana",
        compressible: true,
        extensions: ["ttf"]
      },
      "font/woff": {
        source: "iana",
        extensions: ["woff"]
      },
      "font/woff2": {
        source: "iana",
        extensions: ["woff2"]
      },
      "image/aces": {
        source: "iana",
        extensions: ["exr"]
      },
      "image/apng": {
        compressible: false,
        extensions: ["apng"]
      },
      "image/avci": {
        source: "iana",
        extensions: ["avci"]
      },
      "image/avcs": {
        source: "iana",
        extensions: ["avcs"]
      },
      "image/avif": {
        source: "iana",
        compressible: false,
        extensions: ["avif"]
      },
      "image/bmp": {
        source: "iana",
        compressible: true,
        extensions: ["bmp"]
      },
      "image/cgm": {
        source: "iana",
        extensions: ["cgm"]
      },
      "image/dicom-rle": {
        source: "iana",
        extensions: ["drle"]
      },
      "image/emf": {
        source: "iana",
        extensions: ["emf"]
      },
      "image/fits": {
        source: "iana",
        extensions: ["fits"]
      },
      "image/g3fax": {
        source: "iana",
        extensions: ["g3"]
      },
      "image/gif": {
        source: "iana",
        compressible: false,
        extensions: ["gif"]
      },
      "image/heic": {
        source: "iana",
        extensions: ["heic"]
      },
      "image/heic-sequence": {
        source: "iana",
        extensions: ["heics"]
      },
      "image/heif": {
        source: "iana",
        extensions: ["heif"]
      },
      "image/heif-sequence": {
        source: "iana",
        extensions: ["heifs"]
      },
      "image/hej2k": {
        source: "iana",
        extensions: ["hej2"]
      },
      "image/hsj2": {
        source: "iana",
        extensions: ["hsj2"]
      },
      "image/ief": {
        source: "iana",
        extensions: ["ief"]
      },
      "image/jls": {
        source: "iana",
        extensions: ["jls"]
      },
      "image/jp2": {
        source: "iana",
        compressible: false,
        extensions: ["jp2", "jpg2"]
      },
      "image/jpeg": {
        source: "iana",
        compressible: false,
        extensions: ["jpeg", "jpg", "jpe"]
      },
      "image/jph": {
        source: "iana",
        extensions: ["jph"]
      },
      "image/jphc": {
        source: "iana",
        extensions: ["jhc"]
      },
      "image/jpm": {
        source: "iana",
        compressible: false,
        extensions: ["jpm"]
      },
      "image/jpx": {
        source: "iana",
        compressible: false,
        extensions: ["jpx", "jpf"]
      },
      "image/jxr": {
        source: "iana",
        extensions: ["jxr"]
      },
      "image/jxra": {
        source: "iana",
        extensions: ["jxra"]
      },
      "image/jxrs": {
        source: "iana",
        extensions: ["jxrs"]
      },
      "image/jxs": {
        source: "iana",
        extensions: ["jxs"]
      },
      "image/jxsc": {
        source: "iana",
        extensions: ["jxsc"]
      },
      "image/jxsi": {
        source: "iana",
        extensions: ["jxsi"]
      },
      "image/jxss": {
        source: "iana",
        extensions: ["jxss"]
      },
      "image/ktx": {
        source: "iana",
        extensions: ["ktx"]
      },
      "image/ktx2": {
        source: "iana",
        extensions: ["ktx2"]
      },
      "image/naplps": {
        source: "iana"
      },
      "image/pjpeg": {
        compressible: false
      },
      "image/png": {
        source: "iana",
        compressible: false,
        extensions: ["png"]
      },
      "image/prs.btif": {
        source: "iana",
        extensions: ["btif"]
      },
      "image/prs.pti": {
        source: "iana",
        extensions: ["pti"]
      },
      "image/pwg-raster": {
        source: "iana"
      },
      "image/sgi": {
        source: "apache",
        extensions: ["sgi"]
      },
      "image/svg+xml": {
        source: "iana",
        compressible: true,
        extensions: ["svg", "svgz"]
      },
      "image/t38": {
        source: "iana",
        extensions: ["t38"]
      },
      "image/tiff": {
        source: "iana",
        compressible: false,
        extensions: ["tif", "tiff"]
      },
      "image/tiff-fx": {
        source: "iana",
        extensions: ["tfx"]
      },
      "image/vnd.adobe.photoshop": {
        source: "iana",
        compressible: true,
        extensions: ["psd"]
      },
      "image/vnd.airzip.accelerator.azv": {
        source: "iana",
        extensions: ["azv"]
      },
      "image/vnd.cns.inf2": {
        source: "iana"
      },
      "image/vnd.dece.graphic": {
        source: "iana",
        extensions: ["uvi", "uvvi", "uvg", "uvvg"]
      },
      "image/vnd.djvu": {
        source: "iana",
        extensions: ["djvu", "djv"]
      },
      "image/vnd.dvb.subtitle": {
        source: "iana",
        extensions: ["sub"]
      },
      "image/vnd.dwg": {
        source: "iana",
        extensions: ["dwg"]
      },
      "image/vnd.dxf": {
        source: "iana",
        extensions: ["dxf"]
      },
      "image/vnd.fastbidsheet": {
        source: "iana",
        extensions: ["fbs"]
      },
      "image/vnd.fpx": {
        source: "iana",
        extensions: ["fpx"]
      },
      "image/vnd.fst": {
        source: "iana",
        extensions: ["fst"]
      },
      "image/vnd.fujixerox.edmics-mmr": {
        source: "iana",
        extensions: ["mmr"]
      },
      "image/vnd.fujixerox.edmics-rlc": {
        source: "iana",
        extensions: ["rlc"]
      },
      "image/vnd.globalgraphics.pgb": {
        source: "iana"
      },
      "image/vnd.microsoft.icon": {
        source: "iana",
        compressible: true,
        extensions: ["ico"]
      },
      "image/vnd.mix": {
        source: "iana"
      },
      "image/vnd.mozilla.apng": {
        source: "iana"
      },
      "image/vnd.ms-dds": {
        compressible: true,
        extensions: ["dds"]
      },
      "image/vnd.ms-modi": {
        source: "iana",
        extensions: ["mdi"]
      },
      "image/vnd.ms-photo": {
        source: "apache",
        extensions: ["wdp"]
      },
      "image/vnd.net-fpx": {
        source: "iana",
        extensions: ["npx"]
      },
      "image/vnd.pco.b16": {
        source: "iana",
        extensions: ["b16"]
      },
      "image/vnd.radiance": {
        source: "iana"
      },
      "image/vnd.sealed.png": {
        source: "iana"
      },
      "image/vnd.sealedmedia.softseal.gif": {
        source: "iana"
      },
      "image/vnd.sealedmedia.softseal.jpg": {
        source: "iana"
      },
      "image/vnd.svf": {
        source: "iana"
      },
      "image/vnd.tencent.tap": {
        source: "iana",
        extensions: ["tap"]
      },
      "image/vnd.valve.source.texture": {
        source: "iana",
        extensions: ["vtf"]
      },
      "image/vnd.wap.wbmp": {
        source: "iana",
        extensions: ["wbmp"]
      },
      "image/vnd.xiff": {
        source: "iana",
        extensions: ["xif"]
      },
      "image/vnd.zbrush.pcx": {
        source: "iana",
        extensions: ["pcx"]
      },
      "image/webp": {
        source: "apache",
        extensions: ["webp"]
      },
      "image/wmf": {
        source: "iana",
        extensions: ["wmf"]
      },
      "image/x-3ds": {
        source: "apache",
        extensions: ["3ds"]
      },
      "image/x-cmu-raster": {
        source: "apache",
        extensions: ["ras"]
      },
      "image/x-cmx": {
        source: "apache",
        extensions: ["cmx"]
      },
      "image/x-freehand": {
        source: "apache",
        extensions: ["fh", "fhc", "fh4", "fh5", "fh7"]
      },
      "image/x-icon": {
        source: "apache",
        compressible: true,
        extensions: ["ico"]
      },
      "image/x-jng": {
        source: "nginx",
        extensions: ["jng"]
      },
      "image/x-mrsid-image": {
        source: "apache",
        extensions: ["sid"]
      },
      "image/x-ms-bmp": {
        source: "nginx",
        compressible: true,
        extensions: ["bmp"]
      },
      "image/x-pcx": {
        source: "apache",
        extensions: ["pcx"]
      },
      "image/x-pict": {
        source: "apache",
        extensions: ["pic", "pct"]
      },
      "image/x-portable-anymap": {
        source: "apache",
        extensions: ["pnm"]
      },
      "image/x-portable-bitmap": {
        source: "apache",
        extensions: ["pbm"]
      },
      "image/x-portable-graymap": {
        source: "apache",
        extensions: ["pgm"]
      },
      "image/x-portable-pixmap": {
        source: "apache",
        extensions: ["ppm"]
      },
      "image/x-rgb": {
        source: "apache",
        extensions: ["rgb"]
      },
      "image/x-tga": {
        source: "apache",
        extensions: ["tga"]
      },
      "image/x-xbitmap": {
        source: "apache",
        extensions: ["xbm"]
      },
      "image/x-xcf": {
        compressible: false
      },
      "image/x-xpixmap": {
        source: "apache",
        extensions: ["xpm"]
      },
      "image/x-xwindowdump": {
        source: "apache",
        extensions: ["xwd"]
      },
      "message/cpim": {
        source: "iana"
      },
      "message/delivery-status": {
        source: "iana"
      },
      "message/disposition-notification": {
        source: "iana",
        extensions: [
          "disposition-notification"
        ]
      },
      "message/external-body": {
        source: "iana"
      },
      "message/feedback-report": {
        source: "iana"
      },
      "message/global": {
        source: "iana",
        extensions: ["u8msg"]
      },
      "message/global-delivery-status": {
        source: "iana",
        extensions: ["u8dsn"]
      },
      "message/global-disposition-notification": {
        source: "iana",
        extensions: ["u8mdn"]
      },
      "message/global-headers": {
        source: "iana",
        extensions: ["u8hdr"]
      },
      "message/http": {
        source: "iana",
        compressible: false
      },
      "message/imdn+xml": {
        source: "iana",
        compressible: true
      },
      "message/news": {
        source: "iana"
      },
      "message/partial": {
        source: "iana",
        compressible: false
      },
      "message/rfc822": {
        source: "iana",
        compressible: true,
        extensions: ["eml", "mime"]
      },
      "message/s-http": {
        source: "iana"
      },
      "message/sip": {
        source: "iana"
      },
      "message/sipfrag": {
        source: "iana"
      },
      "message/tracking-status": {
        source: "iana"
      },
      "message/vnd.si.simp": {
        source: "iana"
      },
      "message/vnd.wfa.wsc": {
        source: "iana",
        extensions: ["wsc"]
      },
      "model/3mf": {
        source: "iana",
        extensions: ["3mf"]
      },
      "model/e57": {
        source: "iana"
      },
      "model/gltf+json": {
        source: "iana",
        compressible: true,
        extensions: ["gltf"]
      },
      "model/gltf-binary": {
        source: "iana",
        compressible: true,
        extensions: ["glb"]
      },
      "model/iges": {
        source: "iana",
        compressible: false,
        extensions: ["igs", "iges"]
      },
      "model/mesh": {
        source: "iana",
        compressible: false,
        extensions: ["msh", "mesh", "silo"]
      },
      "model/mtl": {
        source: "iana",
        extensions: ["mtl"]
      },
      "model/obj": {
        source: "iana",
        extensions: ["obj"]
      },
      "model/step": {
        source: "iana"
      },
      "model/step+xml": {
        source: "iana",
        compressible: true,
        extensions: ["stpx"]
      },
      "model/step+zip": {
        source: "iana",
        compressible: false,
        extensions: ["stpz"]
      },
      "model/step-xml+zip": {
        source: "iana",
        compressible: false,
        extensions: ["stpxz"]
      },
      "model/stl": {
        source: "iana",
        extensions: ["stl"]
      },
      "model/vnd.collada+xml": {
        source: "iana",
        compressible: true,
        extensions: ["dae"]
      },
      "model/vnd.dwf": {
        source: "iana",
        extensions: ["dwf"]
      },
      "model/vnd.flatland.3dml": {
        source: "iana"
      },
      "model/vnd.gdl": {
        source: "iana",
        extensions: ["gdl"]
      },
      "model/vnd.gs-gdl": {
        source: "apache"
      },
      "model/vnd.gs.gdl": {
        source: "iana"
      },
      "model/vnd.gtw": {
        source: "iana",
        extensions: ["gtw"]
      },
      "model/vnd.moml+xml": {
        source: "iana",
        compressible: true
      },
      "model/vnd.mts": {
        source: "iana",
        extensions: ["mts"]
      },
      "model/vnd.opengex": {
        source: "iana",
        extensions: ["ogex"]
      },
      "model/vnd.parasolid.transmit.binary": {
        source: "iana",
        extensions: ["x_b"]
      },
      "model/vnd.parasolid.transmit.text": {
        source: "iana",
        extensions: ["x_t"]
      },
      "model/vnd.pytha.pyox": {
        source: "iana"
      },
      "model/vnd.rosette.annotated-data-model": {
        source: "iana"
      },
      "model/vnd.sap.vds": {
        source: "iana",
        extensions: ["vds"]
      },
      "model/vnd.usdz+zip": {
        source: "iana",
        compressible: false,
        extensions: ["usdz"]
      },
      "model/vnd.valve.source.compiled-map": {
        source: "iana",
        extensions: ["bsp"]
      },
      "model/vnd.vtu": {
        source: "iana",
        extensions: ["vtu"]
      },
      "model/vrml": {
        source: "iana",
        compressible: false,
        extensions: ["wrl", "vrml"]
      },
      "model/x3d+binary": {
        source: "apache",
        compressible: false,
        extensions: ["x3db", "x3dbz"]
      },
      "model/x3d+fastinfoset": {
        source: "iana",
        extensions: ["x3db"]
      },
      "model/x3d+vrml": {
        source: "apache",
        compressible: false,
        extensions: ["x3dv", "x3dvz"]
      },
      "model/x3d+xml": {
        source: "iana",
        compressible: true,
        extensions: ["x3d", "x3dz"]
      },
      "model/x3d-vrml": {
        source: "iana",
        extensions: ["x3dv"]
      },
      "multipart/alternative": {
        source: "iana",
        compressible: false
      },
      "multipart/appledouble": {
        source: "iana"
      },
      "multipart/byteranges": {
        source: "iana"
      },
      "multipart/digest": {
        source: "iana"
      },
      "multipart/encrypted": {
        source: "iana",
        compressible: false
      },
      "multipart/form-data": {
        source: "iana",
        compressible: false
      },
      "multipart/header-set": {
        source: "iana"
      },
      "multipart/mixed": {
        source: "iana"
      },
      "multipart/multilingual": {
        source: "iana"
      },
      "multipart/parallel": {
        source: "iana"
      },
      "multipart/related": {
        source: "iana",
        compressible: false
      },
      "multipart/report": {
        source: "iana"
      },
      "multipart/signed": {
        source: "iana",
        compressible: false
      },
      "multipart/vnd.bint.med-plus": {
        source: "iana"
      },
      "multipart/voice-message": {
        source: "iana"
      },
      "multipart/x-mixed-replace": {
        source: "iana"
      },
      "text/1d-interleaved-parityfec": {
        source: "iana"
      },
      "text/cache-manifest": {
        source: "iana",
        compressible: true,
        extensions: ["appcache", "manifest"]
      },
      "text/calendar": {
        source: "iana",
        extensions: ["ics", "ifb"]
      },
      "text/calender": {
        compressible: true
      },
      "text/cmd": {
        compressible: true
      },
      "text/coffeescript": {
        extensions: ["coffee", "litcoffee"]
      },
      "text/cql": {
        source: "iana"
      },
      "text/cql-expression": {
        source: "iana"
      },
      "text/cql-identifier": {
        source: "iana"
      },
      "text/css": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["css"]
      },
      "text/csv": {
        source: "iana",
        compressible: true,
        extensions: ["csv"]
      },
      "text/csv-schema": {
        source: "iana"
      },
      "text/directory": {
        source: "iana"
      },
      "text/dns": {
        source: "iana"
      },
      "text/ecmascript": {
        source: "iana"
      },
      "text/encaprtp": {
        source: "iana"
      },
      "text/enriched": {
        source: "iana"
      },
      "text/fhirpath": {
        source: "iana"
      },
      "text/flexfec": {
        source: "iana"
      },
      "text/fwdred": {
        source: "iana"
      },
      "text/gff3": {
        source: "iana"
      },
      "text/grammar-ref-list": {
        source: "iana"
      },
      "text/html": {
        source: "iana",
        compressible: true,
        extensions: ["html", "htm", "shtml"]
      },
      "text/jade": {
        extensions: ["jade"]
      },
      "text/javascript": {
        source: "iana",
        compressible: true
      },
      "text/jcr-cnd": {
        source: "iana"
      },
      "text/jsx": {
        compressible: true,
        extensions: ["jsx"]
      },
      "text/less": {
        compressible: true,
        extensions: ["less"]
      },
      "text/markdown": {
        source: "iana",
        compressible: true,
        extensions: ["markdown", "md"]
      },
      "text/mathml": {
        source: "nginx",
        extensions: ["mml"]
      },
      "text/mdx": {
        compressible: true,
        extensions: ["mdx"]
      },
      "text/mizar": {
        source: "iana"
      },
      "text/n3": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["n3"]
      },
      "text/parameters": {
        source: "iana",
        charset: "UTF-8"
      },
      "text/parityfec": {
        source: "iana"
      },
      "text/plain": {
        source: "iana",
        compressible: true,
        extensions: ["txt", "text", "conf", "def", "list", "log", "in", "ini"]
      },
      "text/provenance-notation": {
        source: "iana",
        charset: "UTF-8"
      },
      "text/prs.fallenstein.rst": {
        source: "iana"
      },
      "text/prs.lines.tag": {
        source: "iana",
        extensions: ["dsc"]
      },
      "text/prs.prop.logic": {
        source: "iana"
      },
      "text/raptorfec": {
        source: "iana"
      },
      "text/red": {
        source: "iana"
      },
      "text/rfc822-headers": {
        source: "iana"
      },
      "text/richtext": {
        source: "iana",
        compressible: true,
        extensions: ["rtx"]
      },
      "text/rtf": {
        source: "iana",
        compressible: true,
        extensions: ["rtf"]
      },
      "text/rtp-enc-aescm128": {
        source: "iana"
      },
      "text/rtploopback": {
        source: "iana"
      },
      "text/rtx": {
        source: "iana"
      },
      "text/sgml": {
        source: "iana",
        extensions: ["sgml", "sgm"]
      },
      "text/shaclc": {
        source: "iana"
      },
      "text/shex": {
        source: "iana",
        extensions: ["shex"]
      },
      "text/slim": {
        extensions: ["slim", "slm"]
      },
      "text/spdx": {
        source: "iana",
        extensions: ["spdx"]
      },
      "text/strings": {
        source: "iana"
      },
      "text/stylus": {
        extensions: ["stylus", "styl"]
      },
      "text/t140": {
        source: "iana"
      },
      "text/tab-separated-values": {
        source: "iana",
        compressible: true,
        extensions: ["tsv"]
      },
      "text/troff": {
        source: "iana",
        extensions: ["t", "tr", "roff", "man", "me", "ms"]
      },
      "text/turtle": {
        source: "iana",
        charset: "UTF-8",
        extensions: ["ttl"]
      },
      "text/ulpfec": {
        source: "iana"
      },
      "text/uri-list": {
        source: "iana",
        compressible: true,
        extensions: ["uri", "uris", "urls"]
      },
      "text/vcard": {
        source: "iana",
        compressible: true,
        extensions: ["vcard"]
      },
      "text/vnd.a": {
        source: "iana"
      },
      "text/vnd.abc": {
        source: "iana"
      },
      "text/vnd.ascii-art": {
        source: "iana"
      },
      "text/vnd.curl": {
        source: "iana",
        extensions: ["curl"]
      },
      "text/vnd.curl.dcurl": {
        source: "apache",
        extensions: ["dcurl"]
      },
      "text/vnd.curl.mcurl": {
        source: "apache",
        extensions: ["mcurl"]
      },
      "text/vnd.curl.scurl": {
        source: "apache",
        extensions: ["scurl"]
      },
      "text/vnd.debian.copyright": {
        source: "iana",
        charset: "UTF-8"
      },
      "text/vnd.dmclientscript": {
        source: "iana"
      },
      "text/vnd.dvb.subtitle": {
        source: "iana",
        extensions: ["sub"]
      },
      "text/vnd.esmertec.theme-descriptor": {
        source: "iana",
        charset: "UTF-8"
      },
      "text/vnd.familysearch.gedcom": {
        source: "iana",
        extensions: ["ged"]
      },
      "text/vnd.ficlab.flt": {
        source: "iana"
      },
      "text/vnd.fly": {
        source: "iana",
        extensions: ["fly"]
      },
      "text/vnd.fmi.flexstor": {
        source: "iana",
        extensions: ["flx"]
      },
      "text/vnd.gml": {
        source: "iana"
      },
      "text/vnd.graphviz": {
        source: "iana",
        extensions: ["gv"]
      },
      "text/vnd.hans": {
        source: "iana"
      },
      "text/vnd.hgl": {
        source: "iana"
      },
      "text/vnd.in3d.3dml": {
        source: "iana",
        extensions: ["3dml"]
      },
      "text/vnd.in3d.spot": {
        source: "iana",
        extensions: ["spot"]
      },
      "text/vnd.iptc.newsml": {
        source: "iana"
      },
      "text/vnd.iptc.nitf": {
        source: "iana"
      },
      "text/vnd.latex-z": {
        source: "iana"
      },
      "text/vnd.motorola.reflex": {
        source: "iana"
      },
      "text/vnd.ms-mediapackage": {
        source: "iana"
      },
      "text/vnd.net2phone.commcenter.command": {
        source: "iana"
      },
      "text/vnd.radisys.msml-basic-layout": {
        source: "iana"
      },
      "text/vnd.senx.warpscript": {
        source: "iana"
      },
      "text/vnd.si.uricatalogue": {
        source: "iana"
      },
      "text/vnd.sosi": {
        source: "iana"
      },
      "text/vnd.sun.j2me.app-descriptor": {
        source: "iana",
        charset: "UTF-8",
        extensions: ["jad"]
      },
      "text/vnd.trolltech.linguist": {
        source: "iana",
        charset: "UTF-8"
      },
      "text/vnd.wap.si": {
        source: "iana"
      },
      "text/vnd.wap.sl": {
        source: "iana"
      },
      "text/vnd.wap.wml": {
        source: "iana",
        extensions: ["wml"]
      },
      "text/vnd.wap.wmlscript": {
        source: "iana",
        extensions: ["wmls"]
      },
      "text/vtt": {
        source: "iana",
        charset: "UTF-8",
        compressible: true,
        extensions: ["vtt"]
      },
      "text/x-asm": {
        source: "apache",
        extensions: ["s", "asm"]
      },
      "text/x-c": {
        source: "apache",
        extensions: ["c", "cc", "cxx", "cpp", "h", "hh", "dic"]
      },
      "text/x-component": {
        source: "nginx",
        extensions: ["htc"]
      },
      "text/x-fortran": {
        source: "apache",
        extensions: ["f", "for", "f77", "f90"]
      },
      "text/x-gwt-rpc": {
        compressible: true
      },
      "text/x-handlebars-template": {
        extensions: ["hbs"]
      },
      "text/x-java-source": {
        source: "apache",
        extensions: ["java"]
      },
      "text/x-jquery-tmpl": {
        compressible: true
      },
      "text/x-lua": {
        extensions: ["lua"]
      },
      "text/x-markdown": {
        compressible: true,
        extensions: ["mkd"]
      },
      "text/x-nfo": {
        source: "apache",
        extensions: ["nfo"]
      },
      "text/x-opml": {
        source: "apache",
        extensions: ["opml"]
      },
      "text/x-org": {
        compressible: true,
        extensions: ["org"]
      },
      "text/x-pascal": {
        source: "apache",
        extensions: ["p", "pas"]
      },
      "text/x-processing": {
        compressible: true,
        extensions: ["pde"]
      },
      "text/x-sass": {
        extensions: ["sass"]
      },
      "text/x-scss": {
        extensions: ["scss"]
      },
      "text/x-setext": {
        source: "apache",
        extensions: ["etx"]
      },
      "text/x-sfv": {
        source: "apache",
        extensions: ["sfv"]
      },
      "text/x-suse-ymp": {
        compressible: true,
        extensions: ["ymp"]
      },
      "text/x-uuencode": {
        source: "apache",
        extensions: ["uu"]
      },
      "text/x-vcalendar": {
        source: "apache",
        extensions: ["vcs"]
      },
      "text/x-vcard": {
        source: "apache",
        extensions: ["vcf"]
      },
      "text/xml": {
        source: "iana",
        compressible: true,
        extensions: ["xml"]
      },
      "text/xml-external-parsed-entity": {
        source: "iana"
      },
      "text/yaml": {
        compressible: true,
        extensions: ["yaml", "yml"]
      },
      "video/1d-interleaved-parityfec": {
        source: "iana"
      },
      "video/3gpp": {
        source: "iana",
        extensions: ["3gp", "3gpp"]
      },
      "video/3gpp-tt": {
        source: "iana"
      },
      "video/3gpp2": {
        source: "iana",
        extensions: ["3g2"]
      },
      "video/av1": {
        source: "iana"
      },
      "video/bmpeg": {
        source: "iana"
      },
      "video/bt656": {
        source: "iana"
      },
      "video/celb": {
        source: "iana"
      },
      "video/dv": {
        source: "iana"
      },
      "video/encaprtp": {
        source: "iana"
      },
      "video/ffv1": {
        source: "iana"
      },
      "video/flexfec": {
        source: "iana"
      },
      "video/h261": {
        source: "iana",
        extensions: ["h261"]
      },
      "video/h263": {
        source: "iana",
        extensions: ["h263"]
      },
      "video/h263-1998": {
        source: "iana"
      },
      "video/h263-2000": {
        source: "iana"
      },
      "video/h264": {
        source: "iana",
        extensions: ["h264"]
      },
      "video/h264-rcdo": {
        source: "iana"
      },
      "video/h264-svc": {
        source: "iana"
      },
      "video/h265": {
        source: "iana"
      },
      "video/iso.segment": {
        source: "iana",
        extensions: ["m4s"]
      },
      "video/jpeg": {
        source: "iana",
        extensions: ["jpgv"]
      },
      "video/jpeg2000": {
        source: "iana"
      },
      "video/jpm": {
        source: "apache",
        extensions: ["jpm", "jpgm"]
      },
      "video/jxsv": {
        source: "iana"
      },
      "video/mj2": {
        source: "iana",
        extensions: ["mj2", "mjp2"]
      },
      "video/mp1s": {
        source: "iana"
      },
      "video/mp2p": {
        source: "iana"
      },
      "video/mp2t": {
        source: "iana",
        extensions: ["ts"]
      },
      "video/mp4": {
        source: "iana",
        compressible: false,
        extensions: ["mp4", "mp4v", "mpg4"]
      },
      "video/mp4v-es": {
        source: "iana"
      },
      "video/mpeg": {
        source: "iana",
        compressible: false,
        extensions: ["mpeg", "mpg", "mpe", "m1v", "m2v"]
      },
      "video/mpeg4-generic": {
        source: "iana"
      },
      "video/mpv": {
        source: "iana"
      },
      "video/nv": {
        source: "iana"
      },
      "video/ogg": {
        source: "iana",
        compressible: false,
        extensions: ["ogv"]
      },
      "video/parityfec": {
        source: "iana"
      },
      "video/pointer": {
        source: "iana"
      },
      "video/quicktime": {
        source: "iana",
        compressible: false,
        extensions: ["qt", "mov"]
      },
      "video/raptorfec": {
        source: "iana"
      },
      "video/raw": {
        source: "iana"
      },
      "video/rtp-enc-aescm128": {
        source: "iana"
      },
      "video/rtploopback": {
        source: "iana"
      },
      "video/rtx": {
        source: "iana"
      },
      "video/scip": {
        source: "iana"
      },
      "video/smpte291": {
        source: "iana"
      },
      "video/smpte292m": {
        source: "iana"
      },
      "video/ulpfec": {
        source: "iana"
      },
      "video/vc1": {
        source: "iana"
      },
      "video/vc2": {
        source: "iana"
      },
      "video/vnd.cctv": {
        source: "iana"
      },
      "video/vnd.dece.hd": {
        source: "iana",
        extensions: ["uvh", "uvvh"]
      },
      "video/vnd.dece.mobile": {
        source: "iana",
        extensions: ["uvm", "uvvm"]
      },
      "video/vnd.dece.mp4": {
        source: "iana"
      },
      "video/vnd.dece.pd": {
        source: "iana",
        extensions: ["uvp", "uvvp"]
      },
      "video/vnd.dece.sd": {
        source: "iana",
        extensions: ["uvs", "uvvs"]
      },
      "video/vnd.dece.video": {
        source: "iana",
        extensions: ["uvv", "uvvv"]
      },
      "video/vnd.directv.mpeg": {
        source: "iana"
      },
      "video/vnd.directv.mpeg-tts": {
        source: "iana"
      },
      "video/vnd.dlna.mpeg-tts": {
        source: "iana"
      },
      "video/vnd.dvb.file": {
        source: "iana",
        extensions: ["dvb"]
      },
      "video/vnd.fvt": {
        source: "iana",
        extensions: ["fvt"]
      },
      "video/vnd.hns.video": {
        source: "iana"
      },
      "video/vnd.iptvforum.1dparityfec-1010": {
        source: "iana"
      },
      "video/vnd.iptvforum.1dparityfec-2005": {
        source: "iana"
      },
      "video/vnd.iptvforum.2dparityfec-1010": {
        source: "iana"
      },
      "video/vnd.iptvforum.2dparityfec-2005": {
        source: "iana"
      },
      "video/vnd.iptvforum.ttsavc": {
        source: "iana"
      },
      "video/vnd.iptvforum.ttsmpeg2": {
        source: "iana"
      },
      "video/vnd.motorola.video": {
        source: "iana"
      },
      "video/vnd.motorola.videop": {
        source: "iana"
      },
      "video/vnd.mpegurl": {
        source: "iana",
        extensions: ["mxu", "m4u"]
      },
      "video/vnd.ms-playready.media.pyv": {
        source: "iana",
        extensions: ["pyv"]
      },
      "video/vnd.nokia.interleaved-multimedia": {
        source: "iana"
      },
      "video/vnd.nokia.mp4vr": {
        source: "iana"
      },
      "video/vnd.nokia.videovoip": {
        source: "iana"
      },
      "video/vnd.objectvideo": {
        source: "iana"
      },
      "video/vnd.radgamettools.bink": {
        source: "iana"
      },
      "video/vnd.radgamettools.smacker": {
        source: "iana"
      },
      "video/vnd.sealed.mpeg1": {
        source: "iana"
      },
      "video/vnd.sealed.mpeg4": {
        source: "iana"
      },
      "video/vnd.sealed.swf": {
        source: "iana"
      },
      "video/vnd.sealedmedia.softseal.mov": {
        source: "iana"
      },
      "video/vnd.uvvu.mp4": {
        source: "iana",
        extensions: ["uvu", "uvvu"]
      },
      "video/vnd.vivo": {
        source: "iana",
        extensions: ["viv"]
      },
      "video/vnd.youtube.yt": {
        source: "iana"
      },
      "video/vp8": {
        source: "iana"
      },
      "video/vp9": {
        source: "iana"
      },
      "video/webm": {
        source: "apache",
        compressible: false,
        extensions: ["webm"]
      },
      "video/x-f4v": {
        source: "apache",
        extensions: ["f4v"]
      },
      "video/x-fli": {
        source: "apache",
        extensions: ["fli"]
      },
      "video/x-flv": {
        source: "apache",
        compressible: false,
        extensions: ["flv"]
      },
      "video/x-m4v": {
        source: "apache",
        extensions: ["m4v"]
      },
      "video/x-matroska": {
        source: "apache",
        compressible: false,
        extensions: ["mkv", "mk3d", "mks"]
      },
      "video/x-mng": {
        source: "apache",
        extensions: ["mng"]
      },
      "video/x-ms-asf": {
        source: "apache",
        extensions: ["asf", "asx"]
      },
      "video/x-ms-vob": {
        source: "apache",
        extensions: ["vob"]
      },
      "video/x-ms-wm": {
        source: "apache",
        extensions: ["wm"]
      },
      "video/x-ms-wmv": {
        source: "apache",
        compressible: false,
        extensions: ["wmv"]
      },
      "video/x-ms-wmx": {
        source: "apache",
        extensions: ["wmx"]
      },
      "video/x-ms-wvx": {
        source: "apache",
        extensions: ["wvx"]
      },
      "video/x-msvideo": {
        source: "apache",
        extensions: ["avi"]
      },
      "video/x-sgi-movie": {
        source: "apache",
        extensions: ["movie"]
      },
      "video/x-smv": {
        source: "apache",
        extensions: ["smv"]
      },
      "x-conference/x-cooltalk": {
        source: "apache",
        extensions: ["ice"]
      },
      "x-shader/x-fragment": {
        compressible: true
      },
      "x-shader/x-vertex": {
        compressible: true
      }
    };
  }
});

// ../../node_modules/.pnpm/mime-db@1.52.0/node_modules/mime-db/index.js
var require_mime_db = __commonJS({
  "../../node_modules/.pnpm/mime-db@1.52.0/node_modules/mime-db/index.js"(exports2, module2) {
    module2.exports = require_db();
  }
});

// ../../node_modules/.pnpm/mime-types@2.1.35/node_modules/mime-types/index.js
var require_mime_types = __commonJS({
  "../../node_modules/.pnpm/mime-types@2.1.35/node_modules/mime-types/index.js"(exports2) {
    "use strict";
    var db = require_mime_db();
    var extname4 = require("path").extname;
    var EXTRACT_TYPE_REGEXP = /^\s*([^;\s]*)(?:;|\s|$)/;
    var TEXT_TYPE_REGEXP = /^text\//i;
    exports2.charset = charset;
    exports2.charsets = { lookup: charset };
    exports2.contentType = contentType;
    exports2.extension = extension;
    exports2.extensions = /* @__PURE__ */ Object.create(null);
    exports2.lookup = lookup2;
    exports2.types = /* @__PURE__ */ Object.create(null);
    populateMaps(exports2.extensions, exports2.types);
    function charset(type) {
      if (!type || typeof type !== "string") {
        return false;
      }
      var match = EXTRACT_TYPE_REGEXP.exec(type);
      var mime = match && db[match[1].toLowerCase()];
      if (mime && mime.charset) {
        return mime.charset;
      }
      if (match && TEXT_TYPE_REGEXP.test(match[1])) {
        return "UTF-8";
      }
      return false;
    }
    function contentType(str) {
      if (!str || typeof str !== "string") {
        return false;
      }
      var mime = str.indexOf("/") === -1 ? exports2.lookup(str) : str;
      if (!mime) {
        return false;
      }
      if (mime.indexOf("charset") === -1) {
        var charset2 = exports2.charset(mime);
        if (charset2) mime += "; charset=" + charset2.toLowerCase();
      }
      return mime;
    }
    function extension(type) {
      if (!type || typeof type !== "string") {
        return false;
      }
      var match = EXTRACT_TYPE_REGEXP.exec(type);
      var exts = match && exports2.extensions[match[1].toLowerCase()];
      if (!exts || !exts.length) {
        return false;
      }
      return exts[0];
    }
    function lookup2(path8) {
      if (!path8 || typeof path8 !== "string") {
        return false;
      }
      var extension2 = extname4("x." + path8).toLowerCase().substr(1);
      if (!extension2) {
        return false;
      }
      return exports2.types[extension2] || false;
    }
    function populateMaps(extensions, types) {
      var preference = ["nginx", "apache", void 0, "iana"];
      Object.keys(db).forEach(function forEachMimeType(type) {
        var mime = db[type];
        var exts = mime.extensions;
        if (!exts || !exts.length) {
          return;
        }
        extensions[type] = exts;
        for (var i = 0; i < exts.length; i++) {
          var extension2 = exts[i];
          if (types[extension2]) {
            var from = preference.indexOf(db[types[extension2]].source);
            var to = preference.indexOf(mime.source);
            if (types[extension2] !== "application/octet-stream" && (from > to || from === to && types[extension2].substr(0, 12) === "application/")) {
              continue;
            }
          }
          types[extension2] = type;
        }
      });
    }
  }
});

// ../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/defer.js
var require_defer = __commonJS({
  "../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/defer.js"(exports2, module2) {
    module2.exports = defer;
    function defer(fn) {
      var nextTick = typeof setImmediate == "function" ? setImmediate : typeof process == "object" && typeof process.nextTick == "function" ? process.nextTick : null;
      if (nextTick) {
        nextTick(fn);
      } else {
        setTimeout(fn, 0);
      }
    }
  }
});

// ../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/async.js
var require_async = __commonJS({
  "../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/async.js"(exports2, module2) {
    var defer = require_defer();
    module2.exports = async;
    function async(callback) {
      var isAsync = false;
      defer(function() {
        isAsync = true;
      });
      return function async_callback(err, result) {
        if (isAsync) {
          callback(err, result);
        } else {
          defer(function nextTick_callback() {
            callback(err, result);
          });
        }
      };
    }
  }
});

// ../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/abort.js
var require_abort = __commonJS({
  "../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/abort.js"(exports2, module2) {
    module2.exports = abort;
    function abort(state) {
      Object.keys(state.jobs).forEach(clean.bind(state));
      state.jobs = {};
    }
    function clean(key) {
      if (typeof this.jobs[key] == "function") {
        this.jobs[key]();
      }
    }
  }
});

// ../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/iterate.js
var require_iterate = __commonJS({
  "../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/iterate.js"(exports2, module2) {
    var async = require_async();
    var abort = require_abort();
    module2.exports = iterate;
    function iterate(list, iterator2, state, callback) {
      var key = state["keyedList"] ? state["keyedList"][state.index] : state.index;
      state.jobs[key] = runJob(iterator2, key, list[key], function(error, output) {
        if (!(key in state.jobs)) {
          return;
        }
        delete state.jobs[key];
        if (error) {
          abort(state);
        } else {
          state.results[key] = output;
        }
        callback(error, state.results);
      });
    }
    function runJob(iterator2, key, item, callback) {
      var aborter;
      if (iterator2.length == 2) {
        aborter = iterator2(item, async(callback));
      } else {
        aborter = iterator2(item, key, async(callback));
      }
      return aborter;
    }
  }
});

// ../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/state.js
var require_state = __commonJS({
  "../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/state.js"(exports2, module2) {
    module2.exports = state;
    function state(list, sortMethod) {
      var isNamedList = !Array.isArray(list), initState = {
        index: 0,
        keyedList: isNamedList || sortMethod ? Object.keys(list) : null,
        jobs: {},
        results: isNamedList ? {} : [],
        size: isNamedList ? Object.keys(list).length : list.length
      };
      if (sortMethod) {
        initState.keyedList.sort(isNamedList ? sortMethod : function(a, b) {
          return sortMethod(list[a], list[b]);
        });
      }
      return initState;
    }
  }
});

// ../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/terminator.js
var require_terminator = __commonJS({
  "../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/lib/terminator.js"(exports2, module2) {
    var abort = require_abort();
    var async = require_async();
    module2.exports = terminator;
    function terminator(callback) {
      if (!Object.keys(this.jobs).length) {
        return;
      }
      this.index = this.size;
      abort(this);
      async(callback)(null, this.results);
    }
  }
});

// ../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/parallel.js
var require_parallel = __commonJS({
  "../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/parallel.js"(exports2, module2) {
    var iterate = require_iterate();
    var initState = require_state();
    var terminator = require_terminator();
    module2.exports = parallel;
    function parallel(list, iterator2, callback) {
      var state = initState(list);
      while (state.index < (state["keyedList"] || list).length) {
        iterate(list, iterator2, state, function(error, result) {
          if (error) {
            callback(error, result);
            return;
          }
          if (Object.keys(state.jobs).length === 0) {
            callback(null, state.results);
            return;
          }
        });
        state.index++;
      }
      return terminator.bind(state, callback);
    }
  }
});

// ../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/serialOrdered.js
var require_serialOrdered = __commonJS({
  "../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/serialOrdered.js"(exports2, module2) {
    var iterate = require_iterate();
    var initState = require_state();
    var terminator = require_terminator();
    module2.exports = serialOrdered;
    module2.exports.ascending = ascending;
    module2.exports.descending = descending;
    function serialOrdered(list, iterator2, sortMethod, callback) {
      var state = initState(list, sortMethod);
      iterate(list, iterator2, state, function iteratorHandler(error, result) {
        if (error) {
          callback(error, result);
          return;
        }
        state.index++;
        if (state.index < (state["keyedList"] || list).length) {
          iterate(list, iterator2, state, iteratorHandler);
          return;
        }
        callback(null, state.results);
      });
      return terminator.bind(state, callback);
    }
    function ascending(a, b) {
      return a < b ? -1 : a > b ? 1 : 0;
    }
    function descending(a, b) {
      return -1 * ascending(a, b);
    }
  }
});

// ../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/serial.js
var require_serial = __commonJS({
  "../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/serial.js"(exports2, module2) {
    var serialOrdered = require_serialOrdered();
    module2.exports = serial;
    function serial(list, iterator2, callback) {
      return serialOrdered(list, iterator2, null, callback);
    }
  }
});

// ../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/index.js
var require_asynckit = __commonJS({
  "../../node_modules/.pnpm/asynckit@0.4.0/node_modules/asynckit/index.js"(exports2, module2) {
    module2.exports = {
      parallel: require_parallel(),
      serial: require_serial(),
      serialOrdered: require_serialOrdered()
    };
  }
});

// ../../node_modules/.pnpm/es-object-atoms@1.1.1/node_modules/es-object-atoms/index.js
var require_es_object_atoms = __commonJS({
  "../../node_modules/.pnpm/es-object-atoms@1.1.1/node_modules/es-object-atoms/index.js"(exports2, module2) {
    "use strict";
    module2.exports = Object;
  }
});

// ../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/index.js
var require_es_errors = __commonJS({
  "../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/index.js"(exports2, module2) {
    "use strict";
    module2.exports = Error;
  }
});

// ../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/eval.js
var require_eval = __commonJS({
  "../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/eval.js"(exports2, module2) {
    "use strict";
    module2.exports = EvalError;
  }
});

// ../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/range.js
var require_range = __commonJS({
  "../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/range.js"(exports2, module2) {
    "use strict";
    module2.exports = RangeError;
  }
});

// ../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/ref.js
var require_ref = __commonJS({
  "../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/ref.js"(exports2, module2) {
    "use strict";
    module2.exports = ReferenceError;
  }
});

// ../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/syntax.js
var require_syntax = __commonJS({
  "../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/syntax.js"(exports2, module2) {
    "use strict";
    module2.exports = SyntaxError;
  }
});

// ../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/type.js
var require_type = __commonJS({
  "../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/type.js"(exports2, module2) {
    "use strict";
    module2.exports = TypeError;
  }
});

// ../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/uri.js
var require_uri = __commonJS({
  "../../node_modules/.pnpm/es-errors@1.3.0/node_modules/es-errors/uri.js"(exports2, module2) {
    "use strict";
    module2.exports = URIError;
  }
});

// ../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/abs.js
var require_abs = __commonJS({
  "../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/abs.js"(exports2, module2) {
    "use strict";
    module2.exports = Math.abs;
  }
});

// ../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/floor.js
var require_floor = __commonJS({
  "../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/floor.js"(exports2, module2) {
    "use strict";
    module2.exports = Math.floor;
  }
});

// ../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/max.js
var require_max = __commonJS({
  "../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/max.js"(exports2, module2) {
    "use strict";
    module2.exports = Math.max;
  }
});

// ../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/min.js
var require_min = __commonJS({
  "../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/min.js"(exports2, module2) {
    "use strict";
    module2.exports = Math.min;
  }
});

// ../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/pow.js
var require_pow = __commonJS({
  "../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/pow.js"(exports2, module2) {
    "use strict";
    module2.exports = Math.pow;
  }
});

// ../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/round.js
var require_round = __commonJS({
  "../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/round.js"(exports2, module2) {
    "use strict";
    module2.exports = Math.round;
  }
});

// ../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/isNaN.js
var require_isNaN = __commonJS({
  "../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/isNaN.js"(exports2, module2) {
    "use strict";
    module2.exports = Number.isNaN || function isNaN2(a) {
      return a !== a;
    };
  }
});

// ../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/sign.js
var require_sign = __commonJS({
  "../../node_modules/.pnpm/math-intrinsics@1.1.0/node_modules/math-intrinsics/sign.js"(exports2, module2) {
    "use strict";
    var $isNaN = require_isNaN();
    module2.exports = function sign(number) {
      if ($isNaN(number) || number === 0) {
        return number;
      }
      return number < 0 ? -1 : 1;
    };
  }
});

// ../../node_modules/.pnpm/gopd@1.2.0/node_modules/gopd/gOPD.js
var require_gOPD = __commonJS({
  "../../node_modules/.pnpm/gopd@1.2.0/node_modules/gopd/gOPD.js"(exports2, module2) {
    "use strict";
    module2.exports = Object.getOwnPropertyDescriptor;
  }
});

// ../../node_modules/.pnpm/gopd@1.2.0/node_modules/gopd/index.js
var require_gopd = __commonJS({
  "../../node_modules/.pnpm/gopd@1.2.0/node_modules/gopd/index.js"(exports2, module2) {
    "use strict";
    var $gOPD = require_gOPD();
    if ($gOPD) {
      try {
        $gOPD([], "length");
      } catch (e) {
        $gOPD = null;
      }
    }
    module2.exports = $gOPD;
  }
});

// ../../node_modules/.pnpm/es-define-property@1.0.1/node_modules/es-define-property/index.js
var require_es_define_property = __commonJS({
  "../../node_modules/.pnpm/es-define-property@1.0.1/node_modules/es-define-property/index.js"(exports2, module2) {
    "use strict";
    var $defineProperty = Object.defineProperty || false;
    if ($defineProperty) {
      try {
        $defineProperty({}, "a", { value: 1 });
      } catch (e) {
        $defineProperty = false;
      }
    }
    module2.exports = $defineProperty;
  }
});

// ../../node_modules/.pnpm/has-symbols@1.1.0/node_modules/has-symbols/shams.js
var require_shams = __commonJS({
  "../../node_modules/.pnpm/has-symbols@1.1.0/node_modules/has-symbols/shams.js"(exports2, module2) {
    "use strict";
    module2.exports = function hasSymbols() {
      if (typeof Symbol !== "function" || typeof Object.getOwnPropertySymbols !== "function") {
        return false;
      }
      if (typeof Symbol.iterator === "symbol") {
        return true;
      }
      var obj = {};
      var sym = /* @__PURE__ */ Symbol("test");
      var symObj = Object(sym);
      if (typeof sym === "string") {
        return false;
      }
      if (Object.prototype.toString.call(sym) !== "[object Symbol]") {
        return false;
      }
      if (Object.prototype.toString.call(symObj) !== "[object Symbol]") {
        return false;
      }
      var symVal = 42;
      obj[sym] = symVal;
      for (var _ in obj) {
        return false;
      }
      if (typeof Object.keys === "function" && Object.keys(obj).length !== 0) {
        return false;
      }
      if (typeof Object.getOwnPropertyNames === "function" && Object.getOwnPropertyNames(obj).length !== 0) {
        return false;
      }
      var syms = Object.getOwnPropertySymbols(obj);
      if (syms.length !== 1 || syms[0] !== sym) {
        return false;
      }
      if (!Object.prototype.propertyIsEnumerable.call(obj, sym)) {
        return false;
      }
      if (typeof Object.getOwnPropertyDescriptor === "function") {
        var descriptor = (
          /** @type {PropertyDescriptor} */
          Object.getOwnPropertyDescriptor(obj, sym)
        );
        if (descriptor.value !== symVal || descriptor.enumerable !== true) {
          return false;
        }
      }
      return true;
    };
  }
});

// ../../node_modules/.pnpm/has-symbols@1.1.0/node_modules/has-symbols/index.js
var require_has_symbols = __commonJS({
  "../../node_modules/.pnpm/has-symbols@1.1.0/node_modules/has-symbols/index.js"(exports2, module2) {
    "use strict";
    var origSymbol = typeof Symbol !== "undefined" && Symbol;
    var hasSymbolSham = require_shams();
    module2.exports = function hasNativeSymbols() {
      if (typeof origSymbol !== "function") {
        return false;
      }
      if (typeof Symbol !== "function") {
        return false;
      }
      if (typeof origSymbol("foo") !== "symbol") {
        return false;
      }
      if (typeof /* @__PURE__ */ Symbol("bar") !== "symbol") {
        return false;
      }
      return hasSymbolSham();
    };
  }
});

// ../../node_modules/.pnpm/get-proto@1.0.1/node_modules/get-proto/Reflect.getPrototypeOf.js
var require_Reflect_getPrototypeOf = __commonJS({
  "../../node_modules/.pnpm/get-proto@1.0.1/node_modules/get-proto/Reflect.getPrototypeOf.js"(exports2, module2) {
    "use strict";
    module2.exports = typeof Reflect !== "undefined" && Reflect.getPrototypeOf || null;
  }
});

// ../../node_modules/.pnpm/get-proto@1.0.1/node_modules/get-proto/Object.getPrototypeOf.js
var require_Object_getPrototypeOf = __commonJS({
  "../../node_modules/.pnpm/get-proto@1.0.1/node_modules/get-proto/Object.getPrototypeOf.js"(exports2, module2) {
    "use strict";
    var $Object = require_es_object_atoms();
    module2.exports = $Object.getPrototypeOf || null;
  }
});

// ../../node_modules/.pnpm/function-bind@1.1.2/node_modules/function-bind/implementation.js
var require_implementation = __commonJS({
  "../../node_modules/.pnpm/function-bind@1.1.2/node_modules/function-bind/implementation.js"(exports2, module2) {
    "use strict";
    var ERROR_MESSAGE = "Function.prototype.bind called on incompatible ";
    var toStr = Object.prototype.toString;
    var max = Math.max;
    var funcType = "[object Function]";
    var concatty = function concatty2(a, b) {
      var arr = [];
      for (var i = 0; i < a.length; i += 1) {
        arr[i] = a[i];
      }
      for (var j = 0; j < b.length; j += 1) {
        arr[j + a.length] = b[j];
      }
      return arr;
    };
    var slicy = function slicy2(arrLike, offset) {
      var arr = [];
      for (var i = offset || 0, j = 0; i < arrLike.length; i += 1, j += 1) {
        arr[j] = arrLike[i];
      }
      return arr;
    };
    var joiny = function(arr, joiner) {
      var str = "";
      for (var i = 0; i < arr.length; i += 1) {
        str += arr[i];
        if (i + 1 < arr.length) {
          str += joiner;
        }
      }
      return str;
    };
    module2.exports = function bind2(that) {
      var target = this;
      if (typeof target !== "function" || toStr.apply(target) !== funcType) {
        throw new TypeError(ERROR_MESSAGE + target);
      }
      var args = slicy(arguments, 1);
      var bound;
      var binder = function() {
        if (this instanceof bound) {
          var result = target.apply(
            this,
            concatty(args, arguments)
          );
          if (Object(result) === result) {
            return result;
          }
          return this;
        }
        return target.apply(
          that,
          concatty(args, arguments)
        );
      };
      var boundLength = max(0, target.length - args.length);
      var boundArgs = [];
      for (var i = 0; i < boundLength; i++) {
        boundArgs[i] = "$" + i;
      }
      bound = Function("binder", "return function (" + joiny(boundArgs, ",") + "){ return binder.apply(this,arguments); }")(binder);
      if (target.prototype) {
        var Empty = function Empty2() {
        };
        Empty.prototype = target.prototype;
        bound.prototype = new Empty();
        Empty.prototype = null;
      }
      return bound;
    };
  }
});

// ../../node_modules/.pnpm/function-bind@1.1.2/node_modules/function-bind/index.js
var require_function_bind = __commonJS({
  "../../node_modules/.pnpm/function-bind@1.1.2/node_modules/function-bind/index.js"(exports2, module2) {
    "use strict";
    var implementation = require_implementation();
    module2.exports = Function.prototype.bind || implementation;
  }
});

// ../../node_modules/.pnpm/call-bind-apply-helpers@1.0.2/node_modules/call-bind-apply-helpers/functionCall.js
var require_functionCall = __commonJS({
  "../../node_modules/.pnpm/call-bind-apply-helpers@1.0.2/node_modules/call-bind-apply-helpers/functionCall.js"(exports2, module2) {
    "use strict";
    module2.exports = Function.prototype.call;
  }
});

// ../../node_modules/.pnpm/call-bind-apply-helpers@1.0.2/node_modules/call-bind-apply-helpers/functionApply.js
var require_functionApply = __commonJS({
  "../../node_modules/.pnpm/call-bind-apply-helpers@1.0.2/node_modules/call-bind-apply-helpers/functionApply.js"(exports2, module2) {
    "use strict";
    module2.exports = Function.prototype.apply;
  }
});

// ../../node_modules/.pnpm/call-bind-apply-helpers@1.0.2/node_modules/call-bind-apply-helpers/reflectApply.js
var require_reflectApply = __commonJS({
  "../../node_modules/.pnpm/call-bind-apply-helpers@1.0.2/node_modules/call-bind-apply-helpers/reflectApply.js"(exports2, module2) {
    "use strict";
    module2.exports = typeof Reflect !== "undefined" && Reflect && Reflect.apply;
  }
});

// ../../node_modules/.pnpm/call-bind-apply-helpers@1.0.2/node_modules/call-bind-apply-helpers/actualApply.js
var require_actualApply = __commonJS({
  "../../node_modules/.pnpm/call-bind-apply-helpers@1.0.2/node_modules/call-bind-apply-helpers/actualApply.js"(exports2, module2) {
    "use strict";
    var bind2 = require_function_bind();
    var $apply = require_functionApply();
    var $call = require_functionCall();
    var $reflectApply = require_reflectApply();
    module2.exports = $reflectApply || bind2.call($call, $apply);
  }
});

// ../../node_modules/.pnpm/call-bind-apply-helpers@1.0.2/node_modules/call-bind-apply-helpers/index.js
var require_call_bind_apply_helpers = __commonJS({
  "../../node_modules/.pnpm/call-bind-apply-helpers@1.0.2/node_modules/call-bind-apply-helpers/index.js"(exports2, module2) {
    "use strict";
    var bind2 = require_function_bind();
    var $TypeError = require_type();
    var $call = require_functionCall();
    var $actualApply = require_actualApply();
    module2.exports = function callBindBasic(args) {
      if (args.length < 1 || typeof args[0] !== "function") {
        throw new $TypeError("a function is required");
      }
      return $actualApply(bind2, $call, args);
    };
  }
});

// ../../node_modules/.pnpm/dunder-proto@1.0.1/node_modules/dunder-proto/get.js
var require_get = __commonJS({
  "../../node_modules/.pnpm/dunder-proto@1.0.1/node_modules/dunder-proto/get.js"(exports2, module2) {
    "use strict";
    var callBind = require_call_bind_apply_helpers();
    var gOPD = require_gopd();
    var hasProtoAccessor;
    try {
      hasProtoAccessor = /** @type {{ __proto__?: typeof Array.prototype }} */
      [].__proto__ === Array.prototype;
    } catch (e) {
      if (!e || typeof e !== "object" || !("code" in e) || e.code !== "ERR_PROTO_ACCESS") {
        throw e;
      }
    }
    var desc = !!hasProtoAccessor && gOPD && gOPD(
      Object.prototype,
      /** @type {keyof typeof Object.prototype} */
      "__proto__"
    );
    var $Object = Object;
    var $getPrototypeOf = $Object.getPrototypeOf;
    module2.exports = desc && typeof desc.get === "function" ? callBind([desc.get]) : typeof $getPrototypeOf === "function" ? (
      /** @type {import('./get')} */
      function getDunder(value) {
        return $getPrototypeOf(value == null ? value : $Object(value));
      }
    ) : false;
  }
});

// ../../node_modules/.pnpm/get-proto@1.0.1/node_modules/get-proto/index.js
var require_get_proto = __commonJS({
  "../../node_modules/.pnpm/get-proto@1.0.1/node_modules/get-proto/index.js"(exports2, module2) {
    "use strict";
    var reflectGetProto = require_Reflect_getPrototypeOf();
    var originalGetProto = require_Object_getPrototypeOf();
    var getDunderProto = require_get();
    module2.exports = reflectGetProto ? function getProto(O) {
      return reflectGetProto(O);
    } : originalGetProto ? function getProto(O) {
      if (!O || typeof O !== "object" && typeof O !== "function") {
        throw new TypeError("getProto: not an object");
      }
      return originalGetProto(O);
    } : getDunderProto ? function getProto(O) {
      return getDunderProto(O);
    } : null;
  }
});

// ../../node_modules/.pnpm/hasown@2.0.4/node_modules/hasown/index.js
var require_hasown = __commonJS({
  "../../node_modules/.pnpm/hasown@2.0.4/node_modules/hasown/index.js"(exports2, module2) {
    "use strict";
    var call = Function.prototype.call;
    var $hasOwn = Object.prototype.hasOwnProperty;
    var bind2 = require_function_bind();
    module2.exports = bind2.call(call, $hasOwn);
  }
});

// ../../node_modules/.pnpm/get-intrinsic@1.3.0/node_modules/get-intrinsic/index.js
var require_get_intrinsic = __commonJS({
  "../../node_modules/.pnpm/get-intrinsic@1.3.0/node_modules/get-intrinsic/index.js"(exports2, module2) {
    "use strict";
    var undefined2;
    var $Object = require_es_object_atoms();
    var $Error = require_es_errors();
    var $EvalError = require_eval();
    var $RangeError = require_range();
    var $ReferenceError = require_ref();
    var $SyntaxError = require_syntax();
    var $TypeError = require_type();
    var $URIError = require_uri();
    var abs = require_abs();
    var floor = require_floor();
    var max = require_max();
    var min = require_min();
    var pow = require_pow();
    var round = require_round();
    var sign = require_sign();
    var $Function = Function;
    var getEvalledConstructor = function(expressionSyntax) {
      try {
        return $Function('"use strict"; return (' + expressionSyntax + ").constructor;")();
      } catch (e) {
      }
    };
    var $gOPD = require_gopd();
    var $defineProperty = require_es_define_property();
    var throwTypeError = function() {
      throw new $TypeError();
    };
    var ThrowTypeError = $gOPD ? (function() {
      try {
        arguments.callee;
        return throwTypeError;
      } catch (calleeThrows) {
        try {
          return $gOPD(arguments, "callee").get;
        } catch (gOPDthrows) {
          return throwTypeError;
        }
      }
    })() : throwTypeError;
    var hasSymbols = require_has_symbols()();
    var getProto = require_get_proto();
    var $ObjectGPO = require_Object_getPrototypeOf();
    var $ReflectGPO = require_Reflect_getPrototypeOf();
    var $apply = require_functionApply();
    var $call = require_functionCall();
    var needsEval = {};
    var TypedArray = typeof Uint8Array === "undefined" || !getProto ? undefined2 : getProto(Uint8Array);
    var INTRINSICS = {
      __proto__: null,
      "%AggregateError%": typeof AggregateError === "undefined" ? undefined2 : AggregateError,
      "%Array%": Array,
      "%ArrayBuffer%": typeof ArrayBuffer === "undefined" ? undefined2 : ArrayBuffer,
      "%ArrayIteratorPrototype%": hasSymbols && getProto ? getProto([][Symbol.iterator]()) : undefined2,
      "%AsyncFromSyncIteratorPrototype%": undefined2,
      "%AsyncFunction%": needsEval,
      "%AsyncGenerator%": needsEval,
      "%AsyncGeneratorFunction%": needsEval,
      "%AsyncIteratorPrototype%": needsEval,
      "%Atomics%": typeof Atomics === "undefined" ? undefined2 : Atomics,
      "%BigInt%": typeof BigInt === "undefined" ? undefined2 : BigInt,
      "%BigInt64Array%": typeof BigInt64Array === "undefined" ? undefined2 : BigInt64Array,
      "%BigUint64Array%": typeof BigUint64Array === "undefined" ? undefined2 : BigUint64Array,
      "%Boolean%": Boolean,
      "%DataView%": typeof DataView === "undefined" ? undefined2 : DataView,
      "%Date%": Date,
      "%decodeURI%": decodeURI,
      "%decodeURIComponent%": decodeURIComponent,
      "%encodeURI%": encodeURI,
      "%encodeURIComponent%": encodeURIComponent,
      "%Error%": $Error,
      "%eval%": eval,
      // eslint-disable-line no-eval
      "%EvalError%": $EvalError,
      "%Float16Array%": typeof Float16Array === "undefined" ? undefined2 : Float16Array,
      "%Float32Array%": typeof Float32Array === "undefined" ? undefined2 : Float32Array,
      "%Float64Array%": typeof Float64Array === "undefined" ? undefined2 : Float64Array,
      "%FinalizationRegistry%": typeof FinalizationRegistry === "undefined" ? undefined2 : FinalizationRegistry,
      "%Function%": $Function,
      "%GeneratorFunction%": needsEval,
      "%Int8Array%": typeof Int8Array === "undefined" ? undefined2 : Int8Array,
      "%Int16Array%": typeof Int16Array === "undefined" ? undefined2 : Int16Array,
      "%Int32Array%": typeof Int32Array === "undefined" ? undefined2 : Int32Array,
      "%isFinite%": isFinite,
      "%isNaN%": isNaN,
      "%IteratorPrototype%": hasSymbols && getProto ? getProto(getProto([][Symbol.iterator]())) : undefined2,
      "%JSON%": typeof JSON === "object" ? JSON : undefined2,
      "%Map%": typeof Map === "undefined" ? undefined2 : Map,
      "%MapIteratorPrototype%": typeof Map === "undefined" || !hasSymbols || !getProto ? undefined2 : getProto((/* @__PURE__ */ new Map())[Symbol.iterator]()),
      "%Math%": Math,
      "%Number%": Number,
      "%Object%": $Object,
      "%Object.getOwnPropertyDescriptor%": $gOPD,
      "%parseFloat%": parseFloat,
      "%parseInt%": parseInt,
      "%Promise%": typeof Promise === "undefined" ? undefined2 : Promise,
      "%Proxy%": typeof Proxy === "undefined" ? undefined2 : Proxy,
      "%RangeError%": $RangeError,
      "%ReferenceError%": $ReferenceError,
      "%Reflect%": typeof Reflect === "undefined" ? undefined2 : Reflect,
      "%RegExp%": RegExp,
      "%Set%": typeof Set === "undefined" ? undefined2 : Set,
      "%SetIteratorPrototype%": typeof Set === "undefined" || !hasSymbols || !getProto ? undefined2 : getProto((/* @__PURE__ */ new Set())[Symbol.iterator]()),
      "%SharedArrayBuffer%": typeof SharedArrayBuffer === "undefined" ? undefined2 : SharedArrayBuffer,
      "%String%": String,
      "%StringIteratorPrototype%": hasSymbols && getProto ? getProto(""[Symbol.iterator]()) : undefined2,
      "%Symbol%": hasSymbols ? Symbol : undefined2,
      "%SyntaxError%": $SyntaxError,
      "%ThrowTypeError%": ThrowTypeError,
      "%TypedArray%": TypedArray,
      "%TypeError%": $TypeError,
      "%Uint8Array%": typeof Uint8Array === "undefined" ? undefined2 : Uint8Array,
      "%Uint8ClampedArray%": typeof Uint8ClampedArray === "undefined" ? undefined2 : Uint8ClampedArray,
      "%Uint16Array%": typeof Uint16Array === "undefined" ? undefined2 : Uint16Array,
      "%Uint32Array%": typeof Uint32Array === "undefined" ? undefined2 : Uint32Array,
      "%URIError%": $URIError,
      "%WeakMap%": typeof WeakMap === "undefined" ? undefined2 : WeakMap,
      "%WeakRef%": typeof WeakRef === "undefined" ? undefined2 : WeakRef,
      "%WeakSet%": typeof WeakSet === "undefined" ? undefined2 : WeakSet,
      "%Function.prototype.call%": $call,
      "%Function.prototype.apply%": $apply,
      "%Object.defineProperty%": $defineProperty,
      "%Object.getPrototypeOf%": $ObjectGPO,
      "%Math.abs%": abs,
      "%Math.floor%": floor,
      "%Math.max%": max,
      "%Math.min%": min,
      "%Math.pow%": pow,
      "%Math.round%": round,
      "%Math.sign%": sign,
      "%Reflect.getPrototypeOf%": $ReflectGPO
    };
    if (getProto) {
      try {
        null.error;
      } catch (e) {
        errorProto = getProto(getProto(e));
        INTRINSICS["%Error.prototype%"] = errorProto;
      }
    }
    var errorProto;
    var doEval = function doEval2(name) {
      var value;
      if (name === "%AsyncFunction%") {
        value = getEvalledConstructor("async function () {}");
      } else if (name === "%GeneratorFunction%") {
        value = getEvalledConstructor("function* () {}");
      } else if (name === "%AsyncGeneratorFunction%") {
        value = getEvalledConstructor("async function* () {}");
      } else if (name === "%AsyncGenerator%") {
        var fn = doEval2("%AsyncGeneratorFunction%");
        if (fn) {
          value = fn.prototype;
        }
      } else if (name === "%AsyncIteratorPrototype%") {
        var gen = doEval2("%AsyncGenerator%");
        if (gen && getProto) {
          value = getProto(gen.prototype);
        }
      }
      INTRINSICS[name] = value;
      return value;
    };
    var LEGACY_ALIASES = {
      __proto__: null,
      "%ArrayBufferPrototype%": ["ArrayBuffer", "prototype"],
      "%ArrayPrototype%": ["Array", "prototype"],
      "%ArrayProto_entries%": ["Array", "prototype", "entries"],
      "%ArrayProto_forEach%": ["Array", "prototype", "forEach"],
      "%ArrayProto_keys%": ["Array", "prototype", "keys"],
      "%ArrayProto_values%": ["Array", "prototype", "values"],
      "%AsyncFunctionPrototype%": ["AsyncFunction", "prototype"],
      "%AsyncGenerator%": ["AsyncGeneratorFunction", "prototype"],
      "%AsyncGeneratorPrototype%": ["AsyncGeneratorFunction", "prototype", "prototype"],
      "%BooleanPrototype%": ["Boolean", "prototype"],
      "%DataViewPrototype%": ["DataView", "prototype"],
      "%DatePrototype%": ["Date", "prototype"],
      "%ErrorPrototype%": ["Error", "prototype"],
      "%EvalErrorPrototype%": ["EvalError", "prototype"],
      "%Float32ArrayPrototype%": ["Float32Array", "prototype"],
      "%Float64ArrayPrototype%": ["Float64Array", "prototype"],
      "%FunctionPrototype%": ["Function", "prototype"],
      "%Generator%": ["GeneratorFunction", "prototype"],
      "%GeneratorPrototype%": ["GeneratorFunction", "prototype", "prototype"],
      "%Int8ArrayPrototype%": ["Int8Array", "prototype"],
      "%Int16ArrayPrototype%": ["Int16Array", "prototype"],
      "%Int32ArrayPrototype%": ["Int32Array", "prototype"],
      "%JSONParse%": ["JSON", "parse"],
      "%JSONStringify%": ["JSON", "stringify"],
      "%MapPrototype%": ["Map", "prototype"],
      "%NumberPrototype%": ["Number", "prototype"],
      "%ObjectPrototype%": ["Object", "prototype"],
      "%ObjProto_toString%": ["Object", "prototype", "toString"],
      "%ObjProto_valueOf%": ["Object", "prototype", "valueOf"],
      "%PromisePrototype%": ["Promise", "prototype"],
      "%PromiseProto_then%": ["Promise", "prototype", "then"],
      "%Promise_all%": ["Promise", "all"],
      "%Promise_reject%": ["Promise", "reject"],
      "%Promise_resolve%": ["Promise", "resolve"],
      "%RangeErrorPrototype%": ["RangeError", "prototype"],
      "%ReferenceErrorPrototype%": ["ReferenceError", "prototype"],
      "%RegExpPrototype%": ["RegExp", "prototype"],
      "%SetPrototype%": ["Set", "prototype"],
      "%SharedArrayBufferPrototype%": ["SharedArrayBuffer", "prototype"],
      "%StringPrototype%": ["String", "prototype"],
      "%SymbolPrototype%": ["Symbol", "prototype"],
      "%SyntaxErrorPrototype%": ["SyntaxError", "prototype"],
      "%TypedArrayPrototype%": ["TypedArray", "prototype"],
      "%TypeErrorPrototype%": ["TypeError", "prototype"],
      "%Uint8ArrayPrototype%": ["Uint8Array", "prototype"],
      "%Uint8ClampedArrayPrototype%": ["Uint8ClampedArray", "prototype"],
      "%Uint16ArrayPrototype%": ["Uint16Array", "prototype"],
      "%Uint32ArrayPrototype%": ["Uint32Array", "prototype"],
      "%URIErrorPrototype%": ["URIError", "prototype"],
      "%WeakMapPrototype%": ["WeakMap", "prototype"],
      "%WeakSetPrototype%": ["WeakSet", "prototype"]
    };
    var bind2 = require_function_bind();
    var hasOwn = require_hasown();
    var $concat = bind2.call($call, Array.prototype.concat);
    var $spliceApply = bind2.call($apply, Array.prototype.splice);
    var $replace = bind2.call($call, String.prototype.replace);
    var $strSlice = bind2.call($call, String.prototype.slice);
    var $exec = bind2.call($call, RegExp.prototype.exec);
    var rePropName = /[^%.[\]]+|\[(?:(-?\d+(?:\.\d+)?)|(["'])((?:(?!\2)[^\\]|\\.)*?)\2)\]|(?=(?:\.|\[\])(?:\.|\[\]|%$))/g;
    var reEscapeChar = /\\(\\)?/g;
    var stringToPath = function stringToPath2(string) {
      var first = $strSlice(string, 0, 1);
      var last = $strSlice(string, -1);
      if (first === "%" && last !== "%") {
        throw new $SyntaxError("invalid intrinsic syntax, expected closing `%`");
      } else if (last === "%" && first !== "%") {
        throw new $SyntaxError("invalid intrinsic syntax, expected opening `%`");
      }
      var result = [];
      $replace(string, rePropName, function(match, number, quote, subString) {
        result[result.length] = quote ? $replace(subString, reEscapeChar, "$1") : number || match;
      });
      return result;
    };
    var getBaseIntrinsic = function getBaseIntrinsic2(name, allowMissing) {
      var intrinsicName = name;
      var alias;
      if (hasOwn(LEGACY_ALIASES, intrinsicName)) {
        alias = LEGACY_ALIASES[intrinsicName];
        intrinsicName = "%" + alias[0] + "%";
      }
      if (hasOwn(INTRINSICS, intrinsicName)) {
        var value = INTRINSICS[intrinsicName];
        if (value === needsEval) {
          value = doEval(intrinsicName);
        }
        if (typeof value === "undefined" && !allowMissing) {
          throw new $TypeError("intrinsic " + name + " exists, but is not available. Please file an issue!");
        }
        return {
          alias,
          name: intrinsicName,
          value
        };
      }
      throw new $SyntaxError("intrinsic " + name + " does not exist!");
    };
    module2.exports = function GetIntrinsic(name, allowMissing) {
      if (typeof name !== "string" || name.length === 0) {
        throw new $TypeError("intrinsic name must be a non-empty string");
      }
      if (arguments.length > 1 && typeof allowMissing !== "boolean") {
        throw new $TypeError('"allowMissing" argument must be a boolean');
      }
      if ($exec(/^%?[^%]*%?$/, name) === null) {
        throw new $SyntaxError("`%` may not be present anywhere but at the beginning and end of the intrinsic name");
      }
      var parts = stringToPath(name);
      var intrinsicBaseName = parts.length > 0 ? parts[0] : "";
      var intrinsic = getBaseIntrinsic("%" + intrinsicBaseName + "%", allowMissing);
      var intrinsicRealName = intrinsic.name;
      var value = intrinsic.value;
      var skipFurtherCaching = false;
      var alias = intrinsic.alias;
      if (alias) {
        intrinsicBaseName = alias[0];
        $spliceApply(parts, $concat([0, 1], alias));
      }
      for (var i = 1, isOwn = true; i < parts.length; i += 1) {
        var part = parts[i];
        var first = $strSlice(part, 0, 1);
        var last = $strSlice(part, -1);
        if ((first === '"' || first === "'" || first === "`" || (last === '"' || last === "'" || last === "`")) && first !== last) {
          throw new $SyntaxError("property names with quotes must have matching quotes");
        }
        if (part === "constructor" || !isOwn) {
          skipFurtherCaching = true;
        }
        intrinsicBaseName += "." + part;
        intrinsicRealName = "%" + intrinsicBaseName + "%";
        if (hasOwn(INTRINSICS, intrinsicRealName)) {
          value = INTRINSICS[intrinsicRealName];
        } else if (value != null) {
          if (!(part in value)) {
            if (!allowMissing) {
              throw new $TypeError("base intrinsic for " + name + " exists, but the property is not available.");
            }
            return void undefined2;
          }
          if ($gOPD && i + 1 >= parts.length) {
            var desc = $gOPD(value, part);
            isOwn = !!desc;
            if (isOwn && "get" in desc && !("originalValue" in desc.get)) {
              value = desc.get;
            } else {
              value = value[part];
            }
          } else {
            isOwn = hasOwn(value, part);
            value = value[part];
          }
          if (isOwn && !skipFurtherCaching) {
            INTRINSICS[intrinsicRealName] = value;
          }
        }
      }
      return value;
    };
  }
});

// ../../node_modules/.pnpm/has-tostringtag@1.0.2/node_modules/has-tostringtag/shams.js
var require_shams2 = __commonJS({
  "../../node_modules/.pnpm/has-tostringtag@1.0.2/node_modules/has-tostringtag/shams.js"(exports2, module2) {
    "use strict";
    var hasSymbols = require_shams();
    module2.exports = function hasToStringTagShams() {
      return hasSymbols() && !!Symbol.toStringTag;
    };
  }
});

// ../../node_modules/.pnpm/es-set-tostringtag@2.1.0/node_modules/es-set-tostringtag/index.js
var require_es_set_tostringtag = __commonJS({
  "../../node_modules/.pnpm/es-set-tostringtag@2.1.0/node_modules/es-set-tostringtag/index.js"(exports2, module2) {
    "use strict";
    var GetIntrinsic = require_get_intrinsic();
    var $defineProperty = GetIntrinsic("%Object.defineProperty%", true);
    var hasToStringTag = require_shams2()();
    var hasOwn = require_hasown();
    var $TypeError = require_type();
    var toStringTag2 = hasToStringTag ? Symbol.toStringTag : null;
    module2.exports = function setToStringTag(object, value) {
      var overrideIfSet = arguments.length > 2 && !!arguments[2] && arguments[2].force;
      var nonConfigurable = arguments.length > 2 && !!arguments[2] && arguments[2].nonConfigurable;
      if (typeof overrideIfSet !== "undefined" && typeof overrideIfSet !== "boolean" || typeof nonConfigurable !== "undefined" && typeof nonConfigurable !== "boolean") {
        throw new $TypeError("if provided, the `overrideIfSet` and `nonConfigurable` options must be booleans");
      }
      if (toStringTag2 && (overrideIfSet || !hasOwn(object, toStringTag2))) {
        if ($defineProperty) {
          $defineProperty(object, toStringTag2, {
            configurable: !nonConfigurable,
            enumerable: false,
            value,
            writable: false
          });
        } else {
          object[toStringTag2] = value;
        }
      }
    };
  }
});

// ../../node_modules/.pnpm/form-data@4.0.6/node_modules/form-data/lib/populate.js
var require_populate = __commonJS({
  "../../node_modules/.pnpm/form-data@4.0.6/node_modules/form-data/lib/populate.js"(exports2, module2) {
    "use strict";
    module2.exports = function(dst, src) {
      Object.keys(src).forEach(function(prop) {
        dst[prop] = dst[prop] || src[prop];
      });
      return dst;
    };
  }
});

// ../../node_modules/.pnpm/form-data@4.0.6/node_modules/form-data/lib/form_data.js
var require_form_data = __commonJS({
  "../../node_modules/.pnpm/form-data@4.0.6/node_modules/form-data/lib/form_data.js"(exports2, module2) {
    "use strict";
    var CombinedStream = require_combined_stream();
    var util3 = require("util");
    var path8 = require("path");
    var http3 = require("http");
    var https2 = require("https");
    var parseUrl2 = require("url").parse;
    var fs2 = require("fs");
    var Stream = require("stream").Stream;
    var crypto2 = require("crypto");
    var mime = require_mime_types();
    var asynckit = require_asynckit();
    var setToStringTag = require_es_set_tostringtag();
    var hasOwn = require_hasown();
    var populate = require_populate();
    function escapeHeaderParam(str) {
      return String(str).replace(/\r/g, "%0D").replace(/\n/g, "%0A").replace(/"/g, "%22");
    }
    function FormData3(options) {
      if (!(this instanceof FormData3)) {
        return new FormData3(options);
      }
      this._overheadLength = 0;
      this._valueLength = 0;
      this._valuesToMeasure = [];
      CombinedStream.call(this);
      options = options || {};
      for (var option in options) {
        this[option] = options[option];
      }
    }
    util3.inherits(FormData3, CombinedStream);
    FormData3.LINE_BREAK = "\r\n";
    FormData3.DEFAULT_CONTENT_TYPE = "application/octet-stream";
    FormData3.prototype.append = function(field, value, options) {
      options = options || {};
      if (typeof options === "string") {
        options = { filename: options };
      }
      var append2 = CombinedStream.prototype.append.bind(this);
      if (typeof value === "number" || value == null) {
        value = String(value);
      }
      if (Array.isArray(value)) {
        this._error(new Error("Arrays are not supported."));
        return;
      }
      var header = this._multiPartHeader(field, value, options);
      var footer = this._multiPartFooter();
      append2(header);
      append2(value);
      append2(footer);
      this._trackLength(header, value, options);
    };
    FormData3.prototype._trackLength = function(header, value, options) {
      var valueLength = 0;
      if (options.knownLength != null) {
        valueLength += Number(options.knownLength);
      } else if (Buffer.isBuffer(value)) {
        valueLength = value.length;
      } else if (typeof value === "string") {
        valueLength = Buffer.byteLength(value);
      }
      this._valueLength += valueLength;
      this._overheadLength += Buffer.byteLength(header) + FormData3.LINE_BREAK.length;
      if (!value || !value.path && !(value.readable && hasOwn(value, "httpVersion")) && !(value instanceof Stream)) {
        return;
      }
      if (!options.knownLength) {
        this._valuesToMeasure.push(value);
      }
    };
    FormData3.prototype._lengthRetriever = function(value, callback) {
      if (hasOwn(value, "fd")) {
        if (value.end != void 0 && value.end != Infinity && value.start != void 0) {
          callback(null, value.end + 1 - (value.start ? value.start : 0));
        } else {
          fs2.stat(value.path, function(err, stat3) {
            if (err) {
              callback(err);
              return;
            }
            var fileSize = stat3.size - (value.start ? value.start : 0);
            callback(null, fileSize);
          });
        }
      } else if (hasOwn(value, "httpVersion")) {
        callback(null, Number(value.headers["content-length"]));
      } else if (hasOwn(value, "httpModule")) {
        value.on("response", function(response) {
          value.pause();
          callback(null, Number(response.headers["content-length"]));
        });
        value.resume();
      } else {
        callback("Unknown stream");
      }
    };
    FormData3.prototype._multiPartHeader = function(field, value, options) {
      if (typeof options.header === "string") {
        return options.header;
      }
      var contentDisposition = this._getContentDisposition(value, options);
      var contentType = this._getContentType(value, options);
      var contents = "";
      var headers = {
        // add custom disposition as third element or keep it two elements if not
        "Content-Disposition": ["form-data", 'name="' + escapeHeaderParam(field) + '"'].concat(contentDisposition || []),
        // if no content type. allow it to be empty array
        "Content-Type": [].concat(contentType || [])
      };
      if (typeof options.header === "object") {
        populate(headers, options.header);
      }
      var header;
      for (var prop in headers) {
        if (hasOwn(headers, prop)) {
          header = headers[prop];
          if (header == null) {
            continue;
          }
          if (!Array.isArray(header)) {
            header = [header];
          }
          if (header.length) {
            contents += prop + ": " + header.join("; ") + FormData3.LINE_BREAK;
          }
        }
      }
      return "--" + this.getBoundary() + FormData3.LINE_BREAK + contents + FormData3.LINE_BREAK;
    };
    FormData3.prototype._getContentDisposition = function(value, options) {
      var filename;
      if (typeof options.filepath === "string") {
        filename = path8.normalize(options.filepath).replace(/\\/g, "/");
      } else if (options.filename || value && (value.name || value.path)) {
        filename = path8.basename(options.filename || value && (value.name || value.path));
      } else if (value && value.readable && hasOwn(value, "httpVersion")) {
        filename = path8.basename(value.client._httpMessage.path || "");
      }
      if (filename) {
        return 'filename="' + escapeHeaderParam(filename) + '"';
      }
    };
    FormData3.prototype._getContentType = function(value, options) {
      var contentType = options.contentType;
      if (!contentType && value && value.name) {
        contentType = mime.lookup(value.name);
      }
      if (!contentType && value && value.path) {
        contentType = mime.lookup(value.path);
      }
      if (!contentType && value && value.readable && hasOwn(value, "httpVersion")) {
        contentType = value.headers["content-type"];
      }
      if (!contentType && (options.filepath || options.filename)) {
        contentType = mime.lookup(options.filepath || options.filename);
      }
      if (!contentType && value && typeof value === "object") {
        contentType = FormData3.DEFAULT_CONTENT_TYPE;
      }
      return contentType;
    };
    FormData3.prototype._multiPartFooter = function() {
      return function(next) {
        var footer = FormData3.LINE_BREAK;
        var lastPart = this._streams.length === 0;
        if (lastPart) {
          footer += this._lastBoundary();
        }
        next(footer);
      }.bind(this);
    };
    FormData3.prototype._lastBoundary = function() {
      return "--" + this.getBoundary() + "--" + FormData3.LINE_BREAK;
    };
    FormData3.prototype.getHeaders = function(userHeaders) {
      var header;
      var formHeaders = {
        "content-type": "multipart/form-data; boundary=" + this.getBoundary()
      };
      for (header in userHeaders) {
        if (hasOwn(userHeaders, header)) {
          formHeaders[header.toLowerCase()] = userHeaders[header];
        }
      }
      return formHeaders;
    };
    FormData3.prototype.setBoundary = function(boundary) {
      if (typeof boundary !== "string") {
        throw new TypeError("FormData boundary must be a string");
      }
      this._boundary = boundary;
    };
    FormData3.prototype.getBoundary = function() {
      if (!this._boundary) {
        this._generateBoundary();
      }
      return this._boundary;
    };
    FormData3.prototype.getBuffer = function() {
      var dataBuffer = new Buffer.alloc(0);
      var boundary = this.getBoundary();
      for (var i = 0, len = this._streams.length; i < len; i++) {
        if (typeof this._streams[i] !== "function") {
          if (Buffer.isBuffer(this._streams[i])) {
            dataBuffer = Buffer.concat([dataBuffer, this._streams[i]]);
          } else {
            dataBuffer = Buffer.concat([dataBuffer, Buffer.from(this._streams[i])]);
          }
          if (typeof this._streams[i] !== "string" || this._streams[i].substring(2, boundary.length + 2) !== boundary) {
            dataBuffer = Buffer.concat([dataBuffer, Buffer.from(FormData3.LINE_BREAK)]);
          }
        }
      }
      return Buffer.concat([dataBuffer, Buffer.from(this._lastBoundary())]);
    };
    FormData3.prototype._generateBoundary = function() {
      this._boundary = "--------------------------" + crypto2.randomBytes(12).toString("hex");
    };
    FormData3.prototype.getLengthSync = function() {
      var knownLength = this._overheadLength + this._valueLength;
      if (this._streams.length) {
        knownLength += this._lastBoundary().length;
      }
      if (!this.hasKnownLength()) {
        this._error(new Error("Cannot calculate proper length in synchronous way."));
      }
      return knownLength;
    };
    FormData3.prototype.hasKnownLength = function() {
      var hasKnownLength = true;
      if (this._valuesToMeasure.length) {
        hasKnownLength = false;
      }
      return hasKnownLength;
    };
    FormData3.prototype.getLength = function(cb) {
      var knownLength = this._overheadLength + this._valueLength;
      if (this._streams.length) {
        knownLength += this._lastBoundary().length;
      }
      if (!this._valuesToMeasure.length) {
        process.nextTick(cb.bind(this, null, knownLength));
        return;
      }
      asynckit.parallel(this._valuesToMeasure, this._lengthRetriever, function(err, values) {
        if (err) {
          cb(err);
          return;
        }
        values.forEach(function(length) {
          knownLength += length;
        });
        cb(null, knownLength);
      });
    };
    FormData3.prototype.submit = function(params, cb) {
      var request;
      var options;
      var defaults2 = { method: "post" };
      if (typeof params === "string") {
        params = parseUrl2(params);
        options = populate({
          port: params.port,
          path: params.pathname,
          host: params.hostname,
          protocol: params.protocol
        }, defaults2);
      } else {
        options = populate(params, defaults2);
        if (!options.port) {
          options.port = options.protocol === "https:" ? 443 : 80;
        }
      }
      options.headers = this.getHeaders(params.headers);
      if (options.protocol === "https:") {
        request = https2.request(options);
      } else {
        request = http3.request(options);
      }
      this.getLength(function(err, length) {
        if (err && err !== "Unknown stream") {
          this._error(err);
          return;
        }
        if (length) {
          request.setHeader("Content-Length", length);
        }
        this.pipe(request);
        if (cb) {
          var onResponse;
          var callback = function(error, responce) {
            request.removeListener("error", callback);
            request.removeListener("response", onResponse);
            return cb.call(this, error, responce);
          };
          onResponse = callback.bind(this, null);
          request.on("error", callback);
          request.on("response", onResponse);
        }
      }.bind(this));
      return request;
    };
    FormData3.prototype._error = function(err) {
      if (!this.error) {
        this.error = err;
        this.pause();
        this.emit("error", err);
      }
    };
    FormData3.prototype.toString = function() {
      return "[object FormData]";
    };
    setToStringTag(FormData3.prototype, "FormData");
    module2.exports = FormData3;
  }
});

// ../../node_modules/.pnpm/ms@2.1.3/node_modules/ms/index.js
var require_ms = __commonJS({
  "../../node_modules/.pnpm/ms@2.1.3/node_modules/ms/index.js"(exports2, module2) {
    var s = 1e3;
    var m = s * 60;
    var h = m * 60;
    var d = h * 24;
    var w = d * 7;
    var y = d * 365.25;
    module2.exports = function(val, options) {
      options = options || {};
      var type = typeof val;
      if (type === "string" && val.length > 0) {
        return parse(val);
      } else if (type === "number" && isFinite(val)) {
        return options.long ? fmtLong(val) : fmtShort(val);
      }
      throw new Error(
        "val is not a non-empty string or a valid number. val=" + JSON.stringify(val)
      );
    };
    function parse(str) {
      str = String(str);
      if (str.length > 100) {
        return;
      }
      var match = /^(-?(?:\d+)?\.?\d+) *(milliseconds?|msecs?|ms|seconds?|secs?|s|minutes?|mins?|m|hours?|hrs?|h|days?|d|weeks?|w|years?|yrs?|y)?$/i.exec(
        str
      );
      if (!match) {
        return;
      }
      var n = parseFloat(match[1]);
      var type = (match[2] || "ms").toLowerCase();
      switch (type) {
        case "years":
        case "year":
        case "yrs":
        case "yr":
        case "y":
          return n * y;
        case "weeks":
        case "week":
        case "w":
          return n * w;
        case "days":
        case "day":
        case "d":
          return n * d;
        case "hours":
        case "hour":
        case "hrs":
        case "hr":
        case "h":
          return n * h;
        case "minutes":
        case "minute":
        case "mins":
        case "min":
        case "m":
          return n * m;
        case "seconds":
        case "second":
        case "secs":
        case "sec":
        case "s":
          return n * s;
        case "milliseconds":
        case "millisecond":
        case "msecs":
        case "msec":
        case "ms":
          return n;
        default:
          return void 0;
      }
    }
    function fmtShort(ms) {
      var msAbs = Math.abs(ms);
      if (msAbs >= d) {
        return Math.round(ms / d) + "d";
      }
      if (msAbs >= h) {
        return Math.round(ms / h) + "h";
      }
      if (msAbs >= m) {
        return Math.round(ms / m) + "m";
      }
      if (msAbs >= s) {
        return Math.round(ms / s) + "s";
      }
      return ms + "ms";
    }
    function fmtLong(ms) {
      var msAbs = Math.abs(ms);
      if (msAbs >= d) {
        return plural(ms, msAbs, d, "day");
      }
      if (msAbs >= h) {
        return plural(ms, msAbs, h, "hour");
      }
      if (msAbs >= m) {
        return plural(ms, msAbs, m, "minute");
      }
      if (msAbs >= s) {
        return plural(ms, msAbs, s, "second");
      }
      return ms + " ms";
    }
    function plural(ms, msAbs, n, name) {
      var isPlural = msAbs >= n * 1.5;
      return Math.round(ms / n) + " " + name + (isPlural ? "s" : "");
    }
  }
});

// ../../node_modules/.pnpm/debug@4.4.3_supports-color@10.2.2/node_modules/debug/src/common.js
var require_common = __commonJS({
  "../../node_modules/.pnpm/debug@4.4.3_supports-color@10.2.2/node_modules/debug/src/common.js"(exports2, module2) {
    function setup(env2) {
      createDebug.debug = createDebug;
      createDebug.default = createDebug;
      createDebug.coerce = coerce;
      createDebug.disable = disable;
      createDebug.enable = enable;
      createDebug.enabled = enabled;
      createDebug.humanize = require_ms();
      createDebug.destroy = destroy;
      Object.keys(env2).forEach((key) => {
        createDebug[key] = env2[key];
      });
      createDebug.names = [];
      createDebug.skips = [];
      createDebug.formatters = {};
      function selectColor(namespace) {
        let hash = 0;
        for (let i = 0; i < namespace.length; i++) {
          hash = (hash << 5) - hash + namespace.charCodeAt(i);
          hash |= 0;
        }
        return createDebug.colors[Math.abs(hash) % createDebug.colors.length];
      }
      createDebug.selectColor = selectColor;
      function createDebug(namespace) {
        let prevTime;
        let enableOverride = null;
        let namespacesCache;
        let enabledCache;
        function debug(...args) {
          if (!debug.enabled) {
            return;
          }
          const self2 = debug;
          const curr = Number(/* @__PURE__ */ new Date());
          const ms = curr - (prevTime || curr);
          self2.diff = ms;
          self2.prev = prevTime;
          self2.curr = curr;
          prevTime = curr;
          args[0] = createDebug.coerce(args[0]);
          if (typeof args[0] !== "string") {
            args.unshift("%O");
          }
          let index = 0;
          args[0] = args[0].replace(/%([a-zA-Z%])/g, (match, format) => {
            if (match === "%%") {
              return "%";
            }
            index++;
            const formatter = createDebug.formatters[format];
            if (typeof formatter === "function") {
              const val = args[index];
              match = formatter.call(self2, val);
              args.splice(index, 1);
              index--;
            }
            return match;
          });
          createDebug.formatArgs.call(self2, args);
          const logFn = self2.log || createDebug.log;
          logFn.apply(self2, args);
        }
        debug.namespace = namespace;
        debug.useColors = createDebug.useColors();
        debug.color = createDebug.selectColor(namespace);
        debug.extend = extend2;
        debug.destroy = createDebug.destroy;
        Object.defineProperty(debug, "enabled", {
          enumerable: true,
          configurable: false,
          get: () => {
            if (enableOverride !== null) {
              return enableOverride;
            }
            if (namespacesCache !== createDebug.namespaces) {
              namespacesCache = createDebug.namespaces;
              enabledCache = createDebug.enabled(namespace);
            }
            return enabledCache;
          },
          set: (v) => {
            enableOverride = v;
          }
        });
        if (typeof createDebug.init === "function") {
          createDebug.init(debug);
        }
        return debug;
      }
      function extend2(namespace, delimiter) {
        const newDebug = createDebug(this.namespace + (typeof delimiter === "undefined" ? ":" : delimiter) + namespace);
        newDebug.log = this.log;
        return newDebug;
      }
      function enable(namespaces) {
        createDebug.save(namespaces);
        createDebug.namespaces = namespaces;
        createDebug.names = [];
        createDebug.skips = [];
        const split = (typeof namespaces === "string" ? namespaces : "").trim().replace(/\s+/g, ",").split(",").filter(Boolean);
        for (const ns of split) {
          if (ns[0] === "-") {
            createDebug.skips.push(ns.slice(1));
          } else {
            createDebug.names.push(ns);
          }
        }
      }
      function matchesTemplate(search, template) {
        let searchIndex = 0;
        let templateIndex = 0;
        let starIndex = -1;
        let matchIndex = 0;
        while (searchIndex < search.length) {
          if (templateIndex < template.length && (template[templateIndex] === search[searchIndex] || template[templateIndex] === "*")) {
            if (template[templateIndex] === "*") {
              starIndex = templateIndex;
              matchIndex = searchIndex;
              templateIndex++;
            } else {
              searchIndex++;
              templateIndex++;
            }
          } else if (starIndex !== -1) {
            templateIndex = starIndex + 1;
            matchIndex++;
            searchIndex = matchIndex;
          } else {
            return false;
          }
        }
        while (templateIndex < template.length && template[templateIndex] === "*") {
          templateIndex++;
        }
        return templateIndex === template.length;
      }
      function disable() {
        const namespaces = [
          ...createDebug.names,
          ...createDebug.skips.map((namespace) => "-" + namespace)
        ].join(",");
        createDebug.enable("");
        return namespaces;
      }
      function enabled(name) {
        for (const skip of createDebug.skips) {
          if (matchesTemplate(name, skip)) {
            return false;
          }
        }
        for (const ns of createDebug.names) {
          if (matchesTemplate(name, ns)) {
            return true;
          }
        }
        return false;
      }
      function coerce(val) {
        if (val instanceof Error) {
          return val.stack || val.message;
        }
        return val;
      }
      function destroy() {
        console.warn("Instance method `debug.destroy()` is deprecated and no longer does anything. It will be removed in the next major version of `debug`.");
      }
      createDebug.enable(createDebug.load());
      return createDebug;
    }
    module2.exports = setup;
  }
});

// ../../node_modules/.pnpm/debug@4.4.3_supports-color@10.2.2/node_modules/debug/src/browser.js
var require_browser = __commonJS({
  "../../node_modules/.pnpm/debug@4.4.3_supports-color@10.2.2/node_modules/debug/src/browser.js"(exports2, module2) {
    exports2.formatArgs = formatArgs;
    exports2.save = save;
    exports2.load = load;
    exports2.useColors = useColors;
    exports2.storage = localstorage();
    exports2.destroy = /* @__PURE__ */ (() => {
      let warned = false;
      return () => {
        if (!warned) {
          warned = true;
          console.warn("Instance method `debug.destroy()` is deprecated and no longer does anything. It will be removed in the next major version of `debug`.");
        }
      };
    })();
    exports2.colors = [
      "#0000CC",
      "#0000FF",
      "#0033CC",
      "#0033FF",
      "#0066CC",
      "#0066FF",
      "#0099CC",
      "#0099FF",
      "#00CC00",
      "#00CC33",
      "#00CC66",
      "#00CC99",
      "#00CCCC",
      "#00CCFF",
      "#3300CC",
      "#3300FF",
      "#3333CC",
      "#3333FF",
      "#3366CC",
      "#3366FF",
      "#3399CC",
      "#3399FF",
      "#33CC00",
      "#33CC33",
      "#33CC66",
      "#33CC99",
      "#33CCCC",
      "#33CCFF",
      "#6600CC",
      "#6600FF",
      "#6633CC",
      "#6633FF",
      "#66CC00",
      "#66CC33",
      "#9900CC",
      "#9900FF",
      "#9933CC",
      "#9933FF",
      "#99CC00",
      "#99CC33",
      "#CC0000",
      "#CC0033",
      "#CC0066",
      "#CC0099",
      "#CC00CC",
      "#CC00FF",
      "#CC3300",
      "#CC3333",
      "#CC3366",
      "#CC3399",
      "#CC33CC",
      "#CC33FF",
      "#CC6600",
      "#CC6633",
      "#CC9900",
      "#CC9933",
      "#CCCC00",
      "#CCCC33",
      "#FF0000",
      "#FF0033",
      "#FF0066",
      "#FF0099",
      "#FF00CC",
      "#FF00FF",
      "#FF3300",
      "#FF3333",
      "#FF3366",
      "#FF3399",
      "#FF33CC",
      "#FF33FF",
      "#FF6600",
      "#FF6633",
      "#FF9900",
      "#FF9933",
      "#FFCC00",
      "#FFCC33"
    ];
    function useColors() {
      if (typeof window !== "undefined" && window.process && (window.process.type === "renderer" || window.process.__nwjs)) {
        return true;
      }
      if (typeof navigator !== "undefined" && navigator.userAgent && navigator.userAgent.toLowerCase().match(/(edge|trident)\/(\d+)/)) {
        return false;
      }
      let m;
      return typeof document !== "undefined" && document.documentElement && document.documentElement.style && document.documentElement.style.WebkitAppearance || // Is firebug? http://stackoverflow.com/a/398120/376773
      typeof window !== "undefined" && window.console && (window.console.firebug || window.console.exception && window.console.table) || // Is firefox >= v31?
      // https://developer.mozilla.org/en-US/docs/Tools/Web_Console#Styling_messages
      typeof navigator !== "undefined" && navigator.userAgent && (m = navigator.userAgent.toLowerCase().match(/firefox\/(\d+)/)) && parseInt(m[1], 10) >= 31 || // Double check webkit in userAgent just in case we are in a worker
      typeof navigator !== "undefined" && navigator.userAgent && navigator.userAgent.toLowerCase().match(/applewebkit\/(\d+)/);
    }
    function formatArgs(args) {
      args[0] = (this.useColors ? "%c" : "") + this.namespace + (this.useColors ? " %c" : " ") + args[0] + (this.useColors ? "%c " : " ") + "+" + module2.exports.humanize(this.diff);
      if (!this.useColors) {
        return;
      }
      const c = "color: " + this.color;
      args.splice(1, 0, c, "color: inherit");
      let index = 0;
      let lastC = 0;
      args[0].replace(/%[a-zA-Z%]/g, (match) => {
        if (match === "%%") {
          return;
        }
        index++;
        if (match === "%c") {
          lastC = index;
        }
      });
      args.splice(lastC, 0, c);
    }
    exports2.log = console.debug || console.log || (() => {
    });
    function save(namespaces) {
      try {
        if (namespaces) {
          exports2.storage.setItem("debug", namespaces);
        } else {
          exports2.storage.removeItem("debug");
        }
      } catch (error) {
      }
    }
    function load() {
      let r;
      try {
        r = exports2.storage.getItem("debug") || exports2.storage.getItem("DEBUG");
      } catch (error) {
      }
      if (!r && typeof process !== "undefined" && "env" in process) {
        r = process.env.DEBUG;
      }
      return r;
    }
    function localstorage() {
      try {
        return localStorage;
      } catch (error) {
      }
    }
    module2.exports = require_common()(exports2);
    var { formatters } = module2.exports;
    formatters.j = function(v) {
      try {
        return JSON.stringify(v);
      } catch (error) {
        return "[UnexpectedJSONParseError]: " + error.message;
      }
    };
  }
});

// ../../node_modules/.pnpm/supports-color@10.2.2/node_modules/supports-color/index.js
var supports_color_exports = {};
__export(supports_color_exports, {
  createSupportsColor: () => createSupportsColor,
  default: () => supports_color_default
});
function hasFlag(flag, argv = globalThis.Deno ? globalThis.Deno.args : import_node_process.default.argv) {
  const prefix = flag.startsWith("-") ? "" : flag.length === 1 ? "-" : "--";
  const position = argv.indexOf(prefix + flag);
  const terminatorPosition = argv.indexOf("--");
  return position !== -1 && (terminatorPosition === -1 || position < terminatorPosition);
}
function envForceColor() {
  if (!("FORCE_COLOR" in env)) {
    return;
  }
  if (env.FORCE_COLOR === "true") {
    return 1;
  }
  if (env.FORCE_COLOR === "false") {
    return 0;
  }
  if (env.FORCE_COLOR.length === 0) {
    return 1;
  }
  const level = Math.min(Number.parseInt(env.FORCE_COLOR, 10), 3);
  if (![0, 1, 2, 3].includes(level)) {
    return;
  }
  return level;
}
function translateLevel(level) {
  if (level === 0) {
    return false;
  }
  return {
    level,
    hasBasic: true,
    has256: level >= 2,
    has16m: level >= 3
  };
}
function _supportsColor(haveStream, { streamIsTTY, sniffFlags = true } = {}) {
  const noFlagForceColor = envForceColor();
  if (noFlagForceColor !== void 0) {
    flagForceColor = noFlagForceColor;
  }
  const forceColor = sniffFlags ? flagForceColor : noFlagForceColor;
  if (forceColor === 0) {
    return 0;
  }
  if (sniffFlags) {
    if (hasFlag("color=16m") || hasFlag("color=full") || hasFlag("color=truecolor")) {
      return 3;
    }
    if (hasFlag("color=256")) {
      return 2;
    }
  }
  if ("TF_BUILD" in env && "AGENT_NAME" in env) {
    return 1;
  }
  if (haveStream && !streamIsTTY && forceColor === void 0) {
    return 0;
  }
  const min = forceColor || 0;
  if (env.TERM === "dumb") {
    return min;
  }
  if (import_node_process.default.platform === "win32") {
    const osRelease = import_node_os.default.release().split(".");
    if (Number(osRelease[0]) >= 10 && Number(osRelease[2]) >= 10586) {
      return Number(osRelease[2]) >= 14931 ? 3 : 2;
    }
    return 1;
  }
  if ("CI" in env) {
    if (["GITHUB_ACTIONS", "GITEA_ACTIONS", "CIRCLECI"].some((key) => key in env)) {
      return 3;
    }
    if (["TRAVIS", "APPVEYOR", "GITLAB_CI", "BUILDKITE", "DRONE"].some((sign) => sign in env) || env.CI_NAME === "codeship") {
      return 1;
    }
    return min;
  }
  if ("TEAMCITY_VERSION" in env) {
    return /^(9\.(0*[1-9]\d*)\.|\d{2,}\.)/.test(env.TEAMCITY_VERSION) ? 1 : 0;
  }
  if (env.COLORTERM === "truecolor") {
    return 3;
  }
  if (env.TERM === "xterm-kitty") {
    return 3;
  }
  if (env.TERM === "xterm-ghostty") {
    return 3;
  }
  if (env.TERM === "wezterm") {
    return 3;
  }
  if ("TERM_PROGRAM" in env) {
    const version = Number.parseInt((env.TERM_PROGRAM_VERSION || "").split(".")[0], 10);
    switch (env.TERM_PROGRAM) {
      case "iTerm.app": {
        return version >= 3 ? 3 : 2;
      }
      case "Apple_Terminal": {
        return 2;
      }
    }
  }
  if (/-256(color)?$/i.test(env.TERM)) {
    return 2;
  }
  if (/^screen|^xterm|^vt100|^vt220|^rxvt|color|ansi|cygwin|linux/i.test(env.TERM)) {
    return 1;
  }
  if ("COLORTERM" in env) {
    return 1;
  }
  return min;
}
function createSupportsColor(stream4, options = {}) {
  const level = _supportsColor(stream4, {
    streamIsTTY: stream4 && stream4.isTTY,
    ...options
  });
  return translateLevel(level);
}
var import_node_process, import_node_os, import_node_tty, env, flagForceColor, supportsColor, supports_color_default;
var init_supports_color = __esm({
  "../../node_modules/.pnpm/supports-color@10.2.2/node_modules/supports-color/index.js"() {
    import_node_process = __toESM(require("node:process"), 1);
    import_node_os = __toESM(require("node:os"), 1);
    import_node_tty = __toESM(require("node:tty"), 1);
    ({ env } = import_node_process.default);
    if (hasFlag("no-color") || hasFlag("no-colors") || hasFlag("color=false") || hasFlag("color=never")) {
      flagForceColor = 0;
    } else if (hasFlag("color") || hasFlag("colors") || hasFlag("color=true") || hasFlag("color=always")) {
      flagForceColor = 1;
    }
    supportsColor = {
      stdout: createSupportsColor({ isTTY: import_node_tty.default.isatty(1) }),
      stderr: createSupportsColor({ isTTY: import_node_tty.default.isatty(2) })
    };
    supports_color_default = supportsColor;
  }
});

// ../../node_modules/.pnpm/debug@4.4.3_supports-color@10.2.2/node_modules/debug/src/node.js
var require_node = __commonJS({
  "../../node_modules/.pnpm/debug@4.4.3_supports-color@10.2.2/node_modules/debug/src/node.js"(exports2, module2) {
    var tty2 = require("tty");
    var util3 = require("util");
    exports2.init = init;
    exports2.log = log2;
    exports2.formatArgs = formatArgs;
    exports2.save = save;
    exports2.load = load;
    exports2.useColors = useColors;
    exports2.destroy = util3.deprecate(
      () => {
      },
      "Instance method `debug.destroy()` is deprecated and no longer does anything. It will be removed in the next major version of `debug`."
    );
    exports2.colors = [6, 2, 3, 4, 5, 1];
    try {
      const supportsColor2 = (init_supports_color(), __toCommonJS(supports_color_exports));
      if (supportsColor2 && (supportsColor2.stderr || supportsColor2).level >= 2) {
        exports2.colors = [
          20,
          21,
          26,
          27,
          32,
          33,
          38,
          39,
          40,
          41,
          42,
          43,
          44,
          45,
          56,
          57,
          62,
          63,
          68,
          69,
          74,
          75,
          76,
          77,
          78,
          79,
          80,
          81,
          92,
          93,
          98,
          99,
          112,
          113,
          128,
          129,
          134,
          135,
          148,
          149,
          160,
          161,
          162,
          163,
          164,
          165,
          166,
          167,
          168,
          169,
          170,
          171,
          172,
          173,
          178,
          179,
          184,
          185,
          196,
          197,
          198,
          199,
          200,
          201,
          202,
          203,
          204,
          205,
          206,
          207,
          208,
          209,
          214,
          215,
          220,
          221
        ];
      }
    } catch (error) {
    }
    exports2.inspectOpts = Object.keys(process.env).filter((key) => {
      return /^debug_/i.test(key);
    }).reduce((obj, key) => {
      const prop = key.substring(6).toLowerCase().replace(/_([a-z])/g, (_, k) => {
        return k.toUpperCase();
      });
      let val = process.env[key];
      if (/^(yes|on|true|enabled)$/i.test(val)) {
        val = true;
      } else if (/^(no|off|false|disabled)$/i.test(val)) {
        val = false;
      } else if (val === "null") {
        val = null;
      } else {
        val = Number(val);
      }
      obj[prop] = val;
      return obj;
    }, {});
    function useColors() {
      return "colors" in exports2.inspectOpts ? Boolean(exports2.inspectOpts.colors) : tty2.isatty(process.stderr.fd);
    }
    function formatArgs(args) {
      const { namespace: name, useColors: useColors2 } = this;
      if (useColors2) {
        const c = this.color;
        const colorCode = "\x1B[3" + (c < 8 ? c : "8;5;" + c);
        const prefix = `  ${colorCode};1m${name} \x1B[0m`;
        args[0] = prefix + args[0].split("\n").join("\n" + prefix);
        args.push(colorCode + "m+" + module2.exports.humanize(this.diff) + "\x1B[0m");
      } else {
        args[0] = getDate() + name + " " + args[0];
      }
    }
    function getDate() {
      if (exports2.inspectOpts.hideDate) {
        return "";
      }
      return (/* @__PURE__ */ new Date()).toISOString() + " ";
    }
    function log2(...args) {
      return process.stderr.write(util3.formatWithOptions(exports2.inspectOpts, ...args) + "\n");
    }
    function save(namespaces) {
      if (namespaces) {
        process.env.DEBUG = namespaces;
      } else {
        delete process.env.DEBUG;
      }
    }
    function load() {
      return process.env.DEBUG;
    }
    function init(debug) {
      debug.inspectOpts = {};
      const keys = Object.keys(exports2.inspectOpts);
      for (let i = 0; i < keys.length; i++) {
        debug.inspectOpts[keys[i]] = exports2.inspectOpts[keys[i]];
      }
    }
    module2.exports = require_common()(exports2);
    var { formatters } = module2.exports;
    formatters.o = function(v) {
      this.inspectOpts.colors = this.useColors;
      return util3.inspect(v, this.inspectOpts).split("\n").map((str) => str.trim()).join(" ");
    };
    formatters.O = function(v) {
      this.inspectOpts.colors = this.useColors;
      return util3.inspect(v, this.inspectOpts);
    };
  }
});

// ../../node_modules/.pnpm/debug@4.4.3_supports-color@10.2.2/node_modules/debug/src/index.js
var require_src = __commonJS({
  "../../node_modules/.pnpm/debug@4.4.3_supports-color@10.2.2/node_modules/debug/src/index.js"(exports2, module2) {
    if (typeof process === "undefined" || process.type === "renderer" || process.browser === true || process.__nwjs) {
      module2.exports = require_browser();
    } else {
      module2.exports = require_node();
    }
  }
});

// ../../node_modules/.pnpm/agent-base@6.0.2_supports-color@10.2.2/node_modules/agent-base/dist/src/promisify.js
var require_promisify = __commonJS({
  "../../node_modules/.pnpm/agent-base@6.0.2_supports-color@10.2.2/node_modules/agent-base/dist/src/promisify.js"(exports2) {
    "use strict";
    Object.defineProperty(exports2, "__esModule", { value: true });
    function promisify(fn) {
      return function(req, opts) {
        return new Promise((resolve2, reject) => {
          fn.call(this, req, opts, (err, rtn) => {
            if (err) {
              reject(err);
            } else {
              resolve2(rtn);
            }
          });
        });
      };
    }
    exports2.default = promisify;
  }
});

// ../../node_modules/.pnpm/agent-base@6.0.2_supports-color@10.2.2/node_modules/agent-base/dist/src/index.js
var require_src2 = __commonJS({
  "../../node_modules/.pnpm/agent-base@6.0.2_supports-color@10.2.2/node_modules/agent-base/dist/src/index.js"(exports2, module2) {
    "use strict";
    var __importDefault = exports2 && exports2.__importDefault || function(mod) {
      return mod && mod.__esModule ? mod : { "default": mod };
    };
    var events_1 = require("events");
    var debug_1 = __importDefault(require_src());
    var promisify_1 = __importDefault(require_promisify());
    var debug = debug_1.default("agent-base");
    function isAgent(v) {
      return Boolean(v) && typeof v.addRequest === "function";
    }
    function isSecureEndpoint() {
      const { stack } = new Error();
      if (typeof stack !== "string")
        return false;
      return stack.split("\n").some((l) => l.indexOf("(https.js:") !== -1 || l.indexOf("node:https:") !== -1);
    }
    function createAgent(callback, opts) {
      return new createAgent.Agent(callback, opts);
    }
    (function(createAgent2) {
      class Agent extends events_1.EventEmitter {
        constructor(callback, _opts) {
          super();
          let opts = _opts;
          if (typeof callback === "function") {
            this.callback = callback;
          } else if (callback) {
            opts = callback;
          }
          this.timeout = null;
          if (opts && typeof opts.timeout === "number") {
            this.timeout = opts.timeout;
          }
          this.maxFreeSockets = 1;
          this.maxSockets = 1;
          this.maxTotalSockets = Infinity;
          this.sockets = {};
          this.freeSockets = {};
          this.requests = {};
          this.options = {};
        }
        get defaultPort() {
          if (typeof this.explicitDefaultPort === "number") {
            return this.explicitDefaultPort;
          }
          return isSecureEndpoint() ? 443 : 80;
        }
        set defaultPort(v) {
          this.explicitDefaultPort = v;
        }
        get protocol() {
          if (typeof this.explicitProtocol === "string") {
            return this.explicitProtocol;
          }
          return isSecureEndpoint() ? "https:" : "http:";
        }
        set protocol(v) {
          this.explicitProtocol = v;
        }
        callback(req, opts, fn) {
          throw new Error('"agent-base" has no default implementation, you must subclass and override `callback()`');
        }
        /**
         * Called by node-core's "_http_client.js" module when creating
         * a new HTTP request with this Agent instance.
         *
         * @api public
         */
        addRequest(req, _opts) {
          const opts = Object.assign({}, _opts);
          if (typeof opts.secureEndpoint !== "boolean") {
            opts.secureEndpoint = isSecureEndpoint();
          }
          if (opts.host == null) {
            opts.host = "localhost";
          }
          if (opts.port == null) {
            opts.port = opts.secureEndpoint ? 443 : 80;
          }
          if (opts.protocol == null) {
            opts.protocol = opts.secureEndpoint ? "https:" : "http:";
          }
          if (opts.host && opts.path) {
            delete opts.path;
          }
          delete opts.agent;
          delete opts.hostname;
          delete opts._defaultAgent;
          delete opts.defaultPort;
          delete opts.createConnection;
          req._last = true;
          req.shouldKeepAlive = false;
          let timedOut = false;
          let timeoutId = null;
          const timeoutMs = opts.timeout || this.timeout;
          const onerror = (err) => {
            if (req._hadError)
              return;
            req.emit("error", err);
            req._hadError = true;
          };
          const ontimeout = () => {
            timeoutId = null;
            timedOut = true;
            const err = new Error(`A "socket" was not created for HTTP request before ${timeoutMs}ms`);
            err.code = "ETIMEOUT";
            onerror(err);
          };
          const callbackError = (err) => {
            if (timedOut)
              return;
            if (timeoutId !== null) {
              clearTimeout(timeoutId);
              timeoutId = null;
            }
            onerror(err);
          };
          const onsocket = (socket) => {
            if (timedOut)
              return;
            if (timeoutId != null) {
              clearTimeout(timeoutId);
              timeoutId = null;
            }
            if (isAgent(socket)) {
              debug("Callback returned another Agent instance %o", socket.constructor.name);
              socket.addRequest(req, opts);
              return;
            }
            if (socket) {
              socket.once("free", () => {
                this.freeSocket(socket, opts);
              });
              req.onSocket(socket);
              return;
            }
            const err = new Error(`no Duplex stream was returned to agent-base for \`${req.method} ${req.path}\``);
            onerror(err);
          };
          if (typeof this.callback !== "function") {
            onerror(new Error("`callback` is not defined"));
            return;
          }
          if (!this.promisifiedCallback) {
            if (this.callback.length >= 3) {
              debug("Converting legacy callback function to promise");
              this.promisifiedCallback = promisify_1.default(this.callback);
            } else {
              this.promisifiedCallback = this.callback;
            }
          }
          if (typeof timeoutMs === "number" && timeoutMs > 0) {
            timeoutId = setTimeout(ontimeout, timeoutMs);
          }
          if ("port" in opts && typeof opts.port !== "number") {
            opts.port = Number(opts.port);
          }
          try {
            debug("Resolving socket for %o request: %o", opts.protocol, `${req.method} ${req.path}`);
            Promise.resolve(this.promisifiedCallback(req, opts)).then(onsocket, callbackError);
          } catch (err) {
            Promise.reject(err).catch(callbackError);
          }
        }
        freeSocket(socket, opts) {
          debug("Freeing socket %o %o", socket.constructor.name, opts);
          socket.destroy();
        }
        destroy() {
          debug("Destroying agent %o", this.constructor.name);
        }
      }
      createAgent2.Agent = Agent;
      createAgent2.prototype = createAgent2.Agent.prototype;
    })(createAgent || (createAgent = {}));
    module2.exports = createAgent;
  }
});

// ../../node_modules/.pnpm/https-proxy-agent@5.0.1_supports-color@10.2.2/node_modules/https-proxy-agent/dist/parse-proxy-response.js
var require_parse_proxy_response = __commonJS({
  "../../node_modules/.pnpm/https-proxy-agent@5.0.1_supports-color@10.2.2/node_modules/https-proxy-agent/dist/parse-proxy-response.js"(exports2) {
    "use strict";
    var __importDefault = exports2 && exports2.__importDefault || function(mod) {
      return mod && mod.__esModule ? mod : { "default": mod };
    };
    Object.defineProperty(exports2, "__esModule", { value: true });
    var debug_1 = __importDefault(require_src());
    var debug = debug_1.default("https-proxy-agent:parse-proxy-response");
    function parseProxyResponse(socket) {
      return new Promise((resolve2, reject) => {
        let buffersLength = 0;
        const buffers = [];
        function read() {
          const b = socket.read();
          if (b)
            ondata(b);
          else
            socket.once("readable", read);
        }
        function cleanup() {
          socket.removeListener("end", onend);
          socket.removeListener("error", onerror);
          socket.removeListener("close", onclose);
          socket.removeListener("readable", read);
        }
        function onclose(err) {
          debug("onclose had error %o", err);
        }
        function onend() {
          debug("onend");
        }
        function onerror(err) {
          cleanup();
          debug("onerror %o", err);
          reject(err);
        }
        function ondata(b) {
          buffers.push(b);
          buffersLength += b.length;
          const buffered = Buffer.concat(buffers, buffersLength);
          const endOfHeaders = buffered.indexOf("\r\n\r\n");
          if (endOfHeaders === -1) {
            debug("have not received end of HTTP headers yet...");
            read();
            return;
          }
          const firstLine = buffered.toString("ascii", 0, buffered.indexOf("\r\n"));
          const statusCode = +firstLine.split(" ")[1];
          debug("got proxy server response: %o", firstLine);
          resolve2({
            statusCode,
            buffered
          });
        }
        socket.on("error", onerror);
        socket.on("close", onclose);
        socket.on("end", onend);
        read();
      });
    }
    exports2.default = parseProxyResponse;
  }
});

// ../../node_modules/.pnpm/https-proxy-agent@5.0.1_supports-color@10.2.2/node_modules/https-proxy-agent/dist/agent.js
var require_agent = __commonJS({
  "../../node_modules/.pnpm/https-proxy-agent@5.0.1_supports-color@10.2.2/node_modules/https-proxy-agent/dist/agent.js"(exports2) {
    "use strict";
    var __awaiter = exports2 && exports2.__awaiter || function(thisArg, _arguments, P, generator) {
      function adopt(value) {
        return value instanceof P ? value : new P(function(resolve2) {
          resolve2(value);
        });
      }
      return new (P || (P = Promise))(function(resolve2, reject) {
        function fulfilled(value) {
          try {
            step(generator.next(value));
          } catch (e) {
            reject(e);
          }
        }
        function rejected(value) {
          try {
            step(generator["throw"](value));
          } catch (e) {
            reject(e);
          }
        }
        function step(result) {
          result.done ? resolve2(result.value) : adopt(result.value).then(fulfilled, rejected);
        }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
      });
    };
    var __importDefault = exports2 && exports2.__importDefault || function(mod) {
      return mod && mod.__esModule ? mod : { "default": mod };
    };
    Object.defineProperty(exports2, "__esModule", { value: true });
    var net_1 = __importDefault(require("net"));
    var tls_1 = __importDefault(require("tls"));
    var url_1 = __importDefault(require("url"));
    var assert_1 = __importDefault(require("assert"));
    var debug_1 = __importDefault(require_src());
    var agent_base_1 = require_src2();
    var parse_proxy_response_1 = __importDefault(require_parse_proxy_response());
    var debug = debug_1.default("https-proxy-agent:agent");
    var HttpsProxyAgent2 = class extends agent_base_1.Agent {
      constructor(_opts) {
        let opts;
        if (typeof _opts === "string") {
          opts = url_1.default.parse(_opts);
        } else {
          opts = _opts;
        }
        if (!opts) {
          throw new Error("an HTTP(S) proxy server `host` and `port` must be specified!");
        }
        debug("creating new HttpsProxyAgent instance: %o", opts);
        super(opts);
        const proxy = Object.assign({}, opts);
        this.secureProxy = opts.secureProxy || isHTTPS(proxy.protocol);
        proxy.host = proxy.hostname || proxy.host;
        if (typeof proxy.port === "string") {
          proxy.port = parseInt(proxy.port, 10);
        }
        if (!proxy.port && proxy.host) {
          proxy.port = this.secureProxy ? 443 : 80;
        }
        if (this.secureProxy && !("ALPNProtocols" in proxy)) {
          proxy.ALPNProtocols = ["http 1.1"];
        }
        if (proxy.host && proxy.path) {
          delete proxy.path;
          delete proxy.pathname;
        }
        this.proxy = proxy;
      }
      /**
       * Called when the node-core HTTP client library is creating a
       * new HTTP request.
       *
       * @api protected
       */
      callback(req, opts) {
        return __awaiter(this, void 0, void 0, function* () {
          const { proxy, secureProxy } = this;
          let socket;
          if (secureProxy) {
            debug("Creating `tls.Socket`: %o", proxy);
            socket = tls_1.default.connect(proxy);
          } else {
            debug("Creating `net.Socket`: %o", proxy);
            socket = net_1.default.connect(proxy);
          }
          const headers = Object.assign({}, proxy.headers);
          const hostname = `${opts.host}:${opts.port}`;
          let payload = `CONNECT ${hostname} HTTP/1.1\r
`;
          if (proxy.auth) {
            headers["Proxy-Authorization"] = `Basic ${Buffer.from(proxy.auth).toString("base64")}`;
          }
          let { host, port, secureEndpoint } = opts;
          if (!isDefaultPort(port, secureEndpoint)) {
            host += `:${port}`;
          }
          headers.Host = host;
          headers.Connection = "close";
          for (const name of Object.keys(headers)) {
            payload += `${name}: ${headers[name]}\r
`;
          }
          const proxyResponsePromise = parse_proxy_response_1.default(socket);
          socket.write(`${payload}\r
`);
          const { statusCode, buffered } = yield proxyResponsePromise;
          if (statusCode === 200) {
            req.once("socket", resume);
            if (opts.secureEndpoint) {
              debug("Upgrading socket connection to TLS");
              const servername = opts.servername || opts.host;
              return tls_1.default.connect(Object.assign(Object.assign({}, omit(opts, "host", "hostname", "path", "port")), {
                socket,
                servername
              }));
            }
            return socket;
          }
          socket.destroy();
          const fakeSocket = new net_1.default.Socket({ writable: false });
          fakeSocket.readable = true;
          req.once("socket", (s) => {
            debug("replaying proxy buffer for failed request");
            assert_1.default(s.listenerCount("data") > 0);
            s.push(buffered);
            s.push(null);
          });
          return fakeSocket;
        });
      }
    };
    exports2.default = HttpsProxyAgent2;
    function resume(socket) {
      socket.resume();
    }
    function isDefaultPort(port, secure) {
      return Boolean(!secure && port === 80 || secure && port === 443);
    }
    function isHTTPS(protocol) {
      return typeof protocol === "string" ? /^https:?$/i.test(protocol) : false;
    }
    function omit(obj, ...keys) {
      const ret = {};
      let key;
      for (key in obj) {
        if (!keys.includes(key)) {
          ret[key] = obj[key];
        }
      }
      return ret;
    }
  }
});

// ../../node_modules/.pnpm/https-proxy-agent@5.0.1_supports-color@10.2.2/node_modules/https-proxy-agent/dist/index.js
var require_dist = __commonJS({
  "../../node_modules/.pnpm/https-proxy-agent@5.0.1_supports-color@10.2.2/node_modules/https-proxy-agent/dist/index.js"(exports2, module2) {
    "use strict";
    var __importDefault = exports2 && exports2.__importDefault || function(mod) {
      return mod && mod.__esModule ? mod : { "default": mod };
    };
    var agent_1 = __importDefault(require_agent());
    function createHttpsProxyAgent(opts) {
      return new agent_1.default(opts);
    }
    (function(createHttpsProxyAgent2) {
      createHttpsProxyAgent2.HttpsProxyAgent = agent_1.default;
      createHttpsProxyAgent2.prototype = agent_1.default.prototype;
    })(createHttpsProxyAgent || (createHttpsProxyAgent = {}));
    module2.exports = createHttpsProxyAgent;
  }
});

// ../../node_modules/.pnpm/follow-redirects@1.16.0_debug@4.4.3_supports-color@10.2.2_/node_modules/follow-redirects/debug.js
var require_debug = __commonJS({
  "../../node_modules/.pnpm/follow-redirects@1.16.0_debug@4.4.3_supports-color@10.2.2_/node_modules/follow-redirects/debug.js"(exports2, module2) {
    var debug;
    module2.exports = function() {
      if (!debug) {
        try {
          debug = require_src()("follow-redirects");
        } catch (error) {
        }
        if (typeof debug !== "function") {
          debug = function() {
          };
        }
      }
      debug.apply(null, arguments);
    };
  }
});

// ../../node_modules/.pnpm/follow-redirects@1.16.0_debug@4.4.3_supports-color@10.2.2_/node_modules/follow-redirects/index.js
var require_follow_redirects = __commonJS({
  "../../node_modules/.pnpm/follow-redirects@1.16.0_debug@4.4.3_supports-color@10.2.2_/node_modules/follow-redirects/index.js"(exports2, module2) {
    var url2 = require("url");
    var URL2 = url2.URL;
    var http3 = require("http");
    var https2 = require("https");
    var Writable = require("stream").Writable;
    var assert = require("assert");
    var debug = require_debug();
    (function detectUnsupportedEnvironment() {
      var looksLikeNode = typeof process !== "undefined";
      var looksLikeBrowser = typeof window !== "undefined" && typeof document !== "undefined";
      var looksLikeV8 = isFunction3(Error.captureStackTrace);
      if (!looksLikeNode && (looksLikeBrowser || !looksLikeV8)) {
        console.warn("The follow-redirects package should be excluded from browser builds.");
      }
    })();
    var useNativeURL = false;
    try {
      assert(new URL2(""));
    } catch (error) {
      useNativeURL = error.code === "ERR_INVALID_URL";
    }
    var sensitiveHeaders = [
      "Authorization",
      "Proxy-Authorization",
      "Cookie"
    ];
    var preservedUrlFields = [
      "auth",
      "host",
      "hostname",
      "href",
      "path",
      "pathname",
      "port",
      "protocol",
      "query",
      "search",
      "hash"
    ];
    var events = ["abort", "aborted", "connect", "error", "socket", "timeout"];
    var eventHandlers = /* @__PURE__ */ Object.create(null);
    events.forEach(function(event) {
      eventHandlers[event] = function(arg1, arg2, arg3) {
        this._redirectable.emit(event, arg1, arg2, arg3);
      };
    });
    var InvalidUrlError = createErrorType(
      "ERR_INVALID_URL",
      "Invalid URL",
      TypeError
    );
    var RedirectionError = createErrorType(
      "ERR_FR_REDIRECTION_FAILURE",
      "Redirected request failed"
    );
    var TooManyRedirectsError = createErrorType(
      "ERR_FR_TOO_MANY_REDIRECTS",
      "Maximum number of redirects exceeded",
      RedirectionError
    );
    var MaxBodyLengthExceededError = createErrorType(
      "ERR_FR_MAX_BODY_LENGTH_EXCEEDED",
      "Request body larger than maxBodyLength limit"
    );
    var WriteAfterEndError = createErrorType(
      "ERR_STREAM_WRITE_AFTER_END",
      "write after end"
    );
    var destroy = Writable.prototype.destroy || noop2;
    function RedirectableRequest(options, responseCallback) {
      Writable.call(this);
      this._sanitizeOptions(options);
      this._options = options;
      this._ended = false;
      this._ending = false;
      this._redirectCount = 0;
      this._redirects = [];
      this._requestBodyLength = 0;
      this._requestBodyBuffers = [];
      if (responseCallback) {
        this.on("response", responseCallback);
      }
      var self2 = this;
      this._onNativeResponse = function(response) {
        try {
          self2._processResponse(response);
        } catch (cause) {
          self2.emit("error", cause instanceof RedirectionError ? cause : new RedirectionError({ cause }));
        }
      };
      this._headerFilter = new RegExp("^(?:" + sensitiveHeaders.concat(options.sensitiveHeaders).map(escapeRegex).join("|") + ")$", "i");
      this._performRequest();
    }
    RedirectableRequest.prototype = Object.create(Writable.prototype);
    RedirectableRequest.prototype.abort = function() {
      destroyRequest(this._currentRequest);
      this._currentRequest.abort();
      this.emit("abort");
    };
    RedirectableRequest.prototype.destroy = function(error) {
      destroyRequest(this._currentRequest, error);
      destroy.call(this, error);
      return this;
    };
    RedirectableRequest.prototype.write = function(data, encoding, callback) {
      if (this._ending) {
        throw new WriteAfterEndError();
      }
      if (!isString2(data) && !isBuffer2(data)) {
        throw new TypeError("data should be a string, Buffer or Uint8Array");
      }
      if (isFunction3(encoding)) {
        callback = encoding;
        encoding = null;
      }
      if (data.length === 0) {
        if (callback) {
          callback();
        }
        return;
      }
      if (this._requestBodyLength + data.length <= this._options.maxBodyLength) {
        this._requestBodyLength += data.length;
        this._requestBodyBuffers.push({ data, encoding });
        this._currentRequest.write(data, encoding, callback);
      } else {
        this.emit("error", new MaxBodyLengthExceededError());
        this.abort();
      }
    };
    RedirectableRequest.prototype.end = function(data, encoding, callback) {
      if (isFunction3(data)) {
        callback = data;
        data = encoding = null;
      } else if (isFunction3(encoding)) {
        callback = encoding;
        encoding = null;
      }
      if (!data) {
        this._ended = this._ending = true;
        this._currentRequest.end(null, null, callback);
      } else {
        var self2 = this;
        var currentRequest = this._currentRequest;
        this.write(data, encoding, function() {
          self2._ended = true;
          currentRequest.end(null, null, callback);
        });
        this._ending = true;
      }
    };
    RedirectableRequest.prototype.setHeader = function(name, value) {
      this._options.headers[name] = value;
      this._currentRequest.setHeader(name, value);
    };
    RedirectableRequest.prototype.removeHeader = function(name) {
      delete this._options.headers[name];
      this._currentRequest.removeHeader(name);
    };
    RedirectableRequest.prototype.setTimeout = function(msecs, callback) {
      var self2 = this;
      function destroyOnTimeout(socket) {
        socket.setTimeout(msecs);
        socket.removeListener("timeout", socket.destroy);
        socket.addListener("timeout", socket.destroy);
      }
      function startTimer(socket) {
        if (self2._timeout) {
          clearTimeout(self2._timeout);
        }
        self2._timeout = setTimeout(function() {
          self2.emit("timeout");
          clearTimer();
        }, msecs);
        destroyOnTimeout(socket);
      }
      function clearTimer() {
        if (self2._timeout) {
          clearTimeout(self2._timeout);
          self2._timeout = null;
        }
        self2.removeListener("abort", clearTimer);
        self2.removeListener("error", clearTimer);
        self2.removeListener("response", clearTimer);
        self2.removeListener("close", clearTimer);
        if (callback) {
          self2.removeListener("timeout", callback);
        }
        if (!self2.socket) {
          self2._currentRequest.removeListener("socket", startTimer);
        }
      }
      if (callback) {
        this.on("timeout", callback);
      }
      if (this.socket) {
        startTimer(this.socket);
      } else {
        this._currentRequest.once("socket", startTimer);
      }
      this.on("socket", destroyOnTimeout);
      this.on("abort", clearTimer);
      this.on("error", clearTimer);
      this.on("response", clearTimer);
      this.on("close", clearTimer);
      return this;
    };
    [
      "flushHeaders",
      "getHeader",
      "setNoDelay",
      "setSocketKeepAlive"
    ].forEach(function(method) {
      RedirectableRequest.prototype[method] = function(a, b) {
        return this._currentRequest[method](a, b);
      };
    });
    ["aborted", "connection", "socket"].forEach(function(property) {
      Object.defineProperty(RedirectableRequest.prototype, property, {
        get: function() {
          return this._currentRequest[property];
        }
      });
    });
    RedirectableRequest.prototype._sanitizeOptions = function(options) {
      if (!options.headers) {
        options.headers = {};
      }
      if (!isArray2(options.sensitiveHeaders)) {
        options.sensitiveHeaders = [];
      }
      if (options.host) {
        if (!options.hostname) {
          options.hostname = options.host;
        }
        delete options.host;
      }
      if (!options.pathname && options.path) {
        var searchPos = options.path.indexOf("?");
        if (searchPos < 0) {
          options.pathname = options.path;
        } else {
          options.pathname = options.path.substring(0, searchPos);
          options.search = options.path.substring(searchPos);
        }
      }
    };
    RedirectableRequest.prototype._performRequest = function() {
      var protocol = this._options.protocol;
      var nativeProtocol = this._options.nativeProtocols[protocol];
      if (!nativeProtocol) {
        throw new TypeError("Unsupported protocol " + protocol);
      }
      if (this._options.agents) {
        var scheme = protocol.slice(0, -1);
        this._options.agent = this._options.agents[scheme];
      }
      var request = this._currentRequest = nativeProtocol.request(this._options, this._onNativeResponse);
      request._redirectable = this;
      for (var event of events) {
        request.on(event, eventHandlers[event]);
      }
      this._currentUrl = /^\//.test(this._options.path) ? url2.format(this._options) : (
        // When making a request to a proxy, […]
        // a client MUST send the target URI in absolute-form […].
        this._options.path
      );
      if (this._isRedirect) {
        var i = 0;
        var self2 = this;
        var buffers = this._requestBodyBuffers;
        (function writeNext(error) {
          if (request === self2._currentRequest) {
            if (error) {
              self2.emit("error", error);
            } else if (i < buffers.length) {
              var buffer2 = buffers[i++];
              if (!request.finished) {
                request.write(buffer2.data, buffer2.encoding, writeNext);
              }
            } else if (self2._ended) {
              request.end();
            }
          }
        })();
      }
    };
    RedirectableRequest.prototype._processResponse = function(response) {
      var statusCode = response.statusCode;
      if (this._options.trackRedirects) {
        this._redirects.push({
          url: this._currentUrl,
          headers: response.headers,
          statusCode
        });
      }
      var location = response.headers.location;
      if (!location || this._options.followRedirects === false || statusCode < 300 || statusCode >= 400) {
        response.responseUrl = this._currentUrl;
        response.redirects = this._redirects;
        this.emit("response", response);
        this._requestBodyBuffers = [];
        return;
      }
      destroyRequest(this._currentRequest);
      response.destroy();
      if (++this._redirectCount > this._options.maxRedirects) {
        throw new TooManyRedirectsError();
      }
      var requestHeaders;
      var beforeRedirect = this._options.beforeRedirect;
      if (beforeRedirect) {
        requestHeaders = Object.assign({
          // The Host header was set by nativeProtocol.request
          Host: response.req.getHeader("host")
        }, this._options.headers);
      }
      var method = this._options.method;
      if ((statusCode === 301 || statusCode === 302) && this._options.method === "POST" || // RFC7231§6.4.4: The 303 (See Other) status code indicates that
      // the server is redirecting the user agent to a different resource […]
      // A user agent can perform a retrieval request targeting that URI
      // (a GET or HEAD request if using HTTP) […]
      statusCode === 303 && !/^(?:GET|HEAD)$/.test(this._options.method)) {
        this._options.method = "GET";
        this._requestBodyBuffers = [];
        removeMatchingHeaders(/^content-/i, this._options.headers);
      }
      var currentHostHeader = removeMatchingHeaders(/^host$/i, this._options.headers);
      var currentUrlParts = parseUrl2(this._currentUrl);
      var currentHost = currentHostHeader || currentUrlParts.host;
      var currentUrl = /^\w+:/.test(location) ? this._currentUrl : url2.format(Object.assign(currentUrlParts, { host: currentHost }));
      var redirectUrl = resolveUrl(location, currentUrl);
      debug("redirecting to", redirectUrl.href);
      this._isRedirect = true;
      spreadUrlObject(redirectUrl, this._options);
      if (redirectUrl.protocol !== currentUrlParts.protocol && redirectUrl.protocol !== "https:" || redirectUrl.host !== currentHost && !isSubdomain(redirectUrl.host, currentHost)) {
        removeMatchingHeaders(this._headerFilter, this._options.headers);
      }
      if (isFunction3(beforeRedirect)) {
        var responseDetails = {
          headers: response.headers,
          statusCode
        };
        var requestDetails = {
          url: currentUrl,
          method,
          headers: requestHeaders
        };
        beforeRedirect(this._options, responseDetails, requestDetails);
        this._sanitizeOptions(this._options);
      }
      this._performRequest();
    };
    function wrap(protocols) {
      var exports3 = {
        maxRedirects: 21,
        maxBodyLength: 10 * 1024 * 1024
      };
      var nativeProtocols = {};
      Object.keys(protocols).forEach(function(scheme) {
        var protocol = scheme + ":";
        var nativeProtocol = nativeProtocols[protocol] = protocols[scheme];
        var wrappedProtocol = exports3[scheme] = Object.create(nativeProtocol);
        function request(input, options, callback) {
          if (isURL(input)) {
            input = spreadUrlObject(input);
          } else if (isString2(input)) {
            input = spreadUrlObject(parseUrl2(input));
          } else {
            callback = options;
            options = validateUrl(input);
            input = { protocol };
          }
          if (isFunction3(options)) {
            callback = options;
            options = null;
          }
          options = Object.assign({
            maxRedirects: exports3.maxRedirects,
            maxBodyLength: exports3.maxBodyLength
          }, input, options);
          options.nativeProtocols = nativeProtocols;
          if (!isString2(options.host) && !isString2(options.hostname)) {
            options.hostname = "::1";
          }
          assert.equal(options.protocol, protocol, "protocol mismatch");
          debug("options", options);
          return new RedirectableRequest(options, callback);
        }
        function get(input, options, callback) {
          var wrappedRequest = wrappedProtocol.request(input, options, callback);
          wrappedRequest.end();
          return wrappedRequest;
        }
        Object.defineProperties(wrappedProtocol, {
          request: { value: request, configurable: true, enumerable: true, writable: true },
          get: { value: get, configurable: true, enumerable: true, writable: true }
        });
      });
      return exports3;
    }
    function noop2() {
    }
    function parseUrl2(input) {
      var parsed;
      if (useNativeURL) {
        parsed = new URL2(input);
      } else {
        parsed = validateUrl(url2.parse(input));
        if (!isString2(parsed.protocol)) {
          throw new InvalidUrlError({ input });
        }
      }
      return parsed;
    }
    function resolveUrl(relative2, base) {
      return useNativeURL ? new URL2(relative2, base) : parseUrl2(url2.resolve(base, relative2));
    }
    function validateUrl(input) {
      if (/^\[/.test(input.hostname) && !/^\[[:0-9a-f]+\]$/i.test(input.hostname)) {
        throw new InvalidUrlError({ input: input.href || input });
      }
      if (/^\[/.test(input.host) && !/^\[[:0-9a-f]+\](:\d+)?$/i.test(input.host)) {
        throw new InvalidUrlError({ input: input.href || input });
      }
      return input;
    }
    function spreadUrlObject(urlObject, target) {
      var spread3 = target || {};
      for (var key of preservedUrlFields) {
        spread3[key] = urlObject[key];
      }
      if (spread3.hostname.startsWith("[")) {
        spread3.hostname = spread3.hostname.slice(1, -1);
      }
      if (spread3.port !== "") {
        spread3.port = Number(spread3.port);
      }
      spread3.path = spread3.search ? spread3.pathname + spread3.search : spread3.pathname;
      return spread3;
    }
    function removeMatchingHeaders(regex, headers) {
      var lastValue;
      for (var header in headers) {
        if (regex.test(header)) {
          lastValue = headers[header];
          delete headers[header];
        }
      }
      return lastValue === null || typeof lastValue === "undefined" ? void 0 : String(lastValue).trim();
    }
    function createErrorType(code, message, baseClass) {
      function CustomError(properties) {
        if (isFunction3(Error.captureStackTrace)) {
          Error.captureStackTrace(this, this.constructor);
        }
        Object.assign(this, properties || {});
        this.code = code;
        this.message = this.cause ? message + ": " + this.cause.message : message;
      }
      CustomError.prototype = new (baseClass || Error)();
      Object.defineProperties(CustomError.prototype, {
        constructor: {
          value: CustomError,
          enumerable: false
        },
        name: {
          value: "Error [" + code + "]",
          enumerable: false
        }
      });
      return CustomError;
    }
    function destroyRequest(request, error) {
      for (var event of events) {
        request.removeListener(event, eventHandlers[event]);
      }
      request.on("error", noop2);
      request.destroy(error);
    }
    function isSubdomain(subdomain, domain) {
      assert(isString2(subdomain) && isString2(domain));
      var dot = subdomain.length - domain.length - 1;
      return dot > 0 && subdomain[dot] === "." && subdomain.endsWith(domain);
    }
    function isArray2(value) {
      return value instanceof Array;
    }
    function isString2(value) {
      return typeof value === "string" || value instanceof String;
    }
    function isFunction3(value) {
      return typeof value === "function";
    }
    function isBuffer2(value) {
      return typeof value === "object" && "length" in value;
    }
    function isURL(value) {
      return URL2 && value instanceof URL2;
    }
    function escapeRegex(regex) {
      return regex.replace(/[\]\\/()*+?.$]/g, "\\$&");
    }
    module2.exports = wrap({ http: http3, https: https2 });
    module2.exports.wrap = wrap;
  }
});

// ../../node_modules/.pnpm/kiwi-schema@0.5.0/node_modules/kiwi-schema/kiwi.js
var require_kiwi = __commonJS({
  "../../node_modules/.pnpm/kiwi-schema@0.5.0/node_modules/kiwi-schema/kiwi.js"(exports2, module2) {
    "use strict";
    var __defProp2 = Object.defineProperty;
    var __getOwnPropDesc2 = Object.getOwnPropertyDescriptor;
    var __getOwnPropNames2 = Object.getOwnPropertyNames;
    var __hasOwnProp2 = Object.prototype.hasOwnProperty;
    var __export2 = (target, all3) => {
      for (var name in all3)
        __defProp2(target, name, { get: all3[name], enumerable: true });
    };
    var __copyProps2 = (to, from, except, desc) => {
      if (from && typeof from === "object" || typeof from === "function") {
        for (let key of __getOwnPropNames2(from))
          if (!__hasOwnProp2.call(to, key) && key !== except)
            __defProp2(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc2(from, key)) || desc.enumerable });
      }
      return to;
    };
    var __toCommonJS2 = (mod) => __copyProps2(__defProp2({}, "__esModule", { value: true }), mod);
    var kiwi_exports = {};
    __export2(kiwi_exports, {
      ByteBuffer: () => ByteBuffer,
      compileSchema: () => compileSchema2,
      compileSchemaCPP: () => compileSchemaCPP,
      compileSchemaCallbackCPP: () => compileSchemaCallbackCPP,
      compileSchemaJS: () => compileSchemaJS,
      compileSchemaSkew: () => compileSchemaSkew,
      compileSchemaSkewTypes: () => compileSchemaSkewTypes,
      compileSchemaTypeScript: () => compileSchemaTypeScript,
      decodeBinarySchema: () => decodeBinarySchema,
      encodeBinarySchema: () => encodeBinarySchema,
      parseSchema: () => parseSchema2,
      prettyPrintSchema: () => prettyPrintSchema
    });
    module2.exports = __toCommonJS2(kiwi_exports);
    var int32 = new Int32Array(1);
    var float32 = new Float32Array(int32.buffer);
    var ByteBuffer = class {
      constructor(data) {
        if (data && !(data instanceof Uint8Array)) {
          throw new Error("Must initialize a ByteBuffer with a Uint8Array");
        }
        this._data = data || new Uint8Array(256);
        this._index = 0;
        this.length = data ? data.length : 0;
      }
      toUint8Array() {
        return this._data.subarray(0, this.length);
      }
      readByte() {
        if (this._index + 1 > this._data.length) {
          throw new Error("Index out of bounds");
        }
        return this._data[this._index++];
      }
      readByteArray() {
        let length = this.readVarUint();
        let start = this._index;
        let end = start + length;
        if (end > this._data.length) {
          throw new Error("Read array out of bounds");
        }
        this._index = end;
        let result = new Uint8Array(length);
        result.set(this._data.subarray(start, end));
        return result;
      }
      readVarFloat() {
        let index = this._index;
        let data = this._data;
        let length = data.length;
        if (index + 1 > length) {
          throw new Error("Index out of bounds");
        }
        let first = data[index];
        if (first === 0) {
          this._index = index + 1;
          return 0;
        }
        if (index + 4 > length) {
          throw new Error("Index out of bounds");
        }
        let bits = first | data[index + 1] << 8 | data[index + 2] << 16 | data[index + 3] << 24;
        this._index = index + 4;
        bits = bits << 23 | bits >>> 9;
        int32[0] = bits;
        return float32[0];
      }
      readVarUint() {
        let value = 0;
        let shift = 0;
        do {
          var byte = this.readByte();
          value |= (byte & 127) << shift;
          shift += 7;
        } while (byte & 128 && shift < 35);
        return value >>> 0;
      }
      readVarInt() {
        let value = this.readVarUint() | 0;
        return value & 1 ? ~(value >>> 1) : value >>> 1;
      }
      readVarUint64() {
        let value = BigInt(0);
        let shift = BigInt(0);
        let seven = BigInt(7);
        let byte;
        while ((byte = this.readByte()) & 128 && shift < 56) {
          value |= BigInt(byte & 127) << shift;
          shift += seven;
        }
        value |= BigInt(byte) << shift;
        return value;
      }
      readVarInt64() {
        let value = this.readVarUint64();
        let one = BigInt(1);
        let sign = value & one;
        value >>= one;
        return sign ? ~value : value;
      }
      readString() {
        let result = "";
        while (true) {
          let codePoint;
          let a = this.readByte();
          if (a < 192) {
            codePoint = a;
          } else {
            let b = this.readByte();
            if (a < 224) {
              codePoint = (a & 31) << 6 | b & 63;
            } else {
              let c = this.readByte();
              if (a < 240) {
                codePoint = (a & 15) << 12 | (b & 63) << 6 | c & 63;
              } else {
                let d = this.readByte();
                codePoint = (a & 7) << 18 | (b & 63) << 12 | (c & 63) << 6 | d & 63;
              }
            }
          }
          if (codePoint === 0) {
            break;
          }
          if (codePoint < 65536) {
            result += String.fromCharCode(codePoint);
          } else {
            codePoint -= 65536;
            result += String.fromCharCode((codePoint >> 10) + 55296, (codePoint & (1 << 10) - 1) + 56320);
          }
        }
        return result;
      }
      _growBy(amount) {
        if (this.length + amount > this._data.length) {
          let data = new Uint8Array(this.length + amount << 1);
          data.set(this._data);
          this._data = data;
        }
        this.length += amount;
      }
      writeByte(value) {
        let index = this.length;
        this._growBy(1);
        this._data[index] = value;
      }
      writeByteArray(value) {
        this.writeVarUint(value.length);
        let index = this.length;
        this._growBy(value.length);
        this._data.set(value, index);
      }
      writeVarFloat(value) {
        let index = this.length;
        float32[0] = value;
        let bits = int32[0];
        bits = bits >>> 23 | bits << 9;
        if ((bits & 255) === 0) {
          this.writeByte(0);
          return;
        }
        this._growBy(4);
        let data = this._data;
        data[index] = bits;
        data[index + 1] = bits >> 8;
        data[index + 2] = bits >> 16;
        data[index + 3] = bits >> 24;
      }
      writeVarUint(value) {
        if (value < 0 || value > 4294967295)
          throw new Error("Outside uint range: " + value);
        do {
          let byte = value & 127;
          value >>>= 7;
          this.writeByte(value ? byte | 128 : byte);
        } while (value);
      }
      writeVarInt(value) {
        if (value < -2147483648 || value > 2147483647)
          throw new Error("Outside int range: " + value);
        this.writeVarUint((value << 1 ^ value >> 31) >>> 0);
      }
      writeVarUint64(value) {
        if (typeof value === "string")
          value = BigInt(value);
        else if (typeof value !== "bigint")
          throw new Error("Expected bigint but got " + typeof value + ": " + value);
        if (value < 0 || value > BigInt("0xFFFFFFFFFFFFFFFF"))
          throw new Error("Outside uint64 range: " + value);
        let mask = BigInt(127);
        let seven = BigInt(7);
        for (let i = 0; value > mask && i < 8; i++) {
          this.writeByte(Number(value & mask) | 128);
          value >>= seven;
        }
        this.writeByte(Number(value));
      }
      writeVarInt64(value) {
        if (typeof value === "string")
          value = BigInt(value);
        else if (typeof value !== "bigint")
          throw new Error("Expected bigint but got " + typeof value + ": " + value);
        if (value < -BigInt("0x8000000000000000") || value > BigInt("0x7FFFFFFFFFFFFFFF"))
          throw new Error("Outside int64 range: " + value);
        let one = BigInt(1);
        this.writeVarUint64(value < 0 ? ~(value << one) : value << one);
      }
      writeString(value) {
        let codePoint;
        for (let i = 0; i < value.length; i++) {
          let a = value.charCodeAt(i);
          if (i + 1 === value.length || a < 55296 || a >= 56320) {
            codePoint = a;
          } else {
            let b = value.charCodeAt(++i);
            codePoint = (a << 10) + b + (65536 - (55296 << 10) - 56320);
          }
          if (codePoint === 0) {
            throw new Error("Cannot encode a string containing the null character");
          }
          if (codePoint < 128) {
            this.writeByte(codePoint);
          } else {
            if (codePoint < 2048) {
              this.writeByte(codePoint >> 6 & 31 | 192);
            } else {
              if (codePoint < 65536) {
                this.writeByte(codePoint >> 12 & 15 | 224);
              } else {
                this.writeByte(codePoint >> 18 & 7 | 240);
                this.writeByte(codePoint >> 12 & 63 | 128);
              }
              this.writeByte(codePoint >> 6 & 63 | 128);
            }
            this.writeByte(codePoint & 63 | 128);
          }
        }
        this.writeByte(0);
      }
    };
    function quote(text) {
      return JSON.stringify(text);
    }
    function error(text, line, column) {
      var error2 = new Error(text);
      error2.line = line;
      error2.column = column;
      throw error2;
    }
    function compileDecode(definition, definitions) {
      let lines = [];
      let indent = "  ";
      lines.push("function (bb) {");
      lines.push("  var result = {};");
      lines.push("  if (!(bb instanceof this.ByteBuffer)) {");
      lines.push("    bb = new this.ByteBuffer(bb);");
      lines.push("  }");
      lines.push("");
      if (definition.kind === "MESSAGE") {
        lines.push("  while (true) {");
        lines.push("    switch (bb.readVarUint()) {");
        lines.push("      case 0:");
        lines.push("        return result;");
        lines.push("");
        indent = "        ";
      }
      for (let i = 0; i < definition.fields.length; i++) {
        let field = definition.fields[i];
        let code;
        switch (field.type) {
          case "bool": {
            code = "!!bb.readByte()";
            break;
          }
          case "byte": {
            code = "bb.readByte()";
            break;
          }
          case "int": {
            code = "bb.readVarInt()";
            break;
          }
          case "uint": {
            code = "bb.readVarUint()";
            break;
          }
          case "float": {
            code = "bb.readVarFloat()";
            break;
          }
          case "string": {
            code = "bb.readString()";
            break;
          }
          case "int64": {
            code = "bb.readVarInt64()";
            break;
          }
          case "uint64": {
            code = "bb.readVarUint64()";
            break;
          }
          default: {
            let type = definitions[field.type];
            if (!type) {
              error("Invalid type " + quote(field.type) + " for field " + quote(field.name), field.line, field.column);
            } else if (type.kind === "ENUM") {
              code = "this[" + quote(type.name) + "][bb.readVarUint()]";
            } else {
              code = "this[" + quote("decode" + type.name) + "](bb)";
            }
          }
        }
        if (definition.kind === "MESSAGE") {
          lines.push("      case " + field.value + ":");
        }
        if (field.isArray) {
          if (field.isDeprecated) {
            if (field.type === "byte") {
              lines.push(indent + "bb.readByteArray();");
            } else {
              lines.push(indent + "var length = bb.readVarUint();");
              lines.push(indent + "while (length-- > 0) " + code + ";");
            }
          } else {
            if (field.type === "byte") {
              lines.push(indent + "result[" + quote(field.name) + "] = bb.readByteArray();");
            } else {
              lines.push(indent + "var length = bb.readVarUint();");
              lines.push(indent + "var values = result[" + quote(field.name) + "] = Array(length);");
              lines.push(indent + "for (var i = 0; i < length; i++) values[i] = " + code + ";");
            }
          }
        } else {
          if (field.isDeprecated) {
            lines.push(indent + code + ";");
          } else {
            lines.push(indent + "result[" + quote(field.name) + "] = " + code + ";");
          }
        }
        if (definition.kind === "MESSAGE") {
          lines.push("        break;");
          lines.push("");
        }
      }
      if (definition.kind === "MESSAGE") {
        lines.push("      default:");
        lines.push('        throw new Error("Attempted to parse invalid message");');
        lines.push("    }");
        lines.push("  }");
      } else {
        lines.push("  return result;");
      }
      lines.push("}");
      return lines.join("\n");
    }
    function compileEncode(definition, definitions) {
      let lines = [];
      lines.push("function (message, bb) {");
      lines.push("  var isTopLevel = !bb;");
      lines.push("  if (isTopLevel) bb = new this.ByteBuffer();");
      for (let j = 0; j < definition.fields.length; j++) {
        let field = definition.fields[j];
        let code;
        if (field.isDeprecated) {
          continue;
        }
        switch (field.type) {
          case "bool": {
            code = "bb.writeByte(value);";
            break;
          }
          case "byte": {
            code = "bb.writeByte(value);";
            break;
          }
          case "int": {
            code = "bb.writeVarInt(value);";
            break;
          }
          case "uint": {
            code = "bb.writeVarUint(value);";
            break;
          }
          case "float": {
            code = "bb.writeVarFloat(value);";
            break;
          }
          case "string": {
            code = "bb.writeString(value);";
            break;
          }
          case "int64": {
            code = "bb.writeVarInt64(value);";
            break;
          }
          case "uint64": {
            code = "bb.writeVarUint64(value);";
            break;
          }
          default: {
            let type = definitions[field.type];
            if (!type) {
              throw new Error("Invalid type " + quote(field.type) + " for field " + quote(field.name));
            } else if (type.kind === "ENUM") {
              code = "var encoded = this[" + quote(type.name) + '][value]; if (encoded === void 0) throw new Error("Invalid value " + JSON.stringify(value) + ' + quote(" for enum " + quote(type.name)) + "); bb.writeVarUint(encoded);";
            } else {
              code = "this[" + quote("encode" + type.name) + "](value, bb);";
            }
          }
        }
        lines.push("");
        lines.push("  var value = message[" + quote(field.name) + "];");
        lines.push("  if (value != null) {");
        if (definition.kind === "MESSAGE") {
          lines.push("    bb.writeVarUint(" + field.value + ");");
        }
        if (field.isArray) {
          if (field.type === "byte") {
            lines.push("    bb.writeByteArray(value);");
          } else {
            lines.push("    var values = value, n = values.length;");
            lines.push("    bb.writeVarUint(n);");
            lines.push("    for (var i = 0; i < n; i++) {");
            lines.push("      value = values[i];");
            lines.push("      " + code);
            lines.push("    }");
          }
        } else {
          lines.push("    " + code);
        }
        if (definition.kind === "STRUCT") {
          lines.push("  } else {");
          lines.push("    throw new Error(" + quote("Missing required field " + quote(field.name)) + ");");
        }
        lines.push("  }");
      }
      if (definition.kind === "MESSAGE") {
        lines.push("  bb.writeVarUint(0);");
      }
      lines.push("");
      lines.push("  if (isTopLevel) return bb.toUint8Array();");
      lines.push("}");
      return lines.join("\n");
    }
    function compileSchemaJS(schema) {
      let definitions = {};
      let name = schema.package;
      let js = [];
      if (name !== null) {
        js.push("var " + name + " = exports || " + name + " || {}, exports;");
      } else {
        js.push("var exports = exports || {};");
        name = "exports";
      }
      js.push(name + ".ByteBuffer = " + name + '.ByteBuffer || require("kiwi-schema").ByteBuffer;');
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        definitions[definition.name] = definition;
      }
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        switch (definition.kind) {
          case "ENUM": {
            let value = {};
            for (let j = 0; j < definition.fields.length; j++) {
              let field = definition.fields[j];
              value[field.name] = field.value;
              value[field.value] = field.name;
            }
            js.push(name + "[" + quote(definition.name) + "] = " + JSON.stringify(value, null, 2) + ";");
            break;
          }
          case "STRUCT":
          case "MESSAGE": {
            js.push("");
            js.push(name + "[" + quote("decode" + definition.name) + "] = " + compileDecode(definition, definitions) + ";");
            js.push("");
            js.push(name + "[" + quote("encode" + definition.name) + "] = " + compileEncode(definition, definitions) + ";");
            break;
          }
          default: {
            error("Invalid definition kind " + quote(definition.kind), definition.line, definition.column);
            break;
          }
        }
      }
      js.push("");
      return js.join("\n");
    }
    function compileSchema2(schema) {
      let result = {
        ByteBuffer
      };
      new Function("exports", compileSchemaJS(schema))(result);
      return result;
    }
    function argumentForField(definitions, type, name) {
      switch (type) {
        case "bool":
          return { type: "bool ", name };
        case "byte":
          return { type: "uint8_t ", name };
        case "int":
          return { type: "int32_t ", name };
        case "uint":
          return { type: "uint32_t ", name };
        case "float":
          return { type: "float ", name };
        case "string":
          return { type: "const char *", name };
        case "int64":
          return { type: "int64_t ", name };
        case "uint64":
          return { type: "uint64_t ", name };
        default: {
          let definition = definitions[type];
          if (definition.kind === "ENUM")
            return { type: definition.name + " ", name };
          return null;
        }
      }
    }
    function extractStructArguments(definitions, prefix, fields, allowArrays) {
      let args = [];
      for (let i = 0; i < fields.length; i++) {
        let field = fields[i];
        let name = prefix + field.name;
        if (field.isArray && !allowArrays) {
          return null;
        }
        let arg = argumentForField(definitions, field.type, name);
        if (arg !== null) {
          args.push(arg);
          continue;
        }
        let type = definitions[field.type];
        if (type.kind !== "STRUCT") {
          return null;
        }
        let typeArgs = extractStructArguments(definitions, name + "_", type.fields, false);
        if (typeArgs === null) {
          return null;
        }
        args.push.apply(args, typeArgs);
      }
      return args;
    }
    function argToDeclaration(arg) {
      return arg.type + arg.name;
    }
    function argToName(arg) {
      return arg.name;
    }
    function argToNotRead(arg) {
      switch (arg.type) {
        case "bool ":
        case "uint8_t ":
          return "!bb.readByte(" + arg.name + ")";
        case "int32_t ":
          return "!bb.readVarInt(" + arg.name + ")";
        case "uint32_t ":
          return "!bb.readVarUint(" + arg.name + ")";
        case "float ":
          return "!bb.readVarFloat(" + arg.name + ")";
        case "const char *":
          return "!bb.readString(" + arg.name + ")";
        case "int64_t ":
          return "!bb.readVarInt64(" + arg.name + ")";
        case "uint64_t ":
          return "!bb.readVarUint64(" + arg.name + ")";
        default:
          return "!bb.readVarUint(reinterpret_cast<uint32_t &>(" + arg.name + "))";
      }
    }
    function argToWrite(arg) {
      switch (arg.type) {
        case "bool ":
        case "uint8_t ":
          return "_bb.writeByte(" + arg.name + ")";
        case "int32_t ":
          return "_bb.writeVarInt(" + arg.name + ")";
        case "uint32_t ":
          return "_bb.writeVarUint(" + arg.name + ")";
        case "float ":
          return "_bb.writeVarFloat(" + arg.name + ")";
        case "const char *":
          return "_bb.writeString(" + arg.name + ")";
        case "int64_t ":
          return "_bb.writeVarInt64(" + arg.name + ")";
        case "uint64_t ":
          return "_bb.writeVarUint64(" + arg.name + ")";
        default:
          return "_bb.writeVarUint(static_cast<uint32_t>(" + arg.name + "))";
      }
    }
    function emitReadField(cpp, definitions, definition, field, indent) {
      let name = field.name;
      if (field.isArray) {
        let count = "_" + name + "_count";
        cpp.push(indent + "uint32_t " + count + ";");
        cpp.push(indent + "if (!bb.readVarUint(" + count + ")) return false;");
        if (!field.isDeprecated) {
          cpp.push(indent + "visitor.visit" + definition.name + "_" + field.name + "_count(" + count + ");");
        }
        cpp.push(indent + "while (" + count + "-- > 0) {");
        indent += "  ";
        name += "_element";
      }
      let args = extractStructArguments(definitions, "", [field], true);
      if (args !== null) {
        for (let i = 0; i < args.length; i++) {
          cpp.push(indent + argToDeclaration(args[i]) + ";");
        }
        cpp.push(indent + "if (" + args.map(argToNotRead).join(" || ") + ") return false;");
        if (!field.isDeprecated) {
          cpp.push(indent + "visitor.visit" + definition.name + "_" + name + "(" + args.map(argToName).join(", ") + ");");
        }
      } else {
        if (!field.isDeprecated) {
          cpp.push(indent + "visitor.visit" + definition.name + "_" + name + "();");
        }
        let type = definitions[field.type];
        cpp.push(indent + "if (!parse" + type.name + "(bb, visitor)) return false;");
      }
      if (field.isArray) {
        cpp.push(indent.slice(2) + "}");
      }
    }
    function compileSchemaCallbackCPP(schema) {
      let definitions = {};
      let cpp = [];
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        definitions[definition.name] = definition;
      }
      if (schema.package !== null) {
        cpp.push("#ifndef INCLUDE_" + schema.package.toUpperCase() + "_H");
        cpp.push("#define INCLUDE_" + schema.package.toUpperCase() + "_H");
        cpp.push("");
      }
      cpp.push('#include "kiwi.h"');
      cpp.push("");
      if (schema.package !== null) {
        cpp.push("namespace " + schema.package + " {");
        cpp.push("");
      }
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        if (definition.kind === "ENUM") {
          cpp.push("enum class " + definition.name + " : uint32_t {");
          for (let j = 0; j < definition.fields.length; j++) {
            let field = definition.fields[j];
            if (!field.isDeprecated) {
              cpp.push("  " + field.name + " = " + field.value + ",");
            }
          }
          cpp.push("};");
          cpp.push("");
        }
      }
      for (let pass = 0; pass < 2; pass++) {
        let suffix = ") = 0;";
        if (pass === 0) {
          cpp.push("class Visitor {");
          cpp.push("public:");
        } else {
          cpp.push("class Writer : public Visitor {");
          cpp.push("private:");
          cpp.push("  kiwi::ByteBuffer &_bb;");
          cpp.push("public:");
          cpp.push("  Writer(kiwi::ByteBuffer &bb) : _bb(bb) {}");
          suffix = ") override;";
        }
        for (let i = 0; i < schema.definitions.length; i++) {
          let definition = schema.definitions[i];
          if (definition.kind === "STRUCT") {
            let args = extractStructArguments(definitions, "", definition.fields, false);
            if (args !== null) {
              cpp.push("  virtual void visit" + definition.name + "(" + args.map(argToDeclaration).join(", ") + suffix);
              continue;
            }
          }
          if (definition.kind === "STRUCT" || definition.kind === "MESSAGE") {
            cpp.push("  virtual void begin" + definition.name + "(" + suffix);
            for (let j = 0; j < definition.fields.length; j++) {
              let field = definition.fields[j];
              if (field.isDeprecated) {
                continue;
              }
              let name = field.name;
              if (field.isArray) {
                cpp.push("  virtual void visit" + definition.name + "_" + field.name + "_count(uint32_t size" + suffix);
                name += "_element";
              }
              let args = extractStructArguments(definitions, "", [field], true);
              if (args !== null) {
                cpp.push("  virtual void visit" + definition.name + "_" + name + "(" + args.map(argToDeclaration).join(", ") + suffix);
              } else {
                cpp.push("  virtual void visit" + definition.name + "_" + name + "(" + suffix);
              }
            }
            cpp.push("  virtual void end" + definition.name + "(" + suffix);
          }
        }
        cpp.push("};");
        cpp.push("");
      }
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        if (definition.kind === "STRUCT" || definition.kind === "MESSAGE") {
          cpp.push("bool parse" + definition.name + "(kiwi::ByteBuffer &bb, Visitor &visitor);");
        }
      }
      cpp.push("");
      cpp.push("#ifdef IMPLEMENT_SCHEMA_H");
      cpp.push("");
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        if (definition.kind === "STRUCT") {
          let args = extractStructArguments(definitions, "", definition.fields, false);
          if (args !== null) {
            cpp.push("bool parse" + definition.name + "(kiwi::ByteBuffer &bb, Visitor &visitor) {");
            for (let j = 0; j < args.length; j++) {
              cpp.push("  " + argToDeclaration(args[j]) + ";");
            }
            cpp.push("  if (" + args.map(argToNotRead).join(" || ") + ") return false;");
            cpp.push("  visitor.visit" + definition.name + "(" + args.map(argToName).join(", ") + ");");
            cpp.push("  return true;");
            cpp.push("}");
            cpp.push("");
          } else {
            cpp.push("bool parse" + definition.name + "(kiwi::ByteBuffer &bb, Visitor &visitor) {");
            cpp.push("  visitor.begin" + definition.name + "();");
            for (let j = 0; j < definition.fields.length; j++) {
              let field = definition.fields[j];
              emitReadField(cpp, definitions, definition, field, "  ");
            }
            cpp.push("  visitor.end" + definition.name + "();");
            cpp.push("  return true;");
            cpp.push("}");
            cpp.push("");
          }
        } else if (definition.kind === "MESSAGE") {
          cpp.push("bool parse" + definition.name + "(kiwi::ByteBuffer &bb, Visitor &visitor) {");
          cpp.push("  visitor.begin" + definition.name + "();");
          cpp.push("  while (true) {");
          cpp.push("    uint32_t _type;");
          cpp.push("    if (!bb.readVarUint(_type)) return false;");
          cpp.push("    switch (_type) {");
          cpp.push("      case 0: {");
          cpp.push("        visitor.end" + definition.name + "();");
          cpp.push("        return true;");
          cpp.push("      }");
          for (let j = 0; j < definition.fields.length; j++) {
            let field = definition.fields[j];
            cpp.push("      case " + field.value + ": {");
            emitReadField(cpp, definitions, definition, field, "        ");
            cpp.push("        break;");
            cpp.push("      }");
          }
          cpp.push("      default: return false;");
          cpp.push("    }");
          cpp.push("  }");
          cpp.push("}");
          cpp.push("");
        }
      }
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        if (definition.kind === "STRUCT") {
          let args = extractStructArguments(definitions, "", definition.fields, false);
          if (args !== null) {
            cpp.push("void Writer::visit" + definition.name + "(" + args.map(argToDeclaration).join(", ") + ") {");
            for (let j = 0; j < args.length; j++) {
              cpp.push("  " + argToWrite(args[j]) + ";");
            }
            cpp.push("}");
            cpp.push("");
            continue;
          }
        }
        if (definition.kind === "STRUCT" || definition.kind === "MESSAGE") {
          cpp.push("void Writer::begin" + definition.name + "() {");
          cpp.push("}");
          cpp.push("");
          for (let j = 0; j < definition.fields.length; j++) {
            let field = definition.fields[j];
            if (field.isDeprecated) {
              continue;
            }
            let name = field.name;
            if (field.isArray) {
              cpp.push("void Writer::visit" + definition.name + "_" + field.name + "_count(uint32_t size) {");
              if (definition.kind === "MESSAGE") {
                cpp.push("  _bb.writeVarUint(" + field.value + ");");
              }
              cpp.push("  _bb.writeVarUint(size);");
              cpp.push("}");
              cpp.push("");
              name += "_element";
            }
            let args = extractStructArguments(definitions, "", [field], true);
            if (args !== null) {
              cpp.push("void Writer::visit" + definition.name + "_" + name + "(" + args.map(argToDeclaration).join(", ") + ") {");
              if (definition.kind === "MESSAGE" && !field.isArray) {
                cpp.push("  _bb.writeVarUint(" + field.value + ");");
              }
              for (let k = 0; k < args.length; k++) {
                cpp.push("  " + argToWrite(args[k]) + ";");
              }
              cpp.push("}");
              cpp.push("");
            } else {
              cpp.push("void Writer::visit" + definition.name + "_" + name + "() {");
              if (definition.kind === "MESSAGE" && !field.isArray) {
                cpp.push("  _bb.writeVarUint(" + field.value + ");");
              }
              cpp.push("}");
              cpp.push("");
            }
          }
          cpp.push("void Writer::end" + definition.name + "() {");
          if (definition.kind === "MESSAGE") {
            cpp.push("  _bb.writeVarUint(0);");
          }
          cpp.push("}");
          cpp.push("");
        }
      }
      cpp.push("#endif");
      cpp.push("");
      if (schema.package !== null) {
        cpp.push("}");
        cpp.push("");
        cpp.push("#endif");
        cpp.push("");
      }
      return cpp.join("\n");
    }
    function cppType(definitions, field, isArray2) {
      let type;
      switch (field.type) {
        case "bool":
          type = "bool";
          break;
        case "byte":
          type = "uint8_t";
          break;
        case "int":
          type = "int32_t";
          break;
        case "uint":
          type = "uint32_t";
          break;
        case "float":
          type = "float";
          break;
        case "string":
          type = "kiwi::String";
          break;
        case "int64":
          type = "int64_t";
          break;
        case "uint64":
          type = "uint64_t";
          break;
        default: {
          let definition = definitions[field.type];
          if (!definition) {
            error("Invalid type " + quote(field.type) + " for field " + quote(field.name), field.line, field.column);
          }
          type = definition.name;
          break;
        }
      }
      if (isArray2) {
        type = "kiwi::Array<" + type + ">";
      }
      return type;
    }
    function cppFieldName(field) {
      return "_data_" + field.name;
    }
    function cppFlagIndex(i) {
      return i >> 5;
    }
    function cppFlagMask(i) {
      return 1 << i % 32 >>> 0;
    }
    function cppIsFieldPointer(definitions, field) {
      return !field.isArray && field.type in definitions && definitions[field.type].kind !== "ENUM";
    }
    function compileSchemaCPP(schema) {
      let definitions = {};
      let cpp = [];
      cpp.push('#include "kiwi.h"');
      cpp.push("");
      if (schema.package !== null) {
        cpp.push("namespace " + schema.package + " {");
        cpp.push("");
        cpp.push("#ifndef INCLUDE_" + schema.package.toUpperCase() + "_H");
        cpp.push("#define INCLUDE_" + schema.package.toUpperCase() + "_H");
        cpp.push("");
      }
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        definitions[definition.name] = definition;
      }
      cpp.push("class BinarySchema {");
      cpp.push("public:");
      cpp.push("  bool parse(kiwi::ByteBuffer &bb);");
      cpp.push("  const kiwi::BinarySchema &underlyingSchema() const { return _schema; }");
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        if (definition.kind === "MESSAGE") {
          cpp.push("  bool skip" + definition.name + "Field(kiwi::ByteBuffer &bb, uint32_t id) const;");
        }
      }
      cpp.push("");
      cpp.push("private:");
      cpp.push("  kiwi::BinarySchema _schema;");
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        if (definition.kind === "MESSAGE") {
          cpp.push("  uint32_t _index" + definition.name + " = 0;");
        }
      }
      cpp.push("};");
      cpp.push("");
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        if (definition.kind === "ENUM") {
          cpp.push("enum class " + definition.name + " : uint32_t {");
          for (let j = 0; j < definition.fields.length; j++) {
            let field = definition.fields[j];
            cpp.push("  " + field.name + " = " + field.value + ",");
          }
          cpp.push("};");
          cpp.push("");
        } else if (definition.kind !== "STRUCT" && definition.kind !== "MESSAGE") {
          error("Invalid definition kind " + quote(definition.kind), definition.line, definition.column);
        }
      }
      for (let pass = 0; pass < 3; pass++) {
        let newline = false;
        if (pass === 2) {
          if (schema.package !== null) {
            cpp.push("#endif");
          }
          cpp.push("#ifdef IMPLEMENT_SCHEMA_H");
          cpp.push("");
          cpp.push("bool BinarySchema::parse(kiwi::ByteBuffer &bb) {");
          cpp.push("  if (!_schema.parse(bb)) return false;");
          for (let i = 0; i < schema.definitions.length; i++) {
            let definition = schema.definitions[i];
            if (definition.kind === "MESSAGE") {
              cpp.push('  _schema.findDefinition("' + definition.name + '", _index' + definition.name + ");");
            }
          }
          cpp.push("  return true;");
          cpp.push("}");
          cpp.push("");
          for (let i = 0; i < schema.definitions.length; i++) {
            let definition = schema.definitions[i];
            if (definition.kind === "MESSAGE") {
              cpp.push("bool BinarySchema::skip" + definition.name + "Field(kiwi::ByteBuffer &bb, uint32_t id) const {");
              cpp.push("  return _schema.skipField(bb, _index" + definition.name + ", id);");
              cpp.push("}");
              cpp.push("");
            }
          }
        }
        for (let i = 0; i < schema.definitions.length; i++) {
          let definition = schema.definitions[i];
          if (definition.kind === "ENUM") {
            continue;
          }
          let fields = definition.fields;
          if (pass === 0) {
            cpp.push("class " + definition.name + ";");
            newline = true;
          } else if (pass === 1) {
            cpp.push("class " + definition.name + " {");
            cpp.push("public:");
            cpp.push("  " + definition.name + "() { (void)_flags; }");
            cpp.push("");
            for (let j = 0; j < fields.length; j++) {
              let field = fields[j];
              if (field.isDeprecated) {
                continue;
              }
              let type = cppType(definitions, field, field.isArray);
              if (cppIsFieldPointer(definitions, field)) {
                cpp.push("  " + type + " *" + field.name + "();");
                cpp.push("  const " + type + " *" + field.name + "() const;");
                cpp.push("  void set_" + field.name + "(" + type + " *value);");
              } else if (field.isArray) {
                cpp.push("  " + type + " *" + field.name + "();");
                cpp.push("  const " + type + " *" + field.name + "() const;");
                cpp.push("  " + type + " &set_" + field.name + "(kiwi::MemoryPool &pool, uint32_t count);");
              } else {
                cpp.push("  " + type + " *" + field.name + "();");
                cpp.push("  const " + type + " *" + field.name + "() const;");
                cpp.push("  void set_" + field.name + "(const " + type + " &value);");
              }
              cpp.push("");
            }
            cpp.push("  bool encode(kiwi::ByteBuffer &bb);");
            cpp.push("  bool decode(kiwi::ByteBuffer &bb, kiwi::MemoryPool &pool, const BinarySchema *schema = nullptr);");
            cpp.push("");
            cpp.push("private:");
            cpp.push("  uint32_t _flags[" + (fields.length + 31 >> 5) + "] = {};");
            let sizes = { "bool": 1, "byte": 1, "int": 4, "uint": 4, "float": 4 };
            let sortedFields = fields.slice().sort(function(a, b) {
              let sizeA = !a.isArray && sizes[a.type] || 8;
              let sizeB = !b.isArray && sizes[b.type] || 8;
              if (sizeA !== sizeB)
                return sizeB - sizeA;
              return fields.indexOf(a) - fields.indexOf(b);
            });
            for (let j = 0; j < sortedFields.length; j++) {
              let field = sortedFields[j];
              if (field.isDeprecated) {
                continue;
              }
              let name = cppFieldName(field);
              let type = cppType(definitions, field, field.isArray);
              if (cppIsFieldPointer(definitions, field)) {
                cpp.push("  " + type + " *" + name + " = {};");
              } else {
                cpp.push("  " + type + " " + name + " = {};");
              }
            }
            cpp.push("};");
            cpp.push("");
          } else {
            for (let j = 0; j < fields.length; j++) {
              let field = fields[j];
              let name = cppFieldName(field);
              let type = cppType(definitions, field, field.isArray);
              let flagIndex = cppFlagIndex(j);
              let flagMask = cppFlagMask(j);
              if (field.isDeprecated) {
                continue;
              }
              if (cppIsFieldPointer(definitions, field)) {
                cpp.push(type + " *" + definition.name + "::" + field.name + "() {");
                cpp.push("  return " + name + ";");
                cpp.push("}");
                cpp.push("");
                cpp.push("const " + type + " *" + definition.name + "::" + field.name + "() const {");
                cpp.push("  return " + name + ";");
                cpp.push("}");
                cpp.push("");
                cpp.push("void " + definition.name + "::set_" + field.name + "(" + type + " *value) {");
                cpp.push("  " + name + " = value;");
                cpp.push("}");
                cpp.push("");
              } else if (field.isArray) {
                cpp.push(type + " *" + definition.name + "::" + field.name + "() {");
                cpp.push("  return _flags[" + flagIndex + "] & " + flagMask + " ? &" + name + " : nullptr;");
                cpp.push("}");
                cpp.push("");
                cpp.push("const " + type + " *" + definition.name + "::" + field.name + "() const {");
                cpp.push("  return _flags[" + flagIndex + "] & " + flagMask + " ? &" + name + " : nullptr;");
                cpp.push("}");
                cpp.push("");
                cpp.push(type + " &" + definition.name + "::set_" + field.name + "(kiwi::MemoryPool &pool, uint32_t count) {");
                cpp.push("  _flags[" + flagIndex + "] |= " + flagMask + "; return " + name + " = pool.array<" + cppType(definitions, field, false) + ">(count);");
                cpp.push("}");
                cpp.push("");
              } else {
                cpp.push(type + " *" + definition.name + "::" + field.name + "() {");
                cpp.push("  return _flags[" + flagIndex + "] & " + flagMask + " ? &" + name + " : nullptr;");
                cpp.push("}");
                cpp.push("");
                cpp.push("const " + type + " *" + definition.name + "::" + field.name + "() const {");
                cpp.push("  return _flags[" + flagIndex + "] & " + flagMask + " ? &" + name + " : nullptr;");
                cpp.push("}");
                cpp.push("");
                cpp.push("void " + definition.name + "::set_" + field.name + "(const " + type + " &value) {");
                cpp.push("  _flags[" + flagIndex + "] |= " + flagMask + "; " + name + " = value;");
                cpp.push("}");
                cpp.push("");
              }
            }
            cpp.push("bool " + definition.name + "::encode(kiwi::ByteBuffer &_bb) {");
            for (let j = 0; j < fields.length; j++) {
              let field = fields[j];
              if (field.isDeprecated) {
                continue;
              }
              let name = cppFieldName(field);
              let value = field.isArray ? "_it" : name;
              let flagIndex = cppFlagIndex(j);
              let flagMask = cppFlagMask(j);
              let code;
              switch (field.type) {
                case "bool": {
                  code = "_bb.writeByte(" + value + ");";
                  break;
                }
                case "byte": {
                  code = "_bb.writeByte(" + value + ");";
                  break;
                }
                case "int": {
                  code = "_bb.writeVarInt(" + value + ");";
                  break;
                }
                case "uint": {
                  code = "_bb.writeVarUint(" + value + ");";
                  break;
                }
                case "float": {
                  code = "_bb.writeVarFloat(" + value + ");";
                  break;
                }
                case "string": {
                  code = "_bb.writeString(" + value + ".c_str());";
                  break;
                }
                case "int64": {
                  code = "_bb.writeVarInt64(" + value + ");";
                  break;
                }
                case "uint64": {
                  code = "_bb.writeVarUint64(" + value + ");";
                  break;
                }
                default: {
                  let type = definitions[field.type];
                  if (!type) {
                    error("Invalid type " + quote(field.type) + " for field " + quote(field.name), field.line, field.column);
                  } else if (type.kind === "ENUM") {
                    code = "_bb.writeVarUint(static_cast<uint32_t>(" + value + "));";
                  } else {
                    code = "if (!" + value + (cppIsFieldPointer(definitions, field) ? "->" : ".") + "encode(_bb)) return false;";
                  }
                }
              }
              let indent = "  ";
              if (definition.kind === "STRUCT") {
                cpp.push("  if (" + field.name + "() == nullptr) return false;");
              } else {
                cpp.push("  if (" + field.name + "() != nullptr) {");
                indent = "    ";
              }
              if (definition.kind === "MESSAGE") {
                cpp.push(indent + "_bb.writeVarUint(" + field.value + ");");
              }
              if (field.isArray) {
                cpp.push(indent + "_bb.writeVarUint(" + name + ".size());");
                cpp.push(indent + "for (" + cppType(definitions, field, false) + " &_it : " + name + ") " + code);
              } else {
                cpp.push(indent + code);
              }
              if (definition.kind !== "STRUCT") {
                cpp.push("  }");
              }
            }
            if (definition.kind === "MESSAGE") {
              cpp.push("  _bb.writeVarUint(0);");
            }
            cpp.push("  return true;");
            cpp.push("}");
            cpp.push("");
            cpp.push("bool " + definition.name + "::decode(kiwi::ByteBuffer &_bb, kiwi::MemoryPool &_pool, const BinarySchema *_schema) {");
            for (let j = 0; j < fields.length; j++) {
              if (fields[j].isArray) {
                cpp.push("  uint32_t _count;");
                break;
              }
            }
            if (definition.kind === "MESSAGE") {
              cpp.push("  while (true) {");
              cpp.push("    uint32_t _type;");
              cpp.push("    if (!_bb.readVarUint(_type)) return false;");
              cpp.push("    switch (_type) {");
              cpp.push("      case 0:");
              cpp.push("        return true;");
            }
            for (let j = 0; j < fields.length; j++) {
              let field = fields[j];
              let name = cppFieldName(field);
              let value = field.isArray ? "_it" : name;
              let isPointer = cppIsFieldPointer(definitions, field);
              let code;
              switch (field.type) {
                case "bool": {
                  code = "_bb.readByte(" + value + ")";
                  break;
                }
                case "byte": {
                  code = "_bb.readByte(" + value + ")";
                  break;
                }
                case "int": {
                  code = "_bb.readVarInt(" + value + ")";
                  break;
                }
                case "uint": {
                  code = "_bb.readVarUint(" + value + ")";
                  break;
                }
                case "float": {
                  code = "_bb.readVarFloat(" + value + ")";
                  break;
                }
                case "string": {
                  code = "_bb.readString(" + value + ", _pool)";
                  break;
                }
                case "int64": {
                  code = "_bb.readVarInt64(" + value + ")";
                  break;
                }
                case "uint64": {
                  code = "_bb.readVarUint64(" + value + ")";
                  break;
                }
                default: {
                  let type2 = definitions[field.type];
                  if (!type2) {
                    error("Invalid type " + quote(field.type) + " for field " + quote(field.name), field.line, field.column);
                  } else if (type2.kind === "ENUM") {
                    code = "_bb.readVarUint(reinterpret_cast<uint32_t &>(" + value + "))";
                  } else {
                    code = value + (isPointer ? "->" : ".") + "decode(_bb, _pool, _schema)";
                  }
                }
              }
              let type = cppType(definitions, field, false);
              let indent = "  ";
              if (definition.kind === "MESSAGE") {
                cpp.push("      case " + field.value + ": {");
                indent = "        ";
              }
              if (field.isArray) {
                cpp.push(indent + "if (!_bb.readVarUint(_count)) return false;");
                if (field.isDeprecated) {
                  cpp.push(indent + "for (" + type + " &_it : _pool.array<" + cppType(definitions, field, false) + ">(_count)) if (!" + code + ") return false;");
                } else {
                  cpp.push(indent + "for (" + type + " &_it : set_" + field.name + "(_pool, _count)) if (!" + code + ") return false;");
                }
              } else {
                if (field.isDeprecated) {
                  if (isPointer) {
                    cpp.push(indent + type + " *" + name + " = _pool.allocate<" + type + ">();");
                  } else {
                    cpp.push(indent + type + " " + name + " = {};");
                  }
                  cpp.push(indent + "if (!" + code + ") return false;");
                } else {
                  if (isPointer) {
                    cpp.push(indent + name + " = _pool.allocate<" + type + ">();");
                  }
                  cpp.push(indent + "if (!" + code + ") return false;");
                  if (!isPointer) {
                    cpp.push(indent + "set_" + field.name + "(" + name + ");");
                  }
                }
              }
              if (definition.kind === "MESSAGE") {
                cpp.push("        break;");
                cpp.push("      }");
              }
            }
            if (definition.kind === "MESSAGE") {
              cpp.push("      default: {");
              cpp.push("        if (!_schema || !_schema->skip" + definition.name + "Field(_bb, _type)) return false;");
              cpp.push("        break;");
              cpp.push("      }");
              cpp.push("    }");
              cpp.push("  }");
            } else {
              cpp.push("  return true;");
            }
            cpp.push("}");
            cpp.push("");
          }
        }
        if (pass === 2) {
          cpp.push("#endif");
          cpp.push("");
        } else if (newline)
          cpp.push("");
      }
      if (schema.package !== null) {
        cpp.push("}");
        cpp.push("");
      }
      return cpp.join("\n");
    }
    function popTrailingNewline(lines) {
      if (lines[lines.length - 1] === "") {
        lines.pop();
      }
    }
    function skewDefaultValueForField(definitions, field) {
      if (field.isArray) {
        return "null";
      }
      switch (field.type) {
        case "bool":
          return "false";
        case "byte":
        case "int":
        case "uint":
          return "0";
        case "float":
          return "0.0";
        case "string":
          return "null";
        case "int64":
        case "uint64":
          return "BigInt.new(0)";
      }
      let def = definitions[field.type];
      if (def.kind === "ENUM") {
        if (def.fields.length > 0) {
          return "." + def.fields[0].name;
        }
        return "0 as " + field.type;
      }
      return "null";
    }
    function skewTypeForField(field) {
      let type;
      switch (field.type) {
        case "bool":
          type = "bool";
          break;
        case "byte":
        case "int":
        case "uint":
          type = "int";
          break;
        case "float":
          type = "double";
          break;
        case "string":
          type = "string";
          break;
        case "int64":
        case "uint64":
          type = "BigInt";
          break;
        default:
          type = field.type;
          break;
      }
      if (field.isArray) {
        type = "List<" + type + ">";
      }
      return type;
    }
    function compileSchemaSkew(schema) {
      let definitions = {};
      let indent = "";
      let lines = [];
      if (schema.package !== null) {
        lines.push("namespace " + schema.package + " {");
        indent += "  ";
      }
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        definitions[definition.name] = definition;
      }
      lines.push(indent + "class BinarySchema {");
      lines.push(indent + "  var _schema = Kiwi.BinarySchema.new");
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        if (definition.kind === "MESSAGE") {
          lines.push(indent + "  var _index" + definition.name + " = 0");
        }
      }
      lines.push("");
      lines.push(indent + "  def parse(bytes Uint8Array) {");
      lines.push(indent + "    _schema.parse(Kiwi.ByteBuffer.new(bytes))");
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        if (definition.kind === "MESSAGE") {
          lines.push(indent + "    _index" + definition.name + ' = _schema.findDefinition("' + definition.name + '")');
        }
      }
      lines.push(indent + "  }");
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        if (definition.kind === "MESSAGE") {
          lines.push("");
          lines.push(indent + "  def skip" + definition.name + "Field(bb Kiwi.ByteBuffer, id int) {");
          lines.push(indent + "    _schema.skipField(bb, _index" + definition.name + ", id)");
          lines.push(indent + "  }");
        }
      }
      lines.push(indent + "}");
      lines.push("");
      for (let i = 0; i < schema.definitions.length; i++) {
        let definition = schema.definitions[i];
        switch (definition.kind) {
          case "ENUM": {
            let encode3 = {};
            let decode = {};
            lines.push(indent + "enum " + definition.name + " {");
            for (let j = 0; j < definition.fields.length; j++) {
              let field = definition.fields[j];
              encode3[field.name] = field.value;
              decode[field.value] = field.name;
              lines.push(indent + "  " + field.name);
            }
            lines.push(indent + "}");
            lines.push("");
            lines.push(indent + "namespace " + definition.name + " {");
            lines.push(indent + "  const _encode = " + JSON.stringify(encode3, null, 2).replace(/"/g, "").replace(/\n/g, "\n  " + indent));
            lines.push("");
            lines.push(indent + "  const _decode = " + JSON.stringify(decode, null, 2).replace(/"/g, "").replace(/\n/g, "\n  " + indent));
            lines.push("");
            lines.push(indent + "  def encode(value " + definition.name + ") int {");
            lines.push(indent + "    return _encode[value]");
            lines.push(indent + "  }");
            lines.push("");
            lines.push(indent + "  def decode(value int) " + definition.name + " {");
            lines.push(indent + "    if !(value in _decode) {");
            lines.push(indent + "      Kiwi.DecodeError.throwInvalidEnumValue(" + quote(definition.name) + ")");
            lines.push(indent + "    }");
            lines.push(indent + "    return _decode[value]");
            lines.push(indent + "  }");
            lines.push(indent + "}");
            lines.push("");
            break;
          }
          case "STRUCT":
          case "MESSAGE": {
            lines.push(indent + "class " + definition.name + " {");
            for (let j = 0; j < definition.fields.length; j += 32) {
              lines.push(indent + "  var _flags" + (j >> 5) + " = 0");
            }
            for (let j = 0; j < definition.fields.length; j++) {
              let field = definition.fields[j];
              if (field.isDeprecated) {
                continue;
              }
              lines.push(indent + "  var _" + field.name + " " + skewTypeForField(field) + " = " + skewDefaultValueForField(definitions, field));
            }
            lines.push("");
            for (let j = 0; j < definition.fields.length; j++) {
              let field = definition.fields[j];
              if (field.isDeprecated) {
                continue;
              }
              let type = skewTypeForField(field);
              let flags = "_flags" + (j >> 5);
              let mask = "" + (1 << j % 32 >>> 0);
              lines.push(indent + "  def has_" + field.name + " bool {");
              lines.push(indent + "    return (" + flags + " & " + mask + ") != 0");
              lines.push(indent + "  }");
              lines.push("");
              lines.push(indent + "  def " + field.name + " " + type + " {");
              lines.push(indent + "    assert(has_" + field.name + ")");
              lines.push(indent + "    return _" + field.name);
              lines.push(indent + "  }");
              lines.push("");
              lines.push(indent + "  def " + field.name + "=(value " + type + ") {");
              lines.push(indent + "    _" + field.name + " = value");
              lines.push(indent + "    " + flags + " |= " + mask);
              lines.push(indent + "  }");
              lines.push("");
            }
            lines.push(indent + "  def encode(bb Kiwi.ByteBuffer) {");
            for (let j = 0; j < definition.fields.length; j++) {
              let field = definition.fields[j];
              if (field.isDeprecated) {
                continue;
              }
              let value = "_" + field.name;
              let code;
              if (field.isArray) {
                value = "value";
              }
              switch (field.type) {
                case "bool": {
                  code = "bb.writeByte(" + value + " as int)";
                  break;
                }
                case "byte": {
                  code = "bb.writeByte(" + value + ")";
                  break;
                }
                case "int": {
                  code = "bb.writeVarInt(" + value + ")";
                  break;
                }
                case "uint": {
                  code = "bb.writeVarUint(" + value + ")";
                  break;
                }
                case "float": {
                  code = "bb.writeVarFloat(" + value + ")";
                  break;
                }
                case "string": {
                  code = "bb.writeString(" + value + ")";
                  break;
                }
                case "int64": {
                  code = "bb.writeVarInt64(" + value + ")";
                  break;
                }
                case "uint64": {
                  code = "bb.writeVarUint64(" + value + ")";
                  break;
                }
                default: {
                  let type = definitions[field.type];
                  if (!type) {
                    error("Invalid type " + quote(field.type) + " for field " + quote(field.name), field.line, field.column);
                  } else if (type.kind === "ENUM") {
                    code = "bb.writeVarUint(" + type.name + ".encode(" + value + "))";
                  } else {
                    code = value + ".encode(bb)";
                  }
                }
              }
              let nestedIndent2 = indent + "    ";
              if (definition.kind === "STRUCT") {
                lines.push(nestedIndent2 + "assert(has_" + field.name + ")");
              } else {
                lines.push(nestedIndent2 + "if has_" + field.name + " {");
                nestedIndent2 += "  ";
              }
              if (definition.kind === "MESSAGE") {
                lines.push(nestedIndent2 + "bb.writeVarUint(" + field.value + ")");
              }
              if (field.isArray) {
                lines.push(nestedIndent2 + "bb.writeVarUint(_" + field.name + ".count)");
                lines.push(nestedIndent2 + "for value in _" + field.name + " {");
                lines.push(nestedIndent2 + "  " + code);
                lines.push(nestedIndent2 + "}");
              } else {
                lines.push(nestedIndent2 + code);
              }
              if (definition.kind !== "STRUCT") {
                lines.push(indent + "    }");
              }
              lines.push("");
            }
            if (definition.kind === "MESSAGE") {
              lines.push(indent + "    bb.writeVarUint(0)");
            } else {
              popTrailingNewline(lines);
            }
            lines.push(indent + "  }");
            lines.push("");
            lines.push(indent + "  def encode Uint8Array {");
            lines.push(indent + "    var bb = Kiwi.ByteBuffer.new");
            lines.push(indent + "    encode(bb)");
            lines.push(indent + "    return bb.toUint8Array");
            lines.push(indent + "  }");
            lines.push(indent + "}");
            lines.push("");
            lines.push(indent + "namespace " + definition.name + " {");
            lines.push(indent + "  def decode(bytes Uint8Array) " + definition.name + " {");
            lines.push(indent + "    return decode(Kiwi.ByteBuffer.new(bytes), null)");
            lines.push(indent + "  }");
            lines.push("");
            lines.push(indent + "  def decode(bytes Uint8Array, schema BinarySchema) " + definition.name + " {");
            lines.push(indent + "    return decode(Kiwi.ByteBuffer.new(bytes), schema)");
            lines.push(indent + "  }");
            lines.push("");
            lines.push(indent + "  def decode(bb Kiwi.ByteBuffer, schema BinarySchema) " + definition.name + " {");
            lines.push(indent + "    var self = new");
            for (let j = 0; j < definition.fields.length; j++) {
              if (definition.fields[j].isArray) {
                lines.push(indent + "    var count = 0");
                break;
              }
            }
            let nestedIndent = indent + "  ";
            if (definition.kind === "MESSAGE") {
              lines.push(indent + "    while true {");
              lines.push(indent + "      var type = bb.readVarUint");
              lines.push(indent + "      switch type {");
              lines.push(indent + "        case 0 {");
              lines.push(indent + "          break");
              lines.push(indent + "        }");
              lines.push("");
              nestedIndent += "      ";
            }
            for (let j = 0; j < definition.fields.length; j++) {
              let field = definition.fields[j];
              let code;
              switch (field.type) {
                case "bool": {
                  code = field.isDeprecated ? "bb.readByte" : "bb.readByte as bool";
                  break;
                }
                case "byte": {
                  code = "bb.readByte";
                  break;
                }
                case "int": {
                  code = "bb.readVarInt";
                  break;
                }
                case "uint": {
                  code = "bb.readVarUint";
                  break;
                }
                case "float": {
                  code = "bb.readVarFloat";
                  break;
                }
                case "string": {
                  code = "bb.readString";
                  break;
                }
                case "int64": {
                  code = "bb.readVarInt64";
                  break;
                }
                case "uint64": {
                  code = "bb.readVarUint64";
                  break;
                }
                default: {
                  let type = definitions[field.type];
                  if (!type) {
                    error("Invalid type " + quote(field.type) + " for field " + quote(field.name), field.line, field.column);
                  } else if (type.kind === "ENUM") {
                    code = type.name + ".decode(bb.readVarUint)";
                  } else {
                    code = type.name + ".decode(bb, schema)";
                  }
                }
              }
              if (definition.kind === "MESSAGE") {
                lines.push(nestedIndent + "case " + field.value + " {");
              }
              if (field.isArray) {
                if (field.isDeprecated) {
                  lines.push(nestedIndent + "  for i in 0..bb.readVarUint {");
                  lines.push(nestedIndent + "    " + code);
                  lines.push(nestedIndent + "  }");
                } else {
                  lines.push(nestedIndent + "  count = bb.readVarUint");
                  lines.push(nestedIndent + "  self." + field.name + " = []");
                  lines.push(nestedIndent + "  for array = self._" + field.name + "; count != 0; count-- {");
                  lines.push(nestedIndent + "    array.append(" + code + ")");
                  lines.push(nestedIndent + "  }");
                }
              } else {
                if (field.isDeprecated) {
                  lines.push(nestedIndent + "  " + code);
                } else {
                  lines.push(nestedIndent + "  self." + field.name + " = " + code);
                }
              }
              if (definition.kind === "MESSAGE") {
                lines.push(nestedIndent + "}");
                lines.push("");
              }
            }
            if (definition.kind === "MESSAGE") {
              lines.push(indent + "        default {");
              lines.push(indent + "          if schema == null { Kiwi.DecodeError.throwInvalidMessage }");
              lines.push(indent + "          else { schema.skip" + definition.name + "Field(bb, type) }");
              lines.push(indent + "        }");
              lines.push(indent + "      }");
              lines.push(indent + "    }");
            }
            lines.push(indent + "    return self");
            lines.push(indent + "  }");
            lines.push(indent + "}");
            lines.push("");
            break;
          }
          default: {
            error("Invalid definition kind " + quote(definition.kind), definition.line, definition.column);
            break;
          }
        }
      }
      if (schema.package !== null) {
        popTrailingNewline(lines);
        lines.push("}");
      }
      lines.push("");
      return lines.join("\n");
    }
    function compileSchemaSkewTypes(schema) {
      var indent = "";
      var lines = [];
      if (schema.package !== null) {
        lines.push("namespace " + schema.package + " {");
        indent += "  ";
      }
      for (var i = 0; i < schema.definitions.length; i++) {
        var definition = schema.definitions[i];
        if (definition.kind === "ENUM") {
          lines.push(indent + "type " + definition.name + " : string {");
          lines.push(indent + "  @alwaysinline");
          lines.push(indent + "  def toString string { return self as string }");
          lines.push(indent + "}");
          if (definition.fields.length > 0) {
            lines.push(indent + "namespace " + definition.name + " {");
            for (var j = 0; j < definition.fields.length; j++) {
              lines.push(indent + "  @alwaysinline");
              lines.push(indent + "  def " + definition.fields[j].name + " " + definition.name + " { return " + JSON.stringify(definition.fields[j].name) + " as " + definition.name + " }");
            }
            lines.push(indent + "}");
          }
          lines.push("");
        }
      }
      for (var i = 0; i < schema.definitions.length; i++) {
        var definition = schema.definitions[i];
        if (definition.kind === "STRUCT" || definition.kind === "MESSAGE") {
          lines.push(indent + "@import");
          lines.push(indent + "class " + definition.name + " {");
          for (var j = 0; j < definition.fields.length; j++) {
            var field = definition.fields[j];
            var type;
            if (field.isDeprecated) {
              continue;
            }
            switch (field.type) {
              case "byte":
              case "uint":
                type = "int";
                break;
              case "float":
                type = "double";
                break;
              case "int64":
              case "uint64":
                type = "BigInt";
                break;
              default:
                type = field.type;
                break;
            }
            if (field.type === "byte" && field.isArray)
              type = "Uint8Array";
            else if (field.isArray)
              type = "List<" + type + ">";
            lines.push(indent + "  var " + field.name + " " + type);
            lines.push(indent + "  @alwaysinline");
            lines.push(indent + "  def has_" + field.name + " bool { return self." + field.name + " != dynamic.void(0) }");
          }
          lines.push(indent + "}");
          lines.push("");
          lines.push(indent + "namespace " + definition.name + " {");
          lines.push(indent + "  @alwaysinline");
          lines.push(indent + "  def new " + definition.name + " { return {} as dynamic }");
          lines.push(indent + "}");
          lines.push("");
        } else if (definition.kind !== "ENUM") {
          error("Invalid definition kind " + quote(definition.kind), definition.line, definition.column);
        }
      }
      lines.push(indent + "@import");
      lines.push(indent + "class Schema {");
      for (var i = 0; i < schema.definitions.length; i++) {
        var definition = schema.definitions[i];
        if (definition.kind === "ENUM") {
          lines.push(indent + "  const " + definition.name + " dynamic");
        } else if (definition.kind === "STRUCT" || definition.kind === "MESSAGE") {
          lines.push(indent + "  def encode" + definition.name + "(message " + definition.name + ") Uint8Array");
          lines.push(indent + "  def decode" + definition.name + "(buffer Uint8Array) " + definition.name);
        }
      }
      lines.push(indent + "}");
      if (schema.package !== null) {
        lines.push("}");
      }
      lines.push("");
      return lines.join("\n");
    }
    function compileSchemaTypeScript(schema) {
      var indent = "";
      var lines = [];
      if (schema.package !== null) {
        lines.push("export namespace " + schema.package + " {");
        indent += "  ";
      }
      for (var i = 0; i < schema.definitions.length; i++) {
        var definition = schema.definitions[i];
        if (definition.kind === "ENUM") {
          lines.push(indent + "export type " + definition.name + " =");
          for (var j = 0; j < definition.fields.length; j++) {
            lines.push(indent + "  " + JSON.stringify(definition.fields[j].name) + (j + 1 < definition.fields.length ? " |" : ";"));
          }
          if (!definition.fields.length) {
            lines.push(indent + "  any;");
          }
          lines.push("");
        }
      }
      for (var i = 0; i < schema.definitions.length; i++) {
        var definition = schema.definitions[i];
        if (definition.kind === "STRUCT" || definition.kind === "MESSAGE") {
          lines.push(indent + "export interface " + definition.name + " {");
          for (var j = 0; j < definition.fields.length; j++) {
            var field = definition.fields[j];
            var type;
            if (field.isDeprecated) {
              continue;
            }
            switch (field.type) {
              case "bool":
                type = "boolean";
                break;
              case "byte":
              case "int":
              case "uint":
              case "float":
                type = "number";
                break;
              case "int64":
              case "uint64":
                type = "bigint";
                break;
              default:
                type = field.type;
                break;
            }
            if (field.type === "byte" && field.isArray)
              type = "Uint8Array";
            else if (field.isArray)
              type += "[]";
            lines.push(indent + "  " + field.name + (definition.kind === "MESSAGE" ? "?" : "") + ": " + type + ";");
          }
          lines.push(indent + "}");
          lines.push("");
        } else if (definition.kind !== "ENUM") {
          error("Invalid definition kind " + quote(definition.kind), definition.line, definition.column);
        }
      }
      lines.push(indent + "export interface Schema {");
      for (var i = 0; i < schema.definitions.length; i++) {
        var definition = schema.definitions[i];
        if (definition.kind === "ENUM") {
          lines.push(indent + "  " + definition.name + ": any;");
        } else if (definition.kind === "STRUCT" || definition.kind === "MESSAGE") {
          lines.push(indent + "  encode" + definition.name + "(message: " + definition.name + "): Uint8Array;");
          lines.push(indent + "  decode" + definition.name + "(buffer: Uint8Array): " + definition.name + ";");
        }
      }
      lines.push(indent + "}");
      if (schema.package !== null) {
        lines.push("}");
      }
      lines.push("");
      return lines.join("\n");
    }
    var types = ["bool", "byte", "int", "uint", "float", "string", "int64", "uint64"];
    var kinds = ["ENUM", "STRUCT", "MESSAGE"];
    function decodeBinarySchema(buffer2) {
      let bb = buffer2 instanceof ByteBuffer ? buffer2 : new ByteBuffer(buffer2);
      let definitionCount = bb.readVarUint();
      let definitions = [];
      for (let i = 0; i < definitionCount; i++) {
        let definitionName = bb.readString();
        let kind = bb.readByte();
        let fieldCount = bb.readVarUint();
        let fields = [];
        for (let j = 0; j < fieldCount; j++) {
          let fieldName = bb.readString();
          let type = bb.readVarInt();
          let isArray2 = !!(bb.readByte() & 1);
          let value = bb.readVarUint();
          fields.push({
            name: fieldName,
            line: 0,
            column: 0,
            type: kinds[kind] === "ENUM" ? null : type,
            isArray: isArray2,
            isDeprecated: false,
            value
          });
        }
        definitions.push({
          name: definitionName,
          line: 0,
          column: 0,
          kind: kinds[kind],
          fields
        });
      }
      for (let i = 0; i < definitionCount; i++) {
        let fields = definitions[i].fields;
        for (let j = 0; j < fields.length; j++) {
          let field = fields[j];
          let type = field.type;
          if (type !== null && type < 0) {
            if (~type >= types.length) {
              throw new Error("Invalid type " + type);
            }
            field.type = types[~type];
          } else {
            if (type !== null && type >= definitions.length) {
              throw new Error("Invalid type " + type);
            }
            field.type = type === null ? null : definitions[type].name;
          }
        }
      }
      return {
        package: null,
        definitions
      };
    }
    function encodeBinarySchema(schema) {
      let bb = new ByteBuffer();
      let definitions = schema.definitions;
      let definitionIndex = {};
      bb.writeVarUint(definitions.length);
      for (let i = 0; i < definitions.length; i++) {
        definitionIndex[definitions[i].name] = i;
      }
      for (let i = 0; i < definitions.length; i++) {
        let definition = definitions[i];
        bb.writeString(definition.name);
        bb.writeByte(kinds.indexOf(definition.kind));
        bb.writeVarUint(definition.fields.length);
        for (let j = 0; j < definition.fields.length; j++) {
          let field = definition.fields[j];
          let type = types.indexOf(field.type);
          bb.writeString(field.name);
          bb.writeVarInt(type === -1 ? definitionIndex[field.type] : ~type);
          bb.writeByte(field.isArray ? 1 : 0);
          bb.writeVarUint(field.value);
        }
      }
      return bb.toUint8Array();
    }
    var nativeTypes = [
      "bool",
      "byte",
      "float",
      "int",
      "int64",
      "string",
      "uint",
      "uint64"
    ];
    var reservedNames = [
      "ByteBuffer",
      "package"
    ];
    var regex = /((?:-|\b)\d+\b|[=;{}]|\[\]|\[deprecated\]|\b[A-Za-z_][A-Za-z0-9_]*\b|\/\/.*|\s+)/g;
    var identifier = /^[A-Za-z_][A-Za-z0-9_]*$/;
    var whitespace = /^\/\/.*|\s+$/;
    var equals = /^=$/;
    var endOfFile = /^$/;
    var semicolon = /^;$/;
    var integer = /^-?\d+$/;
    var leftBrace = /^\{$/;
    var rightBrace = /^\}$/;
    var arrayToken = /^\[\]$/;
    var enumKeyword = /^enum$/;
    var structKeyword = /^struct$/;
    var messageKeyword = /^message$/;
    var packageKeyword = /^package$/;
    var deprecatedToken = /^\[deprecated\]$/;
    function tokenize(text) {
      let parts = text.split(regex);
      let tokens = [];
      let column = 0;
      let line = 0;
      for (let i = 0; i < parts.length; i++) {
        let part = parts[i];
        if (i & 1) {
          if (!whitespace.test(part)) {
            tokens.push({
              text: part,
              line: line + 1,
              column: column + 1
            });
          }
        } else if (part !== "") {
          error("Syntax error " + quote(part), line + 1, column + 1);
        }
        let lines = part.split("\n");
        if (lines.length > 1)
          column = 0;
        line += lines.length - 1;
        column += lines[lines.length - 1].length;
      }
      tokens.push({
        text: "",
        line,
        column
      });
      return tokens;
    }
    function parse(tokens) {
      function current() {
        return tokens[index];
      }
      function eat(test2) {
        if (test2.test(current().text)) {
          index++;
          return true;
        }
        return false;
      }
      function expect(test2, expected) {
        if (!eat(test2)) {
          let token = current();
          error("Expected " + expected + " but found " + quote(token.text), token.line, token.column);
        }
      }
      function unexpectedToken() {
        let token = current();
        error("Unexpected token " + quote(token.text), token.line, token.column);
      }
      let definitions = [];
      let packageText = null;
      let index = 0;
      if (eat(packageKeyword)) {
        packageText = current().text;
        expect(identifier, "identifier");
        expect(semicolon, '";"');
      }
      while (index < tokens.length && !eat(endOfFile)) {
        let fields = [];
        let kind;
        if (eat(enumKeyword))
          kind = "ENUM";
        else if (eat(structKeyword))
          kind = "STRUCT";
        else if (eat(messageKeyword))
          kind = "MESSAGE";
        else
          unexpectedToken();
        let name = current();
        expect(identifier, "identifier");
        expect(leftBrace, '"{"');
        while (!eat(rightBrace)) {
          let type = null;
          let isArray2 = false;
          let isDeprecated = false;
          if (kind !== "ENUM") {
            type = current().text;
            expect(identifier, "identifier");
            isArray2 = eat(arrayToken);
          }
          let field = current();
          expect(identifier, "identifier");
          let value = null;
          if (kind !== "STRUCT") {
            expect(equals, '"="');
            value = current();
            expect(integer, "integer");
            if ((+value.text | 0) + "" !== value.text) {
              error("Invalid integer " + quote(value.text), value.line, value.column);
            }
          }
          let deprecated = current();
          if (eat(deprecatedToken)) {
            if (kind !== "MESSAGE") {
              error("Cannot deprecate this field", deprecated.line, deprecated.column);
            }
            isDeprecated = true;
          }
          expect(semicolon, '";"');
          fields.push({
            name: field.text,
            line: field.line,
            column: field.column,
            type,
            isArray: isArray2,
            isDeprecated,
            value: value !== null ? +value.text | 0 : fields.length + 1
          });
        }
        definitions.push({
          name: name.text,
          line: name.line,
          column: name.column,
          kind,
          fields
        });
      }
      return {
        package: packageText,
        definitions
      };
    }
    function verify(root) {
      let definedTypes = nativeTypes.slice();
      let definitions = {};
      for (let i = 0; i < root.definitions.length; i++) {
        let definition = root.definitions[i];
        if (definedTypes.indexOf(definition.name) !== -1) {
          error("The type " + quote(definition.name) + " is defined twice", definition.line, definition.column);
        }
        if (reservedNames.indexOf(definition.name) !== -1) {
          error("The type name " + quote(definition.name) + " is reserved", definition.line, definition.column);
        }
        definedTypes.push(definition.name);
        definitions[definition.name] = definition;
      }
      for (let i = 0; i < root.definitions.length; i++) {
        let definition = root.definitions[i];
        let fields = definition.fields;
        if (definition.kind === "ENUM" || fields.length === 0) {
          continue;
        }
        for (let j = 0; j < fields.length; j++) {
          let field = fields[j];
          if (definedTypes.indexOf(field.type) === -1) {
            error("The type " + quote(field.type) + " is not defined for field " + quote(field.name), field.line, field.column);
          }
        }
        let values = [];
        for (let j = 0; j < fields.length; j++) {
          let field = fields[j];
          if (values.indexOf(field.value) !== -1) {
            error("The id for field " + quote(field.name) + " is used twice", field.line, field.column);
          }
          if (field.value <= 0) {
            error("The id for field " + quote(field.name) + " must be positive", field.line, field.column);
          }
          if (field.value > fields.length) {
            error("The id for field " + quote(field.name) + " cannot be larger than " + fields.length, field.line, field.column);
          }
          values.push(field.value);
        }
      }
      let state = {};
      let check = (name) => {
        let definition = definitions[name];
        if (definition && definition.kind === "STRUCT") {
          if (state[name] === 1) {
            error("Recursive nesting of " + quote(name) + " is not allowed", definition.line, definition.column);
          }
          if (state[name] !== 2 && definition) {
            state[name] = 1;
            let fields = definition.fields;
            for (let i = 0; i < fields.length; i++) {
              let field = fields[i];
              if (!field.isArray) {
                check(field.type);
              }
            }
            state[name] = 2;
          }
        }
        return true;
      };
      for (let i = 0; i < root.definitions.length; i++) {
        check(root.definitions[i].name);
      }
    }
    function parseSchema2(text) {
      let schema = parse(tokenize(text));
      verify(schema);
      return schema;
    }
    function prettyPrintSchema(schema) {
      let definitions = schema.definitions;
      let text = "";
      if (schema.package !== null) {
        text += "package " + schema.package + ";\n";
      }
      for (let i = 0; i < definitions.length; i++) {
        let definition = definitions[i];
        if (i > 0 || schema.package !== null)
          text += "\n";
        text += definition.kind.toLowerCase() + " " + definition.name + " {\n";
        for (let j = 0; j < definition.fields.length; j++) {
          let field = definition.fields[j];
          text += "  ";
          if (definition.kind !== "ENUM") {
            text += field.type;
            if (field.isArray) {
              text += "[]";
            }
            text += " ";
          }
          text += field.name;
          if (definition.kind !== "STRUCT") {
            text += " = " + field.value;
          }
          if (field.isDeprecated) {
            text += " [deprecated]";
          }
          text += ";\n";
        }
        text += "}\n";
      }
      return text;
    }
  }
});

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/canvas-session.ts
var currentCanvasPath;
function getCurrentCanvasPath() {
  return currentCanvasPath;
}
function setCurrentCanvasPath(filePath) {
  currentCanvasPath = filePath;
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/dispatch-lanes.ts
function touchesCanvasSession(parsed, canvasToolNames) {
  const batch = Array.isArray(parsed) ? parsed : [parsed];
  return batch.some((message) => {
    if (!message || typeof message !== "object") {
      return false;
    }
    if (message.method === "resources/read") {
      return true;
    }
    if (message.method !== "tools/call") {
      return false;
    }
    const name = message.params?.name;
    return typeof name === "string" && canvasToolNames.has(name);
  });
}
function createDispatchLanes(dispatch2, logError2, canvasToolNames) {
  let canvasQueue = Promise.resolve();
  const inFlight = /* @__PURE__ */ new Set();
  function runSafely(parsed) {
    return dispatch2(parsed).catch((error) => {
      logError2("dispatch failed:", error instanceof Error ? error.message : String(error));
    });
  }
  function track(task) {
    inFlight.add(task);
    void task.finally(() => {
      inFlight.delete(task);
    });
  }
  return {
    enqueue(parsed) {
      if (touchesCanvasSession(parsed, canvasToolNames)) {
        canvasQueue = canvasQueue.then(() => runSafely(parsed));
        track(canvasQueue);
        return;
      }
      track(runSafely(parsed));
    },
    async drain() {
      while (inFlight.size > 0) {
        await Promise.allSettled([...inFlight]);
      }
    }
  };
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/media/artifact-localizer.ts
var import_node_crypto = require("node:crypto");
var import_node_fs = require("node:fs");
var import_promises2 = require("node:fs/promises");
var os = __toESM(require("node:os"));
var path = __toESM(require("node:path"));
var import_node_stream = require("node:stream");
var import_promises3 = require("node:stream/promises");

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/media/url-safety.ts
var import_promises = require("node:dns/promises");
var import_node_net = require("node:net");
async function assertSafeExternalUrl(rawUrl) {
  let parsed;
  try {
    parsed = new URL(rawUrl);
  } catch {
    throw new Error(`invalid url: ${rawUrl}`);
  }
  if (parsed.protocol !== "https:") {
    throw new Error(`unsupported scheme "${parsed.protocol}"; only https is allowed`);
  }
  const hostname = parsed.hostname;
  if (!hostname) {
    throw new Error("url has no hostname");
  }
  if (looksLikeAmbiguousNumericHost(hostname)) {
    throw new Error(
      `host "${hostname}" is a non-canonical numeric address (octal/hex/integer form is ambiguous)`
    );
  }
  const addresses = isCanonicalIpLiteral(hostname) ? [{ address: stripIpv6Brackets(hostname) }] : await (0, import_promises.lookup)(hostname, { all: true });
  for (const addr of addresses) {
    if (isPrivateAddress(addr.address)) {
      throw new Error(`host ${hostname} resolves to private/reserved address ${addr.address}`);
    }
  }
}
function isCanonicalIpLiteral(host) {
  return (0, import_node_net.isIP)(stripIpv6Brackets(host)) > 0;
}
function looksLikeAmbiguousNumericHost(host) {
  if (host.includes(":")) return false;
  if ((0, import_node_net.isIP)(host) > 0) return false;
  return /^(?:0x[0-9a-f]+|[0-9]+)(?:\.(?:0x[0-9a-f]+|[0-9]+))*\.?$/i.test(host);
}
function stripIpv6Brackets(host) {
  return host.startsWith("[") && host.endsWith("]") ? host.slice(1, -1) : host;
}
function isPrivateAddress(addr) {
  if (addr.includes(":")) {
    return isPrivateIpv6(addr.toLowerCase());
  }
  return isPrivateIpv4(addr);
}
function isPrivateIpv4(addr) {
  const parts = addr.split(".").map((n) => Number.parseInt(n, 10));
  if (parts.length !== 4 || parts.some((n) => !Number.isInteger(n) || n < 0 || n > 255)) {
    return true;
  }
  const [a, b] = parts;
  if (a === 10) return true;
  if (a === 127) return true;
  if (a === 0) return true;
  if (a === 169 && b === 254) return true;
  if (a === 172 && b >= 16 && b <= 31) return true;
  if (a === 192 && b === 168) return true;
  if (a === 100 && b >= 64 && b <= 127) return true;
  if (a >= 224) return true;
  return false;
}
function isPrivateIpv6(addr) {
  if (addr === "::" || addr === "::1") return true;
  if (addr.startsWith("fc") || addr.startsWith("fd")) return true;
  if (addr.startsWith("fe8") || addr.startsWith("fe9") || addr.startsWith("fea") || addr.startsWith("feb")) return true;
  if (addr.startsWith("ff")) return true;
  const mapped = addr.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/);
  if (mapped) return isPrivateIpv4(mapped[1]);
  return false;
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/media/artifact-localizer.ts
var DOWNLOAD_TIMEOUT_MS = 12e4;
var MAX_REDIRECT_HOPS = 3;
var MAX_ARTIFACT_BYTES = 200 * 1024 * 1024;
var STALE_FILE_MAX_AGE_MS = 3 * 24 * 60 * 60 * 1e3;
var MIME_TO_EXT = {
  "image/png": ".png",
  "image/jpeg": ".jpg",
  "image/jpg": ".jpg",
  "image/gif": ".gif",
  "image/webp": ".webp",
  "image/svg+xml": ".svg",
  "image/bmp": ".bmp",
  "video/mp4": ".mp4",
  "video/webm": ".webm",
  "video/quicktime": ".mov",
  "video/x-msvideo": ".avi",
  "video/x-matroska": ".mkv"
};
function resolveExtension(mimeType) {
  const normalized = (mimeType ?? "").trim().toLowerCase().split(";")[0];
  const mapped = MIME_TO_EXT[normalized];
  if (mapped) {
    return mapped;
  }
  return normalized.startsWith("video/") ? ".mp4" : ".png";
}
function resolveOutputDir() {
  const dataDir = process.env.MIORA_DATA_DIR?.trim();
  return path.join(dataDir || os.tmpdir(), "miora-media");
}
function buildFileName(tool, artifact, index) {
  const digest = (0, import_node_crypto.createHash)("sha256").update(artifact.signedUrl).digest("hex").slice(0, 12);
  return `${tool}-${Date.now()}-${index}-${digest}${resolveExtension(artifact.mimeType)}`;
}
async function streamDownloadToFile(url2, target, maxBytes) {
  const response = await fetchFollowingSafeRedirects(url2);
  if (!response.ok) {
    throw new Error(`HTTP ${response.status}`);
  }
  const declared = Number(response.headers.get("content-length") ?? "");
  if (Number.isFinite(declared) && declared > maxBytes) {
    throw new Error(`content-length ${declared} exceeds max ${maxBytes}`);
  }
  if (!response.body) {
    throw new Error("response has no body");
  }
  let received = 0;
  await (0, import_promises3.pipeline)(
    import_node_stream.Readable.fromWeb(response.body),
    async function* countBytes(source) {
      for await (const chunk of source) {
        received += chunk.byteLength;
        if (received > maxBytes) {
          throw new Error(`artifact exceeds ${maxBytes}B (aborted at ${received}B)`);
        }
        yield chunk;
      }
    },
    (0, import_node_fs.createWriteStream)(target)
  );
}
async function fetchFollowingSafeRedirects(initialUrl) {
  let currentUrl = initialUrl;
  for (let hop = 0; hop <= MAX_REDIRECT_HOPS; hop += 1) {
    await assertSafeExternalUrl(currentUrl);
    const response = await fetch(currentUrl, {
      signal: AbortSignal.timeout(DOWNLOAD_TIMEOUT_MS),
      redirect: "manual"
    });
    if (response.status < 300 || response.status >= 400 || response.status === 304) {
      return response;
    }
    const location = response.headers.get("location");
    await response.body?.cancel().catch(() => {
    });
    if (!location) {
      throw new Error(`redirect ${response.status} without Location header`);
    }
    currentUrl = new URL(location, currentUrl).toString();
  }
  throw new Error(`too many redirects (>${MAX_REDIRECT_HOPS}) starting from ${initialUrl}`);
}
var sweepDone = false;
async function sweepStaleArtifacts(dir, onWarn) {
  if (sweepDone) return;
  sweepDone = true;
  try {
    const entries = await (0, import_promises2.readdir)(dir);
    const cutoff = Date.now() - STALE_FILE_MAX_AGE_MS;
    await Promise.all(entries.map(async (name) => {
      const full = path.join(dir, name);
      try {
        const st = await (0, import_promises2.stat)(full);
        if (st.isFile() && st.mtimeMs < cutoff) {
          await (0, import_promises2.unlink)(full);
        }
      } catch {
      }
    }));
  } catch (err) {
    onWarn?.(`miora-media sweep failed: ${String(err)}`);
  }
}
async function localizeArtifacts(tool, artifacts, onWarn) {
  if (artifacts.length === 0) {
    return [];
  }
  const outputDir = resolveOutputDir();
  try {
    await (0, import_promises2.mkdir)(outputDir, { recursive: true });
  } catch (error) {
    onWarn?.(`\u521B\u5EFA\u4EA7\u7269\u76EE\u5F55\u5931\u8D25\uFF0C\u8DF3\u8FC7\u672C\u5730\u5316\uFF1A${outputDir} ${String(error)}`);
    return artifacts.map((artifact) => ({ ...artifact }));
  }
  await sweepStaleArtifacts(outputDir, onWarn);
  const results = [];
  for (const [index, artifact] of artifacts.entries()) {
    if (!artifact.signedUrl) {
      results.push({ ...artifact });
      continue;
    }
    const target = path.join(outputDir, buildFileName(tool, artifact, index));
    const startedAt = Date.now();
    onWarn?.(`localize_start tool=${tool} index=${index} mime=${artifact.mimeType ?? "unknown"}`);
    try {
      await assertSafeExternalUrl(artifact.signedUrl);
      await streamDownloadToFile(artifact.signedUrl, target, MAX_ARTIFACT_BYTES);
      results.push({ ...artifact, localPath: target });
      onWarn?.(`localize_ok tool=${tool} index=${index} elapsedMs=${Date.now() - startedAt}`);
    } catch (error) {
      (0, import_promises2.unlink)(target).catch(() => {
      });
      onWarn?.(
        `localize_fail tool=${tool} index=${index} elapsedMs=${Date.now() - startedAt} target=${target} err=${String(error)}`
      );
      results.push({ ...artifact });
    }
  }
  return results;
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/media/media-common.ts
var SCENARIO_GATE_DESCRIPTION = 'ONLY use this tool when the active skill/scenario is "\u591A\u6A21\u6001\u5185\u5BB9\u751F\u6210" (Multimodal Generation). Do NOT call for general conversation or unrelated skills \u2014 it will not work outside that scenario.';
var TEMP_KEY_PREFLIGHT_DESCRIPTION = "PREFLIGHT: You MUST first call `connect_cloud_service` in this turn (or reuse the `clientTempToken` from an earlier call in the same session if still valid) and pass its `clientTempToken` as the `tempKey` argument here. Do NOT invent or hard-code a tempKey. If you receive `AUTH_INVALID_TEMP_KEY` / `AUTH_MISSING_TEMP_KEY`, re-call `connect_cloud_service` to obtain a fresh `clientTempToken` and retry.";
var DELIVERY_DESCRIPTION = "On success, pass `resultFiles[].localPath` (absolute local path) to `miora_write_canvas` as `items[].source_path`. That tool copies the file into the session directory and returns `copied_assets` \u2014 hand THOSE paths to `present_files` to get artifact cards for the images/videos. Do NOT hand `localPath` straight to `present_files`: it lives in a hidden plugin directory and never becomes an artifact. Do NOT hand `signedUrl` to `present_files` either \u2014 a remote URL opens an in-app browser panel instead of an artifact card, and it expires. Only fall back to `signedUrl` when `localPath` is absent (local download failed).";
var TEMP_KEY_INPUT_SCHEMA = {
  type: "string",
  description: "The `clientTempToken` obtained from `connect_cloud_service`. Required \u2014 miora rejects any other credential (long-lived JWT, personal access tokens, etc.). Do NOT expose this value to the user."
};
var NON_RETRYABLE_CODES = /* @__PURE__ */ new Set([
  "INVALID_INPUT",
  "MISSING_INPUT",
  "MODEL_NOT_ALLOWED",
  "COPYRIGHT_VIOLATION",
  "CONTENT_SAFETY",
  "FEATURE_DISABLED",
  "TOOL_DISABLED",
  "AUTH_SCOPE_DENIED",
  // 429 一律拒 auto-retry：当前不区分节流发生在 submit 前还是 submit 后，
  // 后者盲目重试会双扣费。等拿到生产 429 分布数据后再考虑拆 pre/post submit 放开前者。
  "RATE_LIMITED"
]);
function buildNonRetryInstruction(code) {
  switch (code) {
    case "COPYRIGHT_VIOLATION":
      return "This is a copyright block. Do NOT retry with the same or a similar prompt. Tell the user directly that the content cannot be generated for copyright reasons and ask for a different creative direction.";
    case "CONTENT_SAFETY":
      return "This is a content-safety rejection. Do NOT retry \u2014 retries will hit the same moderation. Tell the user the content did not pass safety review and ask them to adjust the creative direction.";
    case "MODEL_NOT_ALLOWED":
      return "The requested model is not allowed for this entry. Pick a different model from the description before retrying; retrying with the same model will fail identically.";
    case "MISSING_INPUT":
    case "INVALID_INPUT":
      return "This is a parameter error. Fix the offending fields before retrying; retrying with the same arguments will fail identically.";
    case "FEATURE_DISABLED":
      return "Miora media generation is currently under maintenance / disabled by the server. Do NOT retry \u2014 tell the user the capability is temporarily unavailable.";
    case "TOOL_DISABLED":
      return "This specific tool (image or video generation) is disabled by the server. Do NOT retry \u2014 tell the user this capability is temporarily unavailable.";
    case "AUTH_SCOPE_DENIED":
      return "The current account is not authorized to use this capability. Do NOT retry \u2014 tell the user their account lacks the required permission and to contact an administrator.";
    case "RATE_LIMITED":
      return "Miora rate-limited this request (HTTP 429). Do NOT auto-retry: the generation task may already have been submitted upstream, and a blind resubmit would double-charge the user. Tell the user the request was throttled and ask them to explicitly retry later.";
    default:
      return "Do not retry with the same parameters.";
  }
}
async function buildMediaToolSuccess(args) {
  const rawFiles = args.entry.resultFiles ?? [];
  const resultFiles = args.entry.status === "completed" ? await localizeArtifacts(args.tool, rawFiles, args.onWarn) : rawFiles;
  const localized = resultFiles.filter(
    (file) => "localPath" in file && !!file.localPath
  );
  const payload = {
    type: `${args.tool}_result`,
    upstreamTaskId: args.upstreamTaskId,
    model: args.model,
    provider: args.provider,
    status: args.entry.status,
    resultFiles,
    errorMessage: args.entry.errorMessage,
    // localPath 用来**喂画布**，不要直接喂 present_files，更不要用 signedUrl。三个原因：
    //
    // 1. `present_files` 对 http(s) URL 的处理是「在右侧开内置浏览器预览」
    //    （见 workbuddy-server `present-files-tool.ts` 的 isHttp 分支）。把 COS
    //    signedUrl 传进去，用户会莫名多出一个浏览器面板 —— 而他要的是产物卡。
    // 2. signedUrl 带过期时间（见 `artifact-localizer.ts` 头注释），写进画布会变死链。
    // 3. localPath 落在 `MIORA_DATA_DIR`（`~/.codebuddy/plugins/data/<id>`），路径含
    //    `.codebuddy`，命中 WorkBuddy 产物隐藏规则 —— 直接交付它，产物区同样是空的。
    //    `miora_write_canvas` 会把资源拷进会话目录的 `<画布名>_assets/` 并回传
    //    `copied_assets`，那份副本才是能变成产物卡的路径。
    hint: localized.length > 0 ? "Pass `resultFiles[].localPath` (absolute local path) to `miora_write_canvas` as `items[].source_path`; then hand the `copied_assets` it returns to `present_files` so the images/videos appear as artifact cards. Do NOT pass `localPath` straight to `present_files` (hidden plugin directory \u2014 it never becomes an artifact), and do NOT pass `signedUrl` to it either (it expires and opens an unwanted in-app browser panel). Do NOT mention tokens or credentials." : "Local download failed, so only `signedUrl` is available: hand it to `present_files` so the user can still see the result. It cannot be placed on a Miora canvas (canvas requires a local path). Do NOT mention tokens or credentials."
  };
  const isError = args.entry.status !== "completed";
  return {
    content: [{ type: "text", text: JSON.stringify(payload) }],
    isError: isError || void 0
  };
}
function buildMediaToolError(code, message, extras) {
  const payload = {
    type: "miora_media_error",
    code,
    message
  };
  if (NON_RETRYABLE_CODES.has(code)) {
    payload.instruction = buildNonRetryInstruction(code);
  }
  if (extras?.upstreamTaskId) {
    payload.upstreamTaskId = extras.upstreamTaskId;
    payload.resumeHint = "The task was already submitted upstream (upstreamTaskId above) and may still be running \u2014 do NOT resubmit blindly, that would double-charge the user. If the code is `AUTH_INVALID_TEMP_KEY`, re-call `connect_cloud_service` to obtain a fresh `clientTempToken` first. Then call `miora_query_task` with this upstreamTaskId to check the real backend status (it may already be completed) before deciding whether to regenerate.";
  }
  return {
    content: [{ type: "text", text: JSON.stringify(payload) }],
    isError: true
  };
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/media/image-input.ts
var import_node_fs2 = require("node:fs");
var path2 = __toESM(require("node:path"));

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/bind.js
function bind(fn, thisArg) {
  return function wrap() {
    return fn.apply(thisArg, arguments);
  };
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/utils.js
var { toString } = Object.prototype;
var { getPrototypeOf } = Object;
var { iterator, toStringTag } = Symbol;
var kindOf = /* @__PURE__ */ ((cache) => (thing) => {
  const str = toString.call(thing);
  return cache[str] || (cache[str] = str.slice(8, -1).toLowerCase());
})(/* @__PURE__ */ Object.create(null));
var kindOfTest = (type) => {
  type = type.toLowerCase();
  return (thing) => kindOf(thing) === type;
};
var typeOfTest = (type) => (thing) => typeof thing === type;
var { isArray } = Array;
var isUndefined = typeOfTest("undefined");
function isBuffer(val) {
  return val !== null && !isUndefined(val) && val.constructor !== null && !isUndefined(val.constructor) && isFunction(val.constructor.isBuffer) && val.constructor.isBuffer(val);
}
var isArrayBuffer = kindOfTest("ArrayBuffer");
function isArrayBufferView(val) {
  let result;
  if (typeof ArrayBuffer !== "undefined" && ArrayBuffer.isView) {
    result = ArrayBuffer.isView(val);
  } else {
    result = val && val.buffer && isArrayBuffer(val.buffer);
  }
  return result;
}
var isString = typeOfTest("string");
var isFunction = typeOfTest("function");
var isNumber = typeOfTest("number");
var isObject = (thing) => thing !== null && typeof thing === "object";
var isBoolean = (thing) => thing === true || thing === false;
var isPlainObject = (val) => {
  if (kindOf(val) !== "object") {
    return false;
  }
  const prototype2 = getPrototypeOf(val);
  return (prototype2 === null || prototype2 === Object.prototype || Object.getPrototypeOf(prototype2) === null) && !(toStringTag in val) && !(iterator in val);
};
var isEmptyObject = (val) => {
  if (!isObject(val) || isBuffer(val)) {
    return false;
  }
  try {
    return Object.keys(val).length === 0 && Object.getPrototypeOf(val) === Object.prototype;
  } catch (e) {
    return false;
  }
};
var isDate = kindOfTest("Date");
var isFile = kindOfTest("File");
var isReactNativeBlob = (value) => {
  return !!(value && typeof value.uri !== "undefined");
};
var isReactNative = (formData) => formData && typeof formData.getParts !== "undefined";
var isBlob = kindOfTest("Blob");
var isFileList = kindOfTest("FileList");
var isStream = (val) => isObject(val) && isFunction(val.pipe);
function getGlobal() {
  if (typeof globalThis !== "undefined") return globalThis;
  if (typeof self !== "undefined") return self;
  if (typeof window !== "undefined") return window;
  if (typeof global !== "undefined") return global;
  return {};
}
var G = getGlobal();
var FormDataCtor = typeof G.FormData !== "undefined" ? G.FormData : void 0;
var isFormData = (thing) => {
  if (!thing) return false;
  if (FormDataCtor && thing instanceof FormDataCtor) return true;
  const proto = getPrototypeOf(thing);
  if (!proto || proto === Object.prototype) return false;
  if (!isFunction(thing.append)) return false;
  const kind = kindOf(thing);
  return kind === "formdata" || // detect form-data instance
  kind === "object" && isFunction(thing.toString) && thing.toString() === "[object FormData]";
};
var isURLSearchParams = kindOfTest("URLSearchParams");
var [isReadableStream, isRequest, isResponse, isHeaders] = [
  "ReadableStream",
  "Request",
  "Response",
  "Headers"
].map(kindOfTest);
var trim = (str) => {
  return str.trim ? str.trim() : str.replace(/^[\s\uFEFF\xA0]+|[\s\uFEFF\xA0]+$/g, "");
};
function forEach(obj, fn, { allOwnKeys = false } = {}) {
  if (obj === null || typeof obj === "undefined") {
    return;
  }
  let i;
  let l;
  if (typeof obj !== "object") {
    obj = [obj];
  }
  if (isArray(obj)) {
    for (i = 0, l = obj.length; i < l; i++) {
      fn.call(null, obj[i], i, obj);
    }
  } else {
    if (isBuffer(obj)) {
      return;
    }
    const keys = allOwnKeys ? Object.getOwnPropertyNames(obj) : Object.keys(obj);
    const len = keys.length;
    let key;
    for (i = 0; i < len; i++) {
      key = keys[i];
      fn.call(null, obj[key], key, obj);
    }
  }
}
function findKey(obj, key) {
  if (isBuffer(obj)) {
    return null;
  }
  key = key.toLowerCase();
  const keys = Object.keys(obj);
  let i = keys.length;
  let _key;
  while (i-- > 0) {
    _key = keys[i];
    if (key === _key.toLowerCase()) {
      return _key;
    }
  }
  return null;
}
var _global = (() => {
  if (typeof globalThis !== "undefined") return globalThis;
  return typeof self !== "undefined" ? self : typeof window !== "undefined" ? window : global;
})();
var isContextDefined = (context) => !isUndefined(context) && context !== _global;
function merge(...objs) {
  const { caseless, skipUndefined } = isContextDefined(this) && this || {};
  const result = {};
  const assignValue = (val, key) => {
    if (key === "__proto__" || key === "constructor" || key === "prototype") {
      return;
    }
    const targetKey = caseless && findKey(result, key) || key;
    const existing = hasOwnProperty(result, targetKey) ? result[targetKey] : void 0;
    if (isPlainObject(existing) && isPlainObject(val)) {
      result[targetKey] = merge(existing, val);
    } else if (isPlainObject(val)) {
      result[targetKey] = merge({}, val);
    } else if (isArray(val)) {
      result[targetKey] = val.slice();
    } else if (!skipUndefined || !isUndefined(val)) {
      result[targetKey] = val;
    }
  };
  for (let i = 0, l = objs.length; i < l; i++) {
    objs[i] && forEach(objs[i], assignValue);
  }
  return result;
}
var extend = (a, b, thisArg, { allOwnKeys } = {}) => {
  forEach(
    b,
    (val, key) => {
      if (thisArg && isFunction(val)) {
        Object.defineProperty(a, key, {
          // Null-proto descriptor so a polluted Object.prototype.get cannot
          // hijack defineProperty's accessor-vs-data resolution.
          __proto__: null,
          value: bind(val, thisArg),
          writable: true,
          enumerable: true,
          configurable: true
        });
      } else {
        Object.defineProperty(a, key, {
          __proto__: null,
          value: val,
          writable: true,
          enumerable: true,
          configurable: true
        });
      }
    },
    { allOwnKeys }
  );
  return a;
};
var stripBOM = (content) => {
  if (content.charCodeAt(0) === 65279) {
    content = content.slice(1);
  }
  return content;
};
var inherits = (constructor, superConstructor, props, descriptors) => {
  constructor.prototype = Object.create(superConstructor.prototype, descriptors);
  Object.defineProperty(constructor.prototype, "constructor", {
    __proto__: null,
    value: constructor,
    writable: true,
    enumerable: false,
    configurable: true
  });
  Object.defineProperty(constructor, "super", {
    __proto__: null,
    value: superConstructor.prototype
  });
  props && Object.assign(constructor.prototype, props);
};
var toFlatObject = (sourceObj, destObj, filter2, propFilter) => {
  let props;
  let i;
  let prop;
  const merged = {};
  destObj = destObj || {};
  if (sourceObj == null) return destObj;
  do {
    props = Object.getOwnPropertyNames(sourceObj);
    i = props.length;
    while (i-- > 0) {
      prop = props[i];
      if ((!propFilter || propFilter(prop, sourceObj, destObj)) && !merged[prop]) {
        destObj[prop] = sourceObj[prop];
        merged[prop] = true;
      }
    }
    sourceObj = filter2 !== false && getPrototypeOf(sourceObj);
  } while (sourceObj && (!filter2 || filter2(sourceObj, destObj)) && sourceObj !== Object.prototype);
  return destObj;
};
var endsWith = (str, searchString, position) => {
  str = String(str);
  if (position === void 0 || position > str.length) {
    position = str.length;
  }
  position -= searchString.length;
  const lastIndex = str.indexOf(searchString, position);
  return lastIndex !== -1 && lastIndex === position;
};
var toArray = (thing) => {
  if (!thing) return null;
  if (isArray(thing)) return thing;
  let i = thing.length;
  if (!isNumber(i)) return null;
  const arr = new Array(i);
  while (i-- > 0) {
    arr[i] = thing[i];
  }
  return arr;
};
var isTypedArray = /* @__PURE__ */ ((TypedArray) => {
  return (thing) => {
    return TypedArray && thing instanceof TypedArray;
  };
})(typeof Uint8Array !== "undefined" && getPrototypeOf(Uint8Array));
var forEachEntry = (obj, fn) => {
  const generator = obj && obj[iterator];
  const _iterator = generator.call(obj);
  let result;
  while ((result = _iterator.next()) && !result.done) {
    const pair = result.value;
    fn.call(obj, pair[0], pair[1]);
  }
};
var matchAll = (regExp, str) => {
  let matches;
  const arr = [];
  while ((matches = regExp.exec(str)) !== null) {
    arr.push(matches);
  }
  return arr;
};
var isHTMLForm = kindOfTest("HTMLFormElement");
var toCamelCase = (str) => {
  return str.toLowerCase().replace(/[-_\s]([a-z\d])(\w*)/g, function replacer(m, p1, p2) {
    return p1.toUpperCase() + p2;
  });
};
var hasOwnProperty = (({ hasOwnProperty: hasOwnProperty2 }) => (obj, prop) => hasOwnProperty2.call(obj, prop))(Object.prototype);
var isRegExp = kindOfTest("RegExp");
var reduceDescriptors = (obj, reducer) => {
  const descriptors = Object.getOwnPropertyDescriptors(obj);
  const reducedDescriptors = {};
  forEach(descriptors, (descriptor, name) => {
    let ret;
    if ((ret = reducer(descriptor, name, obj)) !== false) {
      reducedDescriptors[name] = ret || descriptor;
    }
  });
  Object.defineProperties(obj, reducedDescriptors);
};
var freezeMethods = (obj) => {
  reduceDescriptors(obj, (descriptor, name) => {
    if (isFunction(obj) && ["arguments", "caller", "callee"].includes(name)) {
      return false;
    }
    const value = obj[name];
    if (!isFunction(value)) return;
    descriptor.enumerable = false;
    if ("writable" in descriptor) {
      descriptor.writable = false;
      return;
    }
    if (!descriptor.set) {
      descriptor.set = () => {
        throw Error("Can not rewrite read-only method '" + name + "'");
      };
    }
  });
};
var toObjectSet = (arrayOrString, delimiter) => {
  const obj = {};
  const define = (arr) => {
    arr.forEach((value) => {
      obj[value] = true;
    });
  };
  isArray(arrayOrString) ? define(arrayOrString) : define(String(arrayOrString).split(delimiter));
  return obj;
};
var noop = () => {
};
var toFiniteNumber = (value, defaultValue) => {
  return value != null && Number.isFinite(value = +value) ? value : defaultValue;
};
function isSpecCompliantForm(thing) {
  return !!(thing && isFunction(thing.append) && thing[toStringTag] === "FormData" && thing[iterator]);
}
var toJSONObject = (obj) => {
  const visited = /* @__PURE__ */ new WeakSet();
  const visit = (source) => {
    if (isObject(source)) {
      if (visited.has(source)) {
        return;
      }
      if (isBuffer(source)) {
        return source;
      }
      if (!("toJSON" in source)) {
        visited.add(source);
        const target = isArray(source) ? [] : {};
        forEach(source, (value, key) => {
          const reducedValue = visit(value);
          !isUndefined(reducedValue) && (target[key] = reducedValue);
        });
        visited.delete(source);
        return target;
      }
    }
    return source;
  };
  return visit(obj);
};
var isAsyncFn = kindOfTest("AsyncFunction");
var isThenable = (thing) => thing && (isObject(thing) || isFunction(thing)) && isFunction(thing.then) && isFunction(thing.catch);
var _setImmediate = ((setImmediateSupported, postMessageSupported) => {
  if (setImmediateSupported) {
    return setImmediate;
  }
  return postMessageSupported ? ((token, callbacks) => {
    _global.addEventListener(
      "message",
      ({ source, data }) => {
        if (source === _global && data === token) {
          callbacks.length && callbacks.shift()();
        }
      },
      false
    );
    return (cb) => {
      callbacks.push(cb);
      _global.postMessage(token, "*");
    };
  })(`axios@${Math.random()}`, []) : (cb) => setTimeout(cb);
})(typeof setImmediate === "function", isFunction(_global.postMessage));
var asap = typeof queueMicrotask !== "undefined" ? queueMicrotask.bind(_global) : typeof process !== "undefined" && process.nextTick || _setImmediate;
var isIterable = (thing) => thing != null && isFunction(thing[iterator]);
var utils_default = {
  isArray,
  isArrayBuffer,
  isBuffer,
  isFormData,
  isArrayBufferView,
  isString,
  isNumber,
  isBoolean,
  isObject,
  isPlainObject,
  isEmptyObject,
  isReadableStream,
  isRequest,
  isResponse,
  isHeaders,
  isUndefined,
  isDate,
  isFile,
  isReactNativeBlob,
  isReactNative,
  isBlob,
  isRegExp,
  isFunction,
  isStream,
  isURLSearchParams,
  isTypedArray,
  isFileList,
  forEach,
  merge,
  extend,
  trim,
  stripBOM,
  inherits,
  toFlatObject,
  kindOf,
  kindOfTest,
  endsWith,
  toArray,
  forEachEntry,
  matchAll,
  isHTMLForm,
  hasOwnProperty,
  hasOwnProp: hasOwnProperty,
  // an alias to avoid ESLint no-prototype-builtins detection
  reduceDescriptors,
  freezeMethods,
  toObjectSet,
  toCamelCase,
  noop,
  toFiniteNumber,
  findKey,
  global: _global,
  isContextDefined,
  isSpecCompliantForm,
  toJSONObject,
  isAsyncFn,
  isThenable,
  setImmediate: _setImmediate,
  asap,
  isIterable
};

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/parseHeaders.js
var ignoreDuplicateOf = utils_default.toObjectSet([
  "age",
  "authorization",
  "content-length",
  "content-type",
  "etag",
  "expires",
  "from",
  "host",
  "if-modified-since",
  "if-unmodified-since",
  "last-modified",
  "location",
  "max-forwards",
  "proxy-authorization",
  "referer",
  "retry-after",
  "user-agent"
]);
var parseHeaders_default = (rawHeaders) => {
  const parsed = {};
  let key;
  let val;
  let i;
  rawHeaders && rawHeaders.split("\n").forEach(function parser(line) {
    i = line.indexOf(":");
    key = line.substring(0, i).trim().toLowerCase();
    val = line.substring(i + 1).trim();
    if (!key || parsed[key] && ignoreDuplicateOf[key]) {
      return;
    }
    if (key === "set-cookie") {
      if (parsed[key]) {
        parsed[key].push(val);
      } else {
        parsed[key] = [val];
      }
    } else {
      parsed[key] = parsed[key] ? parsed[key] + ", " + val : val;
    }
  });
  return parsed;
};

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/sanitizeHeaderValue.js
function trimSPorHTAB(str) {
  let start = 0;
  let end = str.length;
  while (start < end) {
    const code = str.charCodeAt(start);
    if (code !== 9 && code !== 32) {
      break;
    }
    start += 1;
  }
  while (end > start) {
    const code = str.charCodeAt(end - 1);
    if (code !== 9 && code !== 32) {
      break;
    }
    end -= 1;
  }
  return start === 0 && end === str.length ? str : str.slice(start, end);
}
var INVALID_UNICODE_HEADER_VALUE_CHARS = new RegExp("[\\u0000-\\u0008\\u000a-\\u001f\\u007f]+", "g");
var INVALID_BYTE_STRING_HEADER_VALUE_CHARS = new RegExp("[^\\u0009\\u0020-\\u007e\\u0080-\\u00ff]+", "g");
function sanitizeValue(value, invalidChars) {
  if (utils_default.isArray(value)) {
    return value.map((item) => sanitizeValue(item, invalidChars));
  }
  return trimSPorHTAB(String(value).replace(invalidChars, ""));
}
var sanitizeHeaderValue = (value) => sanitizeValue(value, INVALID_UNICODE_HEADER_VALUE_CHARS);
var sanitizeByteStringHeaderValue = (value) => sanitizeValue(value, INVALID_BYTE_STRING_HEADER_VALUE_CHARS);
function toByteStringHeaderObject(headers) {
  const byteStringHeaders = /* @__PURE__ */ Object.create(null);
  utils_default.forEach(headers.toJSON(), (value, header) => {
    byteStringHeaders[header] = sanitizeByteStringHeaderValue(value);
  });
  return byteStringHeaders;
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/core/AxiosHeaders.js
var $internals = /* @__PURE__ */ Symbol("internals");
function normalizeHeader(header) {
  return header && String(header).trim().toLowerCase();
}
function normalizeValue(value) {
  if (value === false || value == null) {
    return value;
  }
  return utils_default.isArray(value) ? value.map(normalizeValue) : sanitizeHeaderValue(String(value));
}
function parseTokens(str) {
  const tokens = /* @__PURE__ */ Object.create(null);
  const tokensRE = /([^\s,;=]+)\s*(?:=\s*([^,;]+))?/g;
  let match;
  while (match = tokensRE.exec(str)) {
    tokens[match[1]] = match[2];
  }
  return tokens;
}
var isValidHeaderName = (str) => /^[-_a-zA-Z0-9^`|~,!#$%&'*+.]+$/.test(str.trim());
function matchHeaderValue(context, value, header, filter2, isHeaderNameFilter) {
  if (utils_default.isFunction(filter2)) {
    return filter2.call(this, value, header);
  }
  if (isHeaderNameFilter) {
    value = header;
  }
  if (!utils_default.isString(value)) return;
  if (utils_default.isString(filter2)) {
    return value.indexOf(filter2) !== -1;
  }
  if (utils_default.isRegExp(filter2)) {
    return filter2.test(value);
  }
}
function formatHeader(header) {
  return header.trim().toLowerCase().replace(/([a-z\d])(\w*)/g, (w, char, str) => {
    return char.toUpperCase() + str;
  });
}
function buildAccessors(obj, header) {
  const accessorName = utils_default.toCamelCase(" " + header);
  ["get", "set", "has"].forEach((methodName) => {
    Object.defineProperty(obj, methodName + accessorName, {
      // Null-proto descriptor so a polluted Object.prototype.get cannot turn
      // this data descriptor into an accessor descriptor on the way in.
      __proto__: null,
      value: function(arg1, arg2, arg3) {
        return this[methodName].call(this, header, arg1, arg2, arg3);
      },
      configurable: true
    });
  });
}
var AxiosHeaders = class {
  constructor(headers) {
    headers && this.set(headers);
  }
  set(header, valueOrRewrite, rewrite) {
    const self2 = this;
    function setHeader(_value, _header, _rewrite) {
      const lHeader = normalizeHeader(_header);
      if (!lHeader) {
        throw new Error("header name must be a non-empty string");
      }
      const key = utils_default.findKey(self2, lHeader);
      if (!key || self2[key] === void 0 || _rewrite === true || _rewrite === void 0 && self2[key] !== false) {
        self2[key || _header] = normalizeValue(_value);
      }
    }
    const setHeaders = (headers, _rewrite) => utils_default.forEach(headers, (_value, _header) => setHeader(_value, _header, _rewrite));
    if (utils_default.isPlainObject(header) || header instanceof this.constructor) {
      setHeaders(header, valueOrRewrite);
    } else if (utils_default.isString(header) && (header = header.trim()) && !isValidHeaderName(header)) {
      setHeaders(parseHeaders_default(header), valueOrRewrite);
    } else if (utils_default.isObject(header) && utils_default.isIterable(header)) {
      let obj = {}, dest, key;
      for (const entry of header) {
        if (!utils_default.isArray(entry)) {
          throw TypeError("Object iterator must return a key-value pair");
        }
        obj[key = entry[0]] = (dest = obj[key]) ? utils_default.isArray(dest) ? [...dest, entry[1]] : [dest, entry[1]] : entry[1];
      }
      setHeaders(obj, valueOrRewrite);
    } else {
      header != null && setHeader(valueOrRewrite, header, rewrite);
    }
    return this;
  }
  get(header, parser) {
    header = normalizeHeader(header);
    if (header) {
      const key = utils_default.findKey(this, header);
      if (key) {
        const value = this[key];
        if (!parser) {
          return value;
        }
        if (parser === true) {
          return parseTokens(value);
        }
        if (utils_default.isFunction(parser)) {
          return parser.call(this, value, key);
        }
        if (utils_default.isRegExp(parser)) {
          return parser.exec(value);
        }
        throw new TypeError("parser must be boolean|regexp|function");
      }
    }
  }
  has(header, matcher) {
    header = normalizeHeader(header);
    if (header) {
      const key = utils_default.findKey(this, header);
      return !!(key && this[key] !== void 0 && (!matcher || matchHeaderValue(this, this[key], key, matcher)));
    }
    return false;
  }
  delete(header, matcher) {
    const self2 = this;
    let deleted = false;
    function deleteHeader(_header) {
      _header = normalizeHeader(_header);
      if (_header) {
        const key = utils_default.findKey(self2, _header);
        if (key && (!matcher || matchHeaderValue(self2, self2[key], key, matcher))) {
          delete self2[key];
          deleted = true;
        }
      }
    }
    if (utils_default.isArray(header)) {
      header.forEach(deleteHeader);
    } else {
      deleteHeader(header);
    }
    return deleted;
  }
  clear(matcher) {
    const keys = Object.keys(this);
    let i = keys.length;
    let deleted = false;
    while (i--) {
      const key = keys[i];
      if (!matcher || matchHeaderValue(this, this[key], key, matcher, true)) {
        delete this[key];
        deleted = true;
      }
    }
    return deleted;
  }
  normalize(format) {
    const self2 = this;
    const headers = {};
    utils_default.forEach(this, (value, header) => {
      const key = utils_default.findKey(headers, header);
      if (key) {
        self2[key] = normalizeValue(value);
        delete self2[header];
        return;
      }
      const normalized = format ? formatHeader(header) : String(header).trim();
      if (normalized !== header) {
        delete self2[header];
      }
      self2[normalized] = normalizeValue(value);
      headers[normalized] = true;
    });
    return this;
  }
  concat(...targets) {
    return this.constructor.concat(this, ...targets);
  }
  toJSON(asStrings) {
    const obj = /* @__PURE__ */ Object.create(null);
    utils_default.forEach(this, (value, header) => {
      value != null && value !== false && (obj[header] = asStrings && utils_default.isArray(value) ? value.join(", ") : value);
    });
    return obj;
  }
  [Symbol.iterator]() {
    return Object.entries(this.toJSON())[Symbol.iterator]();
  }
  toString() {
    return Object.entries(this.toJSON()).map(([header, value]) => header + ": " + value).join("\n");
  }
  getSetCookie() {
    return this.get("set-cookie") || [];
  }
  get [Symbol.toStringTag]() {
    return "AxiosHeaders";
  }
  static from(thing) {
    return thing instanceof this ? thing : new this(thing);
  }
  static concat(first, ...targets) {
    const computed = new this(first);
    targets.forEach((target) => computed.set(target));
    return computed;
  }
  static accessor(header) {
    const internals = this[$internals] = this[$internals] = {
      accessors: {}
    };
    const accessors = internals.accessors;
    const prototype2 = this.prototype;
    function defineAccessor(_header) {
      const lHeader = normalizeHeader(_header);
      if (!accessors[lHeader]) {
        buildAccessors(prototype2, _header);
        accessors[lHeader] = true;
      }
    }
    utils_default.isArray(header) ? header.forEach(defineAccessor) : defineAccessor(header);
    return this;
  }
};
AxiosHeaders.accessor([
  "Content-Type",
  "Content-Length",
  "Accept",
  "Accept-Encoding",
  "User-Agent",
  "Authorization"
]);
utils_default.reduceDescriptors(AxiosHeaders.prototype, ({ value }, key) => {
  let mapped = key[0].toUpperCase() + key.slice(1);
  return {
    get: () => value,
    set(headerValue) {
      this[mapped] = headerValue;
    }
  };
});
utils_default.freezeMethods(AxiosHeaders);
var AxiosHeaders_default = AxiosHeaders;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/core/AxiosError.js
var REDACTED = "[REDACTED ****]";
function hasOwnOrPrototypeToJSON(source) {
  if (utils_default.hasOwnProp(source, "toJSON")) {
    return true;
  }
  let prototype2 = Object.getPrototypeOf(source);
  while (prototype2 && prototype2 !== Object.prototype) {
    if (utils_default.hasOwnProp(prototype2, "toJSON")) {
      return true;
    }
    prototype2 = Object.getPrototypeOf(prototype2);
  }
  return false;
}
function redactConfig(config, redactKeys) {
  const lowerKeys = new Set(redactKeys.map((k) => String(k).toLowerCase()));
  const seen = [];
  const visit = (source) => {
    if (source === null || typeof source !== "object") return source;
    if (utils_default.isBuffer(source)) return source;
    if (seen.indexOf(source) !== -1) return void 0;
    if (source instanceof AxiosHeaders_default) {
      source = source.toJSON();
    }
    seen.push(source);
    let result;
    if (utils_default.isArray(source)) {
      result = [];
      source.forEach((v, i) => {
        const reducedValue = visit(v);
        if (!utils_default.isUndefined(reducedValue)) {
          result[i] = reducedValue;
        }
      });
    } else {
      if (!utils_default.isPlainObject(source) && hasOwnOrPrototypeToJSON(source)) {
        seen.pop();
        return source;
      }
      result = /* @__PURE__ */ Object.create(null);
      for (const [key, value] of Object.entries(source)) {
        const reducedValue = lowerKeys.has(key.toLowerCase()) ? REDACTED : visit(value);
        if (!utils_default.isUndefined(reducedValue)) {
          result[key] = reducedValue;
        }
      }
    }
    seen.pop();
    return result;
  };
  return visit(config);
}
var AxiosError = class _AxiosError extends Error {
  static from(error, code, config, request, response, customProps) {
    const axiosError = new _AxiosError(error.message, code || error.code, config, request, response);
    axiosError.cause = error;
    axiosError.name = error.name;
    if (error.status != null && axiosError.status == null) {
      axiosError.status = error.status;
    }
    customProps && Object.assign(axiosError, customProps);
    return axiosError;
  }
  /**
   * Create an Error with the specified message, config, error code, request and response.
   *
   * @param {string} message The error message.
   * @param {string} [code] The error code (for example, 'ECONNABORTED').
   * @param {Object} [config] The config.
   * @param {Object} [request] The request.
   * @param {Object} [response] The response.
   *
   * @returns {Error} The created error.
   */
  constructor(message, code, config, request, response) {
    super(message);
    Object.defineProperty(this, "message", {
      // Null-proto descriptor so a polluted Object.prototype.get cannot turn
      // this data descriptor into an accessor descriptor on the way in.
      __proto__: null,
      value: message,
      enumerable: true,
      writable: true,
      configurable: true
    });
    this.name = "AxiosError";
    this.isAxiosError = true;
    code && (this.code = code);
    config && (this.config = config);
    request && (this.request = request);
    if (response) {
      this.response = response;
      this.status = response.status;
    }
  }
  toJSON() {
    const config = this.config;
    const redactKeys = config && utils_default.hasOwnProp(config, "redact") ? config.redact : void 0;
    const serializedConfig = utils_default.isArray(redactKeys) && redactKeys.length > 0 ? redactConfig(config, redactKeys) : utils_default.toJSONObject(config);
    return {
      // Standard
      message: this.message,
      name: this.name,
      // Microsoft
      description: this.description,
      number: this.number,
      // Mozilla
      fileName: this.fileName,
      lineNumber: this.lineNumber,
      columnNumber: this.columnNumber,
      stack: this.stack,
      // Axios
      config: serializedConfig,
      code: this.code,
      status: this.status
    };
  }
};
AxiosError.ERR_BAD_OPTION_VALUE = "ERR_BAD_OPTION_VALUE";
AxiosError.ERR_BAD_OPTION = "ERR_BAD_OPTION";
AxiosError.ECONNABORTED = "ECONNABORTED";
AxiosError.ETIMEDOUT = "ETIMEDOUT";
AxiosError.ECONNREFUSED = "ECONNREFUSED";
AxiosError.ERR_NETWORK = "ERR_NETWORK";
AxiosError.ERR_FR_TOO_MANY_REDIRECTS = "ERR_FR_TOO_MANY_REDIRECTS";
AxiosError.ERR_DEPRECATED = "ERR_DEPRECATED";
AxiosError.ERR_BAD_RESPONSE = "ERR_BAD_RESPONSE";
AxiosError.ERR_BAD_REQUEST = "ERR_BAD_REQUEST";
AxiosError.ERR_CANCELED = "ERR_CANCELED";
AxiosError.ERR_NOT_SUPPORT = "ERR_NOT_SUPPORT";
AxiosError.ERR_INVALID_URL = "ERR_INVALID_URL";
AxiosError.ERR_FORM_DATA_DEPTH_EXCEEDED = "ERR_FORM_DATA_DEPTH_EXCEEDED";
var AxiosError_default = AxiosError;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/platform/node/classes/FormData.js
var import_form_data = __toESM(require_form_data(), 1);
var FormData_default = import_form_data.default;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/toFormData.js
function isVisitable(thing) {
  return utils_default.isPlainObject(thing) || utils_default.isArray(thing);
}
function removeBrackets(key) {
  return utils_default.endsWith(key, "[]") ? key.slice(0, -2) : key;
}
function renderKey(path8, key, dots) {
  if (!path8) return key;
  return path8.concat(key).map(function each(token, i) {
    token = removeBrackets(token);
    return !dots && i ? "[" + token + "]" : token;
  }).join(dots ? "." : "");
}
function isFlatArray(arr) {
  return utils_default.isArray(arr) && !arr.some(isVisitable);
}
var predicates = utils_default.toFlatObject(utils_default, {}, null, function filter(prop) {
  return /^is[A-Z]/.test(prop);
});
function toFormData(obj, formData, options) {
  if (!utils_default.isObject(obj)) {
    throw new TypeError("target must be an object");
  }
  formData = formData || new (FormData_default || FormData)();
  options = utils_default.toFlatObject(
    options,
    {
      metaTokens: true,
      dots: false,
      indexes: false
    },
    false,
    function defined(option, source) {
      return !utils_default.isUndefined(source[option]);
    }
  );
  const metaTokens = options.metaTokens;
  const visitor = options.visitor || defaultVisitor;
  const dots = options.dots;
  const indexes = options.indexes;
  const _Blob = options.Blob || typeof Blob !== "undefined" && Blob;
  const maxDepth = options.maxDepth === void 0 ? 100 : options.maxDepth;
  const useBlob = _Blob && utils_default.isSpecCompliantForm(formData);
  if (!utils_default.isFunction(visitor)) {
    throw new TypeError("visitor must be a function");
  }
  function convertValue(value) {
    if (value === null) return "";
    if (utils_default.isDate(value)) {
      return value.toISOString();
    }
    if (utils_default.isBoolean(value)) {
      return value.toString();
    }
    if (!useBlob && utils_default.isBlob(value)) {
      throw new AxiosError_default("Blob is not supported. Use a Buffer instead.");
    }
    if (utils_default.isArrayBuffer(value) || utils_default.isTypedArray(value)) {
      return useBlob && typeof Blob === "function" ? new Blob([value]) : Buffer.from(value);
    }
    return value;
  }
  function defaultVisitor(value, key, path8) {
    let arr = value;
    if (utils_default.isReactNative(formData) && utils_default.isReactNativeBlob(value)) {
      formData.append(renderKey(path8, key, dots), convertValue(value));
      return false;
    }
    if (value && !path8 && typeof value === "object") {
      if (utils_default.endsWith(key, "{}")) {
        key = metaTokens ? key : key.slice(0, -2);
        value = JSON.stringify(value);
      } else if (utils_default.isArray(value) && isFlatArray(value) || (utils_default.isFileList(value) || utils_default.endsWith(key, "[]")) && (arr = utils_default.toArray(value))) {
        key = removeBrackets(key);
        arr.forEach(function each(el, index) {
          !(utils_default.isUndefined(el) || el === null) && formData.append(
            // eslint-disable-next-line no-nested-ternary
            indexes === true ? renderKey([key], index, dots) : indexes === null ? key : key + "[]",
            convertValue(el)
          );
        });
        return false;
      }
    }
    if (isVisitable(value)) {
      return true;
    }
    formData.append(renderKey(path8, key, dots), convertValue(value));
    return false;
  }
  const stack = [];
  const exposedHelpers = Object.assign(predicates, {
    defaultVisitor,
    convertValue,
    isVisitable
  });
  function build(value, path8, depth = 0) {
    if (utils_default.isUndefined(value)) return;
    if (depth > maxDepth) {
      throw new AxiosError_default(
        "Object is too deeply nested (" + depth + " levels). Max depth: " + maxDepth,
        AxiosError_default.ERR_FORM_DATA_DEPTH_EXCEEDED
      );
    }
    if (stack.indexOf(value) !== -1) {
      throw Error("Circular reference detected in " + path8.join("."));
    }
    stack.push(value);
    utils_default.forEach(value, function each(el, key) {
      const result = !(utils_default.isUndefined(el) || el === null) && visitor.call(formData, el, utils_default.isString(key) ? key.trim() : key, path8, exposedHelpers);
      if (result === true) {
        build(el, path8 ? path8.concat(key) : [key], depth + 1);
      }
    });
    stack.pop();
  }
  if (!utils_default.isObject(obj)) {
    throw new TypeError("data must be an object");
  }
  build(obj);
  return formData;
}
var toFormData_default = toFormData;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/AxiosURLSearchParams.js
function encode(str) {
  const charMap = {
    "!": "%21",
    "'": "%27",
    "(": "%28",
    ")": "%29",
    "~": "%7E",
    "%20": "+"
  };
  return encodeURIComponent(str).replace(/[!'()~]|%20/g, function replacer(match) {
    return charMap[match];
  });
}
function AxiosURLSearchParams(params, options) {
  this._pairs = [];
  params && toFormData_default(params, this, options);
}
var prototype = AxiosURLSearchParams.prototype;
prototype.append = function append(name, value) {
  this._pairs.push([name, value]);
};
prototype.toString = function toString2(encoder) {
  const _encode = encoder ? function(value) {
    return encoder.call(this, value, encode);
  } : encode;
  return this._pairs.map(function each(pair) {
    return _encode(pair[0]) + "=" + _encode(pair[1]);
  }, "").join("&");
};
var AxiosURLSearchParams_default = AxiosURLSearchParams;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/buildURL.js
function encode2(val) {
  return encodeURIComponent(val).replace(/%3A/gi, ":").replace(/%24/g, "$").replace(/%2C/gi, ",").replace(/%20/g, "+");
}
function buildURL(url2, params, options) {
  if (!params) {
    return url2;
  }
  const _encode = options && options.encode || encode2;
  const _options = utils_default.isFunction(options) ? {
    serialize: options
  } : options;
  const serializeFn = _options && _options.serialize;
  let serializedParams;
  if (serializeFn) {
    serializedParams = serializeFn(params, _options);
  } else {
    serializedParams = utils_default.isURLSearchParams(params) ? params.toString() : new AxiosURLSearchParams_default(params, _options).toString(_encode);
  }
  if (serializedParams) {
    const hashmarkIndex = url2.indexOf("#");
    if (hashmarkIndex !== -1) {
      url2 = url2.slice(0, hashmarkIndex);
    }
    url2 += (url2.indexOf("?") === -1 ? "?" : "&") + serializedParams;
  }
  return url2;
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/core/InterceptorManager.js
var InterceptorManager = class {
  constructor() {
    this.handlers = [];
  }
  /**
   * Add a new interceptor to the stack
   *
   * @param {Function} fulfilled The function to handle `then` for a `Promise`
   * @param {Function} rejected The function to handle `reject` for a `Promise`
   * @param {Object} options The options for the interceptor, synchronous and runWhen
   *
   * @return {Number} An ID used to remove interceptor later
   */
  use(fulfilled, rejected, options) {
    this.handlers.push({
      fulfilled,
      rejected,
      synchronous: options ? options.synchronous : false,
      runWhen: options ? options.runWhen : null
    });
    return this.handlers.length - 1;
  }
  /**
   * Remove an interceptor from the stack
   *
   * @param {Number} id The ID that was returned by `use`
   *
   * @returns {void}
   */
  eject(id) {
    if (this.handlers[id]) {
      this.handlers[id] = null;
    }
  }
  /**
   * Clear all interceptors from the stack
   *
   * @returns {void}
   */
  clear() {
    if (this.handlers) {
      this.handlers = [];
    }
  }
  /**
   * Iterate over all the registered interceptors
   *
   * This method is particularly useful for skipping over any
   * interceptors that may have become `null` calling `eject`.
   *
   * @param {Function} fn The function to call for each interceptor
   *
   * @returns {void}
   */
  forEach(fn) {
    utils_default.forEach(this.handlers, function forEachHandler(h) {
      if (h !== null) {
        fn(h);
      }
    });
  }
};
var InterceptorManager_default = InterceptorManager;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/defaults/transitional.js
var transitional_default = {
  silentJSONParsing: true,
  forcedJSONParsing: true,
  clarifyTimeoutError: false,
  legacyInterceptorReqResOrdering: true
};

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/platform/node/index.js
var import_crypto = __toESM(require("crypto"), 1);

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/platform/node/classes/URLSearchParams.js
var import_url = __toESM(require("url"), 1);
var URLSearchParams_default = import_url.default.URLSearchParams;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/platform/node/index.js
var ALPHA = "abcdefghijklmnopqrstuvwxyz";
var DIGIT = "0123456789";
var ALPHABET = {
  DIGIT,
  ALPHA,
  ALPHA_DIGIT: ALPHA + ALPHA.toUpperCase() + DIGIT
};
var generateString = (size = 16, alphabet = ALPHABET.ALPHA_DIGIT) => {
  let str = "";
  const { length } = alphabet;
  const randomValues = new Uint32Array(size);
  import_crypto.default.randomFillSync(randomValues);
  for (let i = 0; i < size; i++) {
    str += alphabet[randomValues[i] % length];
  }
  return str;
};
var node_default = {
  isNode: true,
  classes: {
    URLSearchParams: URLSearchParams_default,
    FormData: FormData_default,
    Blob: typeof Blob !== "undefined" && Blob || null
  },
  ALPHABET,
  generateString,
  protocols: ["http", "https", "file", "data"]
};

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/platform/common/utils.js
var utils_exports = {};
__export(utils_exports, {
  hasBrowserEnv: () => hasBrowserEnv,
  hasStandardBrowserEnv: () => hasStandardBrowserEnv,
  hasStandardBrowserWebWorkerEnv: () => hasStandardBrowserWebWorkerEnv,
  navigator: () => _navigator,
  origin: () => origin
});
var hasBrowserEnv = typeof window !== "undefined" && typeof document !== "undefined";
var _navigator = typeof navigator === "object" && navigator || void 0;
var hasStandardBrowserEnv = hasBrowserEnv && (!_navigator || ["ReactNative", "NativeScript", "NS"].indexOf(_navigator.product) < 0);
var hasStandardBrowserWebWorkerEnv = (() => {
  return typeof WorkerGlobalScope !== "undefined" && // eslint-disable-next-line no-undef
  self instanceof WorkerGlobalScope && typeof self.importScripts === "function";
})();
var origin = hasBrowserEnv && window.location.href || "http://localhost";

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/platform/index.js
var platform_default = {
  ...utils_exports,
  ...node_default
};

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/toURLEncodedForm.js
function toURLEncodedForm(data, options) {
  return toFormData_default(data, new platform_default.classes.URLSearchParams(), {
    visitor: function(value, key, path8, helpers) {
      if (platform_default.isNode && utils_default.isBuffer(value)) {
        this.append(key, value.toString("base64"));
        return false;
      }
      return helpers.defaultVisitor.apply(this, arguments);
    },
    ...options
  });
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/formDataToJSON.js
function parsePropPath(name) {
  return utils_default.matchAll(/\w+|\[(\w*)]/g, name).map((match) => {
    return match[0] === "[]" ? "" : match[1] || match[0];
  });
}
function arrayToObject(arr) {
  const obj = {};
  const keys = Object.keys(arr);
  let i;
  const len = keys.length;
  let key;
  for (i = 0; i < len; i++) {
    key = keys[i];
    obj[key] = arr[key];
  }
  return obj;
}
function formDataToJSON(formData) {
  function buildPath(path8, value, target, index) {
    let name = path8[index++];
    if (name === "__proto__") return true;
    const isNumericKey = Number.isFinite(+name);
    const isLast = index >= path8.length;
    name = !name && utils_default.isArray(target) ? target.length : name;
    if (isLast) {
      if (utils_default.hasOwnProp(target, name)) {
        target[name] = utils_default.isArray(target[name]) ? target[name].concat(value) : [target[name], value];
      } else {
        target[name] = value;
      }
      return !isNumericKey;
    }
    if (!utils_default.hasOwnProp(target, name) || !utils_default.isObject(target[name])) {
      target[name] = [];
    }
    const result = buildPath(path8, value, target[name], index);
    if (result && utils_default.isArray(target[name])) {
      target[name] = arrayToObject(target[name]);
    }
    return !isNumericKey;
  }
  if (utils_default.isFormData(formData) && utils_default.isFunction(formData.entries)) {
    const obj = {};
    utils_default.forEachEntry(formData, (name, value) => {
      buildPath(parsePropPath(name), value, obj, 0);
    });
    return obj;
  }
  return null;
}
var formDataToJSON_default = formDataToJSON;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/defaults/index.js
var own = (obj, key) => obj != null && utils_default.hasOwnProp(obj, key) ? obj[key] : void 0;
function stringifySafely(rawValue, parser, encoder) {
  if (utils_default.isString(rawValue)) {
    try {
      (parser || JSON.parse)(rawValue);
      return utils_default.trim(rawValue);
    } catch (e) {
      if (e.name !== "SyntaxError") {
        throw e;
      }
    }
  }
  return (encoder || JSON.stringify)(rawValue);
}
var defaults = {
  transitional: transitional_default,
  adapter: ["xhr", "http", "fetch"],
  transformRequest: [
    function transformRequest(data, headers) {
      const contentType = headers.getContentType() || "";
      const hasJSONContentType = contentType.indexOf("application/json") > -1;
      const isObjectPayload = utils_default.isObject(data);
      if (isObjectPayload && utils_default.isHTMLForm(data)) {
        data = new FormData(data);
      }
      const isFormData2 = utils_default.isFormData(data);
      if (isFormData2) {
        return hasJSONContentType ? JSON.stringify(formDataToJSON_default(data)) : data;
      }
      if (utils_default.isArrayBuffer(data) || utils_default.isBuffer(data) || utils_default.isStream(data) || utils_default.isFile(data) || utils_default.isBlob(data) || utils_default.isReadableStream(data)) {
        return data;
      }
      if (utils_default.isArrayBufferView(data)) {
        return data.buffer;
      }
      if (utils_default.isURLSearchParams(data)) {
        headers.setContentType("application/x-www-form-urlencoded;charset=utf-8", false);
        return data.toString();
      }
      let isFileList2;
      if (isObjectPayload) {
        const formSerializer = own(this, "formSerializer");
        if (contentType.indexOf("application/x-www-form-urlencoded") > -1) {
          return toURLEncodedForm(data, formSerializer).toString();
        }
        if ((isFileList2 = utils_default.isFileList(data)) || contentType.indexOf("multipart/form-data") > -1) {
          const env2 = own(this, "env");
          const _FormData = env2 && env2.FormData;
          return toFormData_default(
            isFileList2 ? { "files[]": data } : data,
            _FormData && new _FormData(),
            formSerializer
          );
        }
      }
      if (isObjectPayload || hasJSONContentType) {
        headers.setContentType("application/json", false);
        return stringifySafely(data);
      }
      return data;
    }
  ],
  transformResponse: [
    function transformResponse(data) {
      const transitional2 = own(this, "transitional") || defaults.transitional;
      const forcedJSONParsing = transitional2 && transitional2.forcedJSONParsing;
      const responseType = own(this, "responseType");
      const JSONRequested = responseType === "json";
      if (utils_default.isResponse(data) || utils_default.isReadableStream(data)) {
        return data;
      }
      if (data && utils_default.isString(data) && (forcedJSONParsing && !responseType || JSONRequested)) {
        const silentJSONParsing = transitional2 && transitional2.silentJSONParsing;
        const strictJSONParsing = !silentJSONParsing && JSONRequested;
        try {
          return JSON.parse(data, own(this, "parseReviver"));
        } catch (e) {
          if (strictJSONParsing) {
            if (e.name === "SyntaxError") {
              throw AxiosError_default.from(e, AxiosError_default.ERR_BAD_RESPONSE, this, null, own(this, "response"));
            }
            throw e;
          }
        }
      }
      return data;
    }
  ],
  /**
   * A timeout in milliseconds to abort a request. If set to 0 (default) a
   * timeout is not created.
   */
  timeout: 0,
  xsrfCookieName: "XSRF-TOKEN",
  xsrfHeaderName: "X-XSRF-TOKEN",
  maxContentLength: -1,
  maxBodyLength: -1,
  env: {
    FormData: platform_default.classes.FormData,
    Blob: platform_default.classes.Blob
  },
  validateStatus: function validateStatus(status) {
    return status >= 200 && status < 300;
  },
  headers: {
    common: {
      Accept: "application/json, text/plain, */*",
      "Content-Type": void 0
    }
  }
};
utils_default.forEach(["delete", "get", "head", "post", "put", "patch", "query"], (method) => {
  defaults.headers[method] = {};
});
var defaults_default = defaults;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/core/transformData.js
function transformData(fns, response) {
  const config = this || defaults_default;
  const context = response || config;
  const headers = AxiosHeaders_default.from(context.headers);
  let data = context.data;
  utils_default.forEach(fns, function transform(fn) {
    data = fn.call(config, data, headers.normalize(), response ? response.status : void 0);
  });
  headers.normalize();
  return data;
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/cancel/isCancel.js
function isCancel(value) {
  return !!(value && value.__CANCEL__);
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/cancel/CanceledError.js
var CanceledError = class extends AxiosError_default {
  /**
   * A `CanceledError` is an object that is thrown when an operation is canceled.
   *
   * @param {string=} message The message.
   * @param {Object=} config The config.
   * @param {Object=} request The request.
   *
   * @returns {CanceledError} The created error.
   */
  constructor(message, config, request) {
    super(message == null ? "canceled" : message, AxiosError_default.ERR_CANCELED, config, request);
    this.name = "CanceledError";
    this.__CANCEL__ = true;
  }
};
var CanceledError_default = CanceledError;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/core/settle.js
function settle(resolve2, reject, response) {
  const validateStatus2 = response.config.validateStatus;
  if (!response.status || !validateStatus2 || validateStatus2(response.status)) {
    resolve2(response);
  } else {
    reject(new AxiosError_default(
      "Request failed with status code " + response.status,
      response.status >= 400 && response.status < 500 ? AxiosError_default.ERR_BAD_REQUEST : AxiosError_default.ERR_BAD_RESPONSE,
      response.config,
      response.request,
      response
    ));
  }
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/isAbsoluteURL.js
function isAbsoluteURL(url2) {
  if (typeof url2 !== "string") {
    return false;
  }
  return /^([a-z][a-z\d+\-.]*:)?\/\//i.test(url2);
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/combineURLs.js
function combineURLs(baseURL, relativeURL) {
  return relativeURL ? baseURL.replace(/\/?\/$/, "") + "/" + relativeURL.replace(/^\/+/, "") : baseURL;
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/core/buildFullPath.js
function buildFullPath(baseURL, requestedURL, allowAbsoluteUrls) {
  let isRelativeUrl = !isAbsoluteURL(requestedURL);
  if (baseURL && (isRelativeUrl || allowAbsoluteUrls === false)) {
    return combineURLs(baseURL, requestedURL);
  }
  return requestedURL;
}

// ../../node_modules/.pnpm/proxy-from-env@2.1.0/node_modules/proxy-from-env/index.js
var DEFAULT_PORTS = {
  ftp: 21,
  gopher: 70,
  http: 80,
  https: 443,
  ws: 80,
  wss: 443
};
function parseUrl(urlString) {
  try {
    return new URL(urlString);
  } catch {
    return null;
  }
}
function getProxyForUrl(url2) {
  var parsedUrl = (typeof url2 === "string" ? parseUrl(url2) : url2) || {};
  var proto = parsedUrl.protocol;
  var hostname = parsedUrl.host;
  var port = parsedUrl.port;
  if (typeof hostname !== "string" || !hostname || typeof proto !== "string") {
    return "";
  }
  proto = proto.split(":", 1)[0];
  hostname = hostname.replace(/:\d*$/, "");
  port = parseInt(port) || DEFAULT_PORTS[proto] || 0;
  if (!shouldProxy(hostname, port)) {
    return "";
  }
  var proxy = getEnv(proto + "_proxy") || getEnv("all_proxy");
  if (proxy && proxy.indexOf("://") === -1) {
    proxy = proto + "://" + proxy;
  }
  return proxy;
}
function shouldProxy(hostname, port) {
  var NO_PROXY = getEnv("no_proxy").toLowerCase();
  if (!NO_PROXY) {
    return true;
  }
  if (NO_PROXY === "*") {
    return false;
  }
  return NO_PROXY.split(/[,\s]/).every(function(proxy) {
    if (!proxy) {
      return true;
    }
    var parsedProxy = proxy.match(/^(.+):(\d+)$/);
    var parsedProxyHostname = parsedProxy ? parsedProxy[1] : proxy;
    var parsedProxyPort = parsedProxy ? parseInt(parsedProxy[2]) : 0;
    if (parsedProxyPort && parsedProxyPort !== port) {
      return true;
    }
    if (!/^[.*]/.test(parsedProxyHostname)) {
      return hostname !== parsedProxyHostname;
    }
    if (parsedProxyHostname.charAt(0) === "*") {
      parsedProxyHostname = parsedProxyHostname.slice(1);
    }
    return !hostname.endsWith(parsedProxyHostname);
  });
}
function getEnv(key) {
  return process.env[key.toLowerCase()] || process.env[key.toUpperCase()] || "";
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/adapters/http.js
var import_https_proxy_agent = __toESM(require_dist(), 1);
var import_http = __toESM(require("http"), 1);
var import_https = __toESM(require("https"), 1);
var import_http2 = __toESM(require("http2"), 1);
var import_util2 = __toESM(require("util"), 1);
var import_path = require("path");
var import_follow_redirects = __toESM(require_follow_redirects(), 1);
var import_zlib = __toESM(require("zlib"), 1);

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/env/data.js
var VERSION = "1.16.1";

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/parseProtocol.js
function parseProtocol(url2) {
  const match = /^([-+\w]{1,25}):(?:\/\/)?/.exec(url2);
  return match && match[1] || "";
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/fromDataURI.js
var DATA_URL_PATTERN = /^([^,;]+\/[^,;]+)?((?:;[^,;=]+=[^,;]+)*)(;base64)?,([\s\S]*)$/;
function fromDataURI(uri, asBlob, options) {
  const _Blob = options && options.Blob || platform_default.classes.Blob;
  const protocol = parseProtocol(uri);
  if (asBlob === void 0 && _Blob) {
    asBlob = true;
  }
  if (protocol === "data") {
    uri = protocol.length ? uri.slice(protocol.length + 1) : uri;
    const match = DATA_URL_PATTERN.exec(uri);
    if (!match) {
      throw new AxiosError_default("Invalid URL", AxiosError_default.ERR_INVALID_URL);
    }
    const type = match[1];
    const params = match[2];
    const encoding = match[3] ? "base64" : "utf8";
    const body = match[4];
    let mime;
    if (type) {
      mime = params ? type + params : type;
    } else if (params) {
      mime = "text/plain" + params;
    }
    const buffer2 = Buffer.from(decodeURIComponent(body), encoding);
    if (asBlob) {
      if (!_Blob) {
        throw new AxiosError_default("Blob is not supported", AxiosError_default.ERR_NOT_SUPPORT);
      }
      return new _Blob([buffer2], { type: mime });
    }
    return buffer2;
  }
  throw new AxiosError_default("Unsupported protocol " + protocol, AxiosError_default.ERR_NOT_SUPPORT);
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/adapters/http.js
var import_stream4 = __toESM(require("stream"), 1);

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/AxiosTransformStream.js
var import_stream = __toESM(require("stream"), 1);
var kInternals = /* @__PURE__ */ Symbol("internals");
var AxiosTransformStream = class extends import_stream.default.Transform {
  constructor(options) {
    options = utils_default.toFlatObject(
      options,
      {
        maxRate: 0,
        chunkSize: 64 * 1024,
        minChunkSize: 100,
        timeWindow: 500,
        ticksRate: 2,
        samplesCount: 15
      },
      null,
      (prop, source) => {
        return !utils_default.isUndefined(source[prop]);
      }
    );
    super({
      readableHighWaterMark: options.chunkSize
    });
    const internals = this[kInternals] = {
      timeWindow: options.timeWindow,
      chunkSize: options.chunkSize,
      maxRate: options.maxRate,
      minChunkSize: options.minChunkSize,
      bytesSeen: 0,
      isCaptured: false,
      notifiedBytesLoaded: 0,
      ts: Date.now(),
      bytes: 0,
      onReadCallback: null
    };
    this.on("newListener", (event) => {
      if (event === "progress") {
        if (!internals.isCaptured) {
          internals.isCaptured = true;
        }
      }
    });
  }
  _read(size) {
    const internals = this[kInternals];
    if (internals.onReadCallback) {
      internals.onReadCallback();
    }
    return super._read(size);
  }
  _transform(chunk, encoding, callback) {
    const internals = this[kInternals];
    const maxRate = internals.maxRate;
    const readableHighWaterMark = this.readableHighWaterMark;
    const timeWindow = internals.timeWindow;
    const divider = 1e3 / timeWindow;
    const bytesThreshold = maxRate / divider;
    const minChunkSize = internals.minChunkSize !== false ? Math.max(internals.minChunkSize, bytesThreshold * 0.01) : 0;
    const pushChunk = (_chunk, _callback) => {
      const bytes = Buffer.byteLength(_chunk);
      internals.bytesSeen += bytes;
      internals.bytes += bytes;
      internals.isCaptured && this.emit("progress", internals.bytesSeen);
      if (this.push(_chunk)) {
        process.nextTick(_callback);
      } else {
        internals.onReadCallback = () => {
          internals.onReadCallback = null;
          process.nextTick(_callback);
        };
      }
    };
    const transformChunk = (_chunk, _callback) => {
      const chunkSize = Buffer.byteLength(_chunk);
      let chunkRemainder = null;
      let maxChunkSize = readableHighWaterMark;
      let bytesLeft;
      let passed = 0;
      if (maxRate) {
        const now = Date.now();
        if (!internals.ts || (passed = now - internals.ts) >= timeWindow) {
          internals.ts = now;
          bytesLeft = bytesThreshold - internals.bytes;
          internals.bytes = bytesLeft < 0 ? -bytesLeft : 0;
          passed = 0;
        }
        bytesLeft = bytesThreshold - internals.bytes;
      }
      if (maxRate) {
        if (bytesLeft <= 0) {
          return setTimeout(() => {
            _callback(null, _chunk);
          }, timeWindow - passed);
        }
        if (bytesLeft < maxChunkSize) {
          maxChunkSize = bytesLeft;
        }
      }
      if (maxChunkSize && chunkSize > maxChunkSize && chunkSize - maxChunkSize > minChunkSize) {
        chunkRemainder = _chunk.subarray(maxChunkSize);
        _chunk = _chunk.subarray(0, maxChunkSize);
      }
      pushChunk(
        _chunk,
        chunkRemainder ? () => {
          process.nextTick(_callback, null, chunkRemainder);
        } : _callback
      );
    };
    transformChunk(chunk, function transformNextChunk(err, _chunk) {
      if (err) {
        return callback(err);
      }
      if (_chunk) {
        transformChunk(_chunk, transformNextChunk);
      } else {
        callback(null);
      }
    });
  }
};
var AxiosTransformStream_default = AxiosTransformStream;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/adapters/http.js
var import_events = require("events");

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/formDataToStream.js
var import_util = __toESM(require("util"), 1);
var import_stream2 = require("stream");

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/readBlob.js
var { asyncIterator } = Symbol;
var readBlob = async function* (blob) {
  if (blob.stream) {
    yield* blob.stream();
  } else if (blob.arrayBuffer) {
    yield await blob.arrayBuffer();
  } else if (blob[asyncIterator]) {
    yield* blob[asyncIterator]();
  } else {
    yield blob;
  }
};
var readBlob_default = readBlob;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/formDataToStream.js
var BOUNDARY_ALPHABET = platform_default.ALPHABET.ALPHA_DIGIT + "-_";
var textEncoder = typeof TextEncoder === "function" ? new TextEncoder() : new import_util.default.TextEncoder();
var CRLF = "\r\n";
var CRLF_BYTES = textEncoder.encode(CRLF);
var CRLF_BYTES_COUNT = 2;
var FormDataPart = class {
  constructor(name, value) {
    const { escapeName } = this.constructor;
    const isStringValue = utils_default.isString(value);
    let headers = `Content-Disposition: form-data; name="${escapeName(name)}"${!isStringValue && value.name ? `; filename="${escapeName(value.name)}"` : ""}${CRLF}`;
    if (isStringValue) {
      value = textEncoder.encode(String(value).replace(/\r?\n|\r\n?/g, CRLF));
    } else {
      const safeType = String(value.type || "application/octet-stream").replace(/[\r\n]/g, "");
      headers += `Content-Type: ${safeType}${CRLF}`;
    }
    this.headers = textEncoder.encode(headers + CRLF);
    this.contentLength = isStringValue ? value.byteLength : value.size;
    this.size = this.headers.byteLength + this.contentLength + CRLF_BYTES_COUNT;
    this.name = name;
    this.value = value;
  }
  async *encode() {
    yield this.headers;
    const { value } = this;
    if (utils_default.isTypedArray(value)) {
      yield value;
    } else {
      yield* readBlob_default(value);
    }
    yield CRLF_BYTES;
  }
  static escapeName(name) {
    return String(name).replace(
      /[\r\n"]/g,
      (match) => ({
        "\r": "%0D",
        "\n": "%0A",
        '"': "%22"
      })[match]
    );
  }
};
var formDataToStream = (form, headersHandler, options) => {
  const {
    tag = "form-data-boundary",
    size = 25,
    boundary = tag + "-" + platform_default.generateString(size, BOUNDARY_ALPHABET)
  } = options || {};
  if (!utils_default.isFormData(form)) {
    throw TypeError("FormData instance required");
  }
  if (boundary.length < 1 || boundary.length > 70) {
    throw Error("boundary must be 1-70 characters long");
  }
  const boundaryBytes = textEncoder.encode("--" + boundary + CRLF);
  const footerBytes = textEncoder.encode("--" + boundary + "--" + CRLF);
  let contentLength = footerBytes.byteLength;
  const parts = Array.from(form.entries()).map(([name, value]) => {
    const part = new FormDataPart(name, value);
    contentLength += part.size;
    return part;
  });
  contentLength += boundaryBytes.byteLength * parts.length;
  contentLength = utils_default.toFiniteNumber(contentLength);
  const computedHeaders = {
    "Content-Type": `multipart/form-data; boundary=${boundary}`
  };
  if (Number.isFinite(contentLength)) {
    computedHeaders["Content-Length"] = contentLength;
  }
  headersHandler && headersHandler(computedHeaders);
  return import_stream2.Readable.from(
    (async function* () {
      for (const part of parts) {
        yield boundaryBytes;
        yield* part.encode();
      }
      yield footerBytes;
    })()
  );
};
var formDataToStream_default = formDataToStream;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/ZlibHeaderTransformStream.js
var import_stream3 = __toESM(require("stream"), 1);
var ZlibHeaderTransformStream = class extends import_stream3.default.Transform {
  __transform(chunk, encoding, callback) {
    this.push(chunk);
    callback();
  }
  _transform(chunk, encoding, callback) {
    if (chunk.length !== 0) {
      this._transform = this.__transform;
      if (chunk[0] !== 120) {
        const header = Buffer.alloc(2);
        header[0] = 120;
        header[1] = 156;
        this.push(header, encoding);
      }
    }
    this.__transform(chunk, encoding, callback);
  }
};
var ZlibHeaderTransformStream_default = ZlibHeaderTransformStream;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/callbackify.js
var callbackify = (fn, reducer) => {
  return utils_default.isAsyncFn(fn) ? function(...args) {
    const cb = args.pop();
    fn.apply(this, args).then((value) => {
      try {
        reducer ? cb(null, ...reducer(value)) : cb(null, value);
      } catch (err) {
        cb(err);
      }
    }, cb);
  } : fn;
};
var callbackify_default = callbackify;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/shouldBypassProxy.js
var LOOPBACK_HOSTNAMES = /* @__PURE__ */ new Set(["localhost"]);
var isIPv4Loopback = (host) => {
  const parts = host.split(".");
  if (parts.length !== 4) return false;
  if (parts[0] !== "127") return false;
  return parts.every((p) => /^\d+$/.test(p) && Number(p) >= 0 && Number(p) <= 255);
};
var isIPv6Loopback = (host) => {
  if (host === "::1") return true;
  const v4MappedDotted = host.match(/^::ffff:(\d+\.\d+\.\d+\.\d+)$/i);
  if (v4MappedDotted) return isIPv4Loopback(v4MappedDotted[1]);
  const v4MappedHex = host.match(/^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/i);
  if (v4MappedHex) {
    const high = parseInt(v4MappedHex[1], 16);
    return high >= 32512 && high <= 32767;
  }
  const groups = host.split(":");
  if (groups.length === 8) {
    for (let i = 0; i < 7; i++) {
      if (!/^0+$/.test(groups[i])) return false;
    }
    return /^0*1$/.test(groups[7]);
  }
  return false;
};
var isLoopback = (host) => {
  if (!host) return false;
  if (LOOPBACK_HOSTNAMES.has(host)) return true;
  if (isIPv4Loopback(host)) return true;
  return isIPv6Loopback(host);
};
var DEFAULT_PORTS2 = {
  http: 80,
  https: 443,
  ws: 80,
  wss: 443,
  ftp: 21
};
var parseNoProxyEntry = (entry) => {
  let entryHost = entry;
  let entryPort = 0;
  if (entryHost.charAt(0) === "[") {
    const bracketIndex = entryHost.indexOf("]");
    if (bracketIndex !== -1) {
      const host = entryHost.slice(1, bracketIndex);
      const rest = entryHost.slice(bracketIndex + 1);
      if (rest.charAt(0) === ":" && /^\d+$/.test(rest.slice(1))) {
        entryPort = Number.parseInt(rest.slice(1), 10);
      }
      return [host, entryPort];
    }
  }
  const firstColon = entryHost.indexOf(":");
  const lastColon = entryHost.lastIndexOf(":");
  if (firstColon !== -1 && firstColon === lastColon && /^\d+$/.test(entryHost.slice(lastColon + 1))) {
    entryPort = Number.parseInt(entryHost.slice(lastColon + 1), 10);
    entryHost = entryHost.slice(0, lastColon);
  }
  return [entryHost, entryPort];
};
var IPV4_MAPPED_DOTTED_RE = /^(?:::|(?:0{1,4}:){1,4}:|(?:0{1,4}:){5})ffff:(\d+\.\d+\.\d+\.\d+)$/i;
var IPV4_MAPPED_HEX_RE = /^(?:::|(?:0{1,4}:){1,4}:|(?:0{1,4}:){5})ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/i;
var unmapIPv4MappedIPv6 = (host) => {
  if (typeof host !== "string" || host.indexOf(":") === -1) return host;
  const dotted = host.match(IPV4_MAPPED_DOTTED_RE);
  if (dotted) return dotted[1];
  const hex = host.match(IPV4_MAPPED_HEX_RE);
  if (hex) {
    const high = parseInt(hex[1], 16);
    const low = parseInt(hex[2], 16);
    return `${high >> 8}.${high & 255}.${low >> 8}.${low & 255}`;
  }
  return host;
};
var normalizeNoProxyHost = (hostname) => {
  if (!hostname) {
    return hostname;
  }
  if (hostname.charAt(0) === "[" && hostname.charAt(hostname.length - 1) === "]") {
    hostname = hostname.slice(1, -1);
  }
  return unmapIPv4MappedIPv6(hostname.replace(/\.+$/, ""));
};
function shouldBypassProxy(location) {
  let parsed;
  try {
    parsed = new URL(location);
  } catch (_err) {
    return false;
  }
  const noProxy = (process.env.no_proxy || process.env.NO_PROXY || "").toLowerCase();
  if (!noProxy) {
    return false;
  }
  if (noProxy === "*") {
    return true;
  }
  const port = Number.parseInt(parsed.port, 10) || DEFAULT_PORTS2[parsed.protocol.split(":", 1)[0]] || 0;
  const hostname = normalizeNoProxyHost(parsed.hostname.toLowerCase());
  return noProxy.split(/[\s,]+/).some((entry) => {
    if (!entry) {
      return false;
    }
    let [entryHost, entryPort] = parseNoProxyEntry(entry);
    entryHost = normalizeNoProxyHost(entryHost);
    if (!entryHost) {
      return false;
    }
    if (entryPort && entryPort !== port) {
      return false;
    }
    if (entryHost.charAt(0) === "*") {
      entryHost = entryHost.slice(1);
    }
    if (entryHost.charAt(0) === ".") {
      return hostname.endsWith(entryHost);
    }
    return hostname === entryHost || isLoopback(hostname) && isLoopback(entryHost);
  });
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/speedometer.js
function speedometer(samplesCount, min) {
  samplesCount = samplesCount || 10;
  const bytes = new Array(samplesCount);
  const timestamps = new Array(samplesCount);
  let head = 0;
  let tail = 0;
  let firstSampleTS;
  min = min !== void 0 ? min : 1e3;
  return function push(chunkLength) {
    const now = Date.now();
    const startedAt = timestamps[tail];
    if (!firstSampleTS) {
      firstSampleTS = now;
    }
    bytes[head] = chunkLength;
    timestamps[head] = now;
    let i = tail;
    let bytesCount = 0;
    while (i !== head) {
      bytesCount += bytes[i++];
      i = i % samplesCount;
    }
    head = (head + 1) % samplesCount;
    if (head === tail) {
      tail = (tail + 1) % samplesCount;
    }
    if (now - firstSampleTS < min) {
      return;
    }
    const passed = startedAt && now - startedAt;
    return passed ? Math.round(bytesCount * 1e3 / passed) : void 0;
  };
}
var speedometer_default = speedometer;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/throttle.js
function throttle(fn, freq) {
  let timestamp = 0;
  let threshold = 1e3 / freq;
  let lastArgs;
  let timer;
  const invoke = (args, now = Date.now()) => {
    timestamp = now;
    lastArgs = null;
    if (timer) {
      clearTimeout(timer);
      timer = null;
    }
    fn(...args);
  };
  const throttled = (...args) => {
    const now = Date.now();
    const passed = now - timestamp;
    if (passed >= threshold) {
      invoke(args, now);
    } else {
      lastArgs = args;
      if (!timer) {
        timer = setTimeout(() => {
          timer = null;
          invoke(lastArgs);
        }, threshold - passed);
      }
    }
  };
  const flush = () => lastArgs && invoke(lastArgs);
  return [throttled, flush];
}
var throttle_default = throttle;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/progressEventReducer.js
var progressEventReducer = (listener, isDownloadStream, freq = 3) => {
  let bytesNotified = 0;
  const _speedometer = speedometer_default(50, 250);
  return throttle_default((e) => {
    if (!e || typeof e.loaded !== "number") {
      return;
    }
    const rawLoaded = e.loaded;
    const total = e.lengthComputable ? e.total : void 0;
    const loaded = total != null ? Math.min(rawLoaded, total) : rawLoaded;
    const progressBytes = Math.max(0, loaded - bytesNotified);
    const rate = _speedometer(progressBytes);
    bytesNotified = Math.max(bytesNotified, loaded);
    const data = {
      loaded,
      total,
      progress: total ? loaded / total : void 0,
      bytes: progressBytes,
      rate: rate ? rate : void 0,
      estimated: rate && total ? (total - loaded) / rate : void 0,
      event: e,
      lengthComputable: total != null,
      [isDownloadStream ? "download" : "upload"]: true
    };
    listener(data);
  }, freq);
};
var progressEventDecorator = (total, throttled) => {
  const lengthComputable = total != null;
  return [
    (loaded) => throttled[0]({
      lengthComputable,
      total,
      loaded
    }),
    throttled[1]
  ];
};
var asyncDecorator = (fn) => (...args) => utils_default.asap(() => fn(...args));

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/estimateDataURLDecodedBytes.js
function estimateDataURLDecodedBytes(url2) {
  if (!url2 || typeof url2 !== "string") return 0;
  if (!url2.startsWith("data:")) return 0;
  const comma = url2.indexOf(",");
  if (comma < 0) return 0;
  const meta = url2.slice(5, comma);
  const body = url2.slice(comma + 1);
  const isBase64 = /;base64/i.test(meta);
  if (isBase64) {
    let effectiveLen = body.length;
    const len = body.length;
    for (let i = 0; i < len; i++) {
      if (body.charCodeAt(i) === 37 && i + 2 < len) {
        const a = body.charCodeAt(i + 1);
        const b = body.charCodeAt(i + 2);
        const isHex = (a >= 48 && a <= 57 || a >= 65 && a <= 70 || a >= 97 && a <= 102) && (b >= 48 && b <= 57 || b >= 65 && b <= 70 || b >= 97 && b <= 102);
        if (isHex) {
          effectiveLen -= 2;
          i += 2;
        }
      }
    }
    let pad = 0;
    let idx = len - 1;
    const tailIsPct3D = (j) => j >= 2 && body.charCodeAt(j - 2) === 37 && // '%'
    body.charCodeAt(j - 1) === 51 && // '3'
    (body.charCodeAt(j) === 68 || body.charCodeAt(j) === 100);
    if (idx >= 0) {
      if (body.charCodeAt(idx) === 61) {
        pad++;
        idx--;
      } else if (tailIsPct3D(idx)) {
        pad++;
        idx -= 3;
      }
    }
    if (pad === 1 && idx >= 0) {
      if (body.charCodeAt(idx) === 61) {
        pad++;
      } else if (tailIsPct3D(idx)) {
        pad++;
      }
    }
    const groups = Math.floor(effectiveLen / 4);
    const bytes2 = groups * 3 - (pad || 0);
    return bytes2 > 0 ? bytes2 : 0;
  }
  if (typeof Buffer !== "undefined" && typeof Buffer.byteLength === "function") {
    return Buffer.byteLength(body, "utf8");
  }
  let bytes = 0;
  for (let i = 0, len = body.length; i < len; i++) {
    const c = body.charCodeAt(i);
    if (c < 128) {
      bytes += 1;
    } else if (c < 2048) {
      bytes += 2;
    } else if (c >= 55296 && c <= 56319 && i + 1 < len) {
      const next = body.charCodeAt(i + 1);
      if (next >= 56320 && next <= 57343) {
        bytes += 4;
        i++;
      } else {
        bytes += 3;
      }
    } else {
      bytes += 3;
    }
  }
  return bytes;
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/adapters/http.js
var zlibOptions = {
  flush: import_zlib.default.constants.Z_SYNC_FLUSH,
  finishFlush: import_zlib.default.constants.Z_SYNC_FLUSH
};
var brotliOptions = {
  flush: import_zlib.default.constants.BROTLI_OPERATION_FLUSH,
  finishFlush: import_zlib.default.constants.BROTLI_OPERATION_FLUSH
};
var isBrotliSupported = utils_default.isFunction(import_zlib.default.createBrotliDecompress);
var { http: httpFollow, https: httpsFollow } = import_follow_redirects.default;
var isHttps = /https:?/;
var FORM_DATA_CONTENT_HEADERS = ["content-type", "content-length"];
function setFormDataHeaders(headers, formHeaders, policy) {
  if (policy !== "content-only") {
    headers.set(formHeaders);
    return;
  }
  Object.entries(formHeaders).forEach(([key, val]) => {
    if (FORM_DATA_CONTENT_HEADERS.includes(key.toLowerCase())) {
      headers.set(key, val);
    }
  });
}
var kAxiosSocketListener = /* @__PURE__ */ Symbol("axios.http.socketListener");
var kAxiosCurrentReq = /* @__PURE__ */ Symbol("axios.http.currentReq");
var kAxiosInstalledTunnel = /* @__PURE__ */ Symbol("axios.http.installedTunnel");
var tunnelingAgentCache = /* @__PURE__ */ new Map();
var tunnelingAgentCacheUser = /* @__PURE__ */ new WeakMap();
function getTunnelingAgent(agentOptions, userHttpsAgent) {
  const key = agentOptions.protocol + "//" + agentOptions.hostname + ":" + (agentOptions.port || "") + "#" + (agentOptions.auth || "");
  const cache = userHttpsAgent ? tunnelingAgentCacheUser.get(userHttpsAgent) || tunnelingAgentCacheUser.set(userHttpsAgent, /* @__PURE__ */ new Map()).get(userHttpsAgent) : tunnelingAgentCache;
  let agent = cache.get(key);
  if (agent) return agent;
  const merged = userHttpsAgent && userHttpsAgent.options ? { ...userHttpsAgent.options, ...agentOptions } : agentOptions;
  agent = new import_https_proxy_agent.default(merged);
  agent[kAxiosInstalledTunnel] = true;
  cache.set(key, agent);
  return agent;
}
var supportedProtocols = platform_default.protocols.map((protocol) => {
  return protocol + ":";
});
var decodeURIComponentSafe = (value) => {
  if (!utils_default.isString(value)) {
    return value;
  }
  try {
    return decodeURIComponent(value);
  } catch (error) {
    return value;
  }
};
var flushOnFinish = (stream4, [throttled, flush]) => {
  stream4.on("end", flush).on("error", flush);
  return throttled;
};
var Http2Sessions = class {
  constructor() {
    this.sessions = /* @__PURE__ */ Object.create(null);
  }
  getSession(authority, options) {
    options = Object.assign(
      {
        sessionTimeout: 1e3
      },
      options
    );
    let authoritySessions = this.sessions[authority];
    if (authoritySessions) {
      let len = authoritySessions.length;
      for (let i = 0; i < len; i++) {
        const [sessionHandle, sessionOptions] = authoritySessions[i];
        if (!sessionHandle.destroyed && !sessionHandle.closed && import_util2.default.isDeepStrictEqual(sessionOptions, options)) {
          return sessionHandle;
        }
      }
    }
    const session = import_http2.default.connect(authority, options);
    let removed;
    const removeSession = () => {
      if (removed) {
        return;
      }
      removed = true;
      let entries = authoritySessions, len = entries.length, i = len;
      while (i--) {
        if (entries[i][0] === session) {
          if (len === 1) {
            delete this.sessions[authority];
          } else {
            entries.splice(i, 1);
          }
          if (!session.closed) {
            session.close();
          }
          return;
        }
      }
    };
    const originalRequestFn = session.request;
    const { sessionTimeout } = options;
    if (sessionTimeout != null) {
      let timer;
      let streamsCount = 0;
      session.request = function() {
        const stream4 = originalRequestFn.apply(this, arguments);
        streamsCount++;
        if (timer) {
          clearTimeout(timer);
          timer = null;
        }
        stream4.once("close", () => {
          if (!--streamsCount) {
            timer = setTimeout(() => {
              timer = null;
              removeSession();
            }, sessionTimeout);
          }
        });
        return stream4;
      };
    }
    session.once("close", removeSession);
    let entry = [session, options];
    authoritySessions ? authoritySessions.push(entry) : authoritySessions = this.sessions[authority] = [entry];
    return session;
  }
};
var http2Sessions = new Http2Sessions();
function dispatchBeforeRedirect(options, responseDetails, requestDetails) {
  if (options.beforeRedirects.proxy) {
    options.beforeRedirects.proxy(options);
  }
  if (options.beforeRedirects.config) {
    options.beforeRedirects.config(options, responseDetails, requestDetails);
  }
}
function setProxy(options, configProxy, location, isRedirect, configHttpsAgent) {
  let proxy = configProxy;
  if (!proxy && proxy !== false) {
    const proxyUrl = getProxyForUrl(location);
    if (proxyUrl) {
      if (!shouldBypassProxy(location)) {
        proxy = new URL(proxyUrl);
      }
    }
  }
  if (isRedirect && options.headers) {
    for (const name of Object.keys(options.headers)) {
      if (name.toLowerCase() === "proxy-authorization") {
        delete options.headers[name];
      }
    }
  }
  if (isRedirect && options.agent && options.agent[kAxiosInstalledTunnel]) {
    options.agent = void 0;
  }
  if (proxy) {
    const isProxyURL = proxy instanceof URL;
    const readProxyField = (key) => isProxyURL || utils_default.hasOwnProp(proxy, key) ? proxy[key] : void 0;
    const proxyUsername = readProxyField("username");
    const proxyPassword = readProxyField("password");
    let proxyAuth = utils_default.hasOwnProp(proxy, "auth") ? proxy.auth : void 0;
    if (proxyUsername) {
      proxyAuth = (proxyUsername || "") + ":" + (proxyPassword || "");
    }
    if (proxyAuth) {
      const authIsObject = typeof proxyAuth === "object";
      const authUsername = authIsObject && utils_default.hasOwnProp(proxyAuth, "username") ? proxyAuth.username : void 0;
      const authPassword = authIsObject && utils_default.hasOwnProp(proxyAuth, "password") ? proxyAuth.password : void 0;
      const validProxyAuth = Boolean(authUsername || authPassword);
      if (validProxyAuth) {
        proxyAuth = (authUsername || "") + ":" + (authPassword || "");
      } else if (authIsObject) {
        throw new AxiosError_default("Invalid proxy authorization", AxiosError_default.ERR_BAD_OPTION, { proxy });
      }
    }
    const targetIsHttps = isHttps.test(options.protocol);
    if (targetIsHttps) {
      if (!(configHttpsAgent instanceof import_https_proxy_agent.default)) {
        const proxyHost = readProxyField("hostname") || readProxyField("host");
        const proxyPort = readProxyField("port");
        const rawProxyProtocol = readProxyField("protocol");
        const normalizedProtocol = rawProxyProtocol ? rawProxyProtocol.includes(":") ? rawProxyProtocol : `${rawProxyProtocol}:` : "http:";
        const proxyHostForURL = proxyHost && proxyHost.includes(":") && !proxyHost.startsWith("[") ? `[${proxyHost}]` : proxyHost;
        const proxyURL = new URL(
          `${normalizedProtocol}//${proxyHostForURL}${proxyPort ? ":" + proxyPort : ""}`
        );
        const agentOptions = {
          protocol: proxyURL.protocol,
          hostname: proxyURL.hostname.replace(/^\[|\]$/g, ""),
          port: proxyURL.port,
          auth: proxyAuth && typeof proxyAuth === "string" ? proxyAuth : void 0
        };
        if (proxyURL.protocol === "https:") {
          agentOptions.ALPNProtocols = ["http/1.1"];
        }
        const tunnelingAgent = getTunnelingAgent(agentOptions, configHttpsAgent);
        options.agent = tunnelingAgent;
        if (options.agents) {
          options.agents.https = tunnelingAgent;
        }
      }
    } else {
      if (proxyAuth) {
        const base64 = Buffer.from(proxyAuth, "utf8").toString("base64");
        options.headers["Proxy-Authorization"] = "Basic " + base64;
      }
      let hasUserHostHeader = false;
      for (const name of Object.keys(options.headers)) {
        if (name.toLowerCase() === "host") {
          hasUserHostHeader = true;
          break;
        }
      }
      if (!hasUserHostHeader) {
        options.headers.host = options.hostname + (options.port ? ":" + options.port : "");
      }
      const proxyHost = readProxyField("hostname") || readProxyField("host");
      options.hostname = proxyHost;
      options.host = proxyHost;
      options.port = readProxyField("port");
      options.path = location;
      const proxyProtocol = readProxyField("protocol");
      if (proxyProtocol) {
        options.protocol = proxyProtocol.includes(":") ? proxyProtocol : `${proxyProtocol}:`;
      }
    }
  }
  options.beforeRedirects.proxy = function beforeRedirect(redirectOptions) {
    setProxy(redirectOptions, configProxy, redirectOptions.href, true, configHttpsAgent);
  };
}
var isHttpAdapterSupported = typeof process !== "undefined" && utils_default.kindOf(process) === "process";
var wrapAsync = (asyncExecutor) => {
  return new Promise((resolve2, reject) => {
    let onDone;
    let isDone;
    const done = (value, isRejected) => {
      if (isDone) return;
      isDone = true;
      onDone && onDone(value, isRejected);
    };
    const _resolve = (value) => {
      done(value);
      resolve2(value);
    };
    const _reject = (reason) => {
      done(reason, true);
      reject(reason);
    };
    asyncExecutor(_resolve, _reject, (onDoneHandler) => onDone = onDoneHandler).catch(_reject);
  });
};
var resolveFamily = ({ address, family }) => {
  if (!utils_default.isString(address)) {
    throw TypeError("address must be a string");
  }
  return {
    address,
    family: family || (address.indexOf(".") < 0 ? 6 : 4)
  };
};
var buildAddressEntry = (address, family) => resolveFamily(utils_default.isObject(address) ? address : { address, family });
var http2Transport = {
  request(options, cb) {
    const authority = options.protocol + "//" + options.hostname + ":" + (options.port || (options.protocol === "https:" ? 443 : 80));
    const { http2Options, headers } = options;
    const session = http2Sessions.getSession(authority, http2Options);
    const { HTTP2_HEADER_SCHEME, HTTP2_HEADER_METHOD, HTTP2_HEADER_PATH, HTTP2_HEADER_STATUS } = import_http2.default.constants;
    const http2Headers = {
      [HTTP2_HEADER_SCHEME]: options.protocol.replace(":", ""),
      [HTTP2_HEADER_METHOD]: options.method,
      [HTTP2_HEADER_PATH]: options.path
    };
    utils_default.forEach(headers, (header, name) => {
      name.charAt(0) !== ":" && (http2Headers[name] = header);
    });
    const req = session.request(http2Headers);
    req.once("response", (responseHeaders) => {
      const response = req;
      responseHeaders = Object.assign({}, responseHeaders);
      const status = responseHeaders[HTTP2_HEADER_STATUS];
      delete responseHeaders[HTTP2_HEADER_STATUS];
      response.headers = responseHeaders;
      response.statusCode = +status;
      cb(response);
    });
    return req;
  }
};
var http_default = isHttpAdapterSupported && function httpAdapter(config) {
  return wrapAsync(async function dispatchHttpRequest(resolve2, reject, onDone) {
    const own2 = (key) => utils_default.hasOwnProp(config, key) ? config[key] : void 0;
    let data = own2("data");
    let lookup2 = own2("lookup");
    let family = own2("family");
    let httpVersion = own2("httpVersion");
    if (httpVersion === void 0) httpVersion = 1;
    let http2Options = own2("http2Options");
    const responseType = own2("responseType");
    const responseEncoding = own2("responseEncoding");
    const method = config.method.toUpperCase();
    let isDone;
    let rejected = false;
    let req;
    let connectPhaseTimer;
    httpVersion = +httpVersion;
    if (Number.isNaN(httpVersion)) {
      throw TypeError(`Invalid protocol version: '${config.httpVersion}' is not a number`);
    }
    if (httpVersion !== 1 && httpVersion !== 2) {
      throw TypeError(`Unsupported protocol version '${httpVersion}'`);
    }
    const isHttp2 = httpVersion === 2;
    if (lookup2) {
      const _lookup = callbackify_default(lookup2, (value) => utils_default.isArray(value) ? value : [value]);
      lookup2 = (hostname, opt, cb) => {
        _lookup(hostname, opt, (err, arg0, arg1) => {
          if (err) {
            return cb(err);
          }
          const addresses = utils_default.isArray(arg0) ? arg0.map((addr) => buildAddressEntry(addr)) : [buildAddressEntry(arg0, arg1)];
          opt.all ? cb(err, addresses) : cb(err, addresses[0].address, addresses[0].family);
        });
      };
    }
    const abortEmitter = new import_events.EventEmitter();
    function abort(reason) {
      try {
        abortEmitter.emit(
          "abort",
          !reason || reason.type ? new CanceledError_default(null, config, req) : reason
        );
      } catch (err) {
        console.warn("emit error", err);
      }
    }
    function clearConnectPhaseTimer() {
      if (connectPhaseTimer) {
        clearTimeout(connectPhaseTimer);
        connectPhaseTimer = null;
      }
    }
    function createTimeoutError() {
      let timeoutErrorMessage = config.timeout ? "timeout of " + config.timeout + "ms exceeded" : "timeout exceeded";
      const transitional2 = config.transitional || transitional_default;
      if (config.timeoutErrorMessage) {
        timeoutErrorMessage = config.timeoutErrorMessage;
      }
      return new AxiosError_default(
        timeoutErrorMessage,
        transitional2.clarifyTimeoutError ? AxiosError_default.ETIMEDOUT : AxiosError_default.ECONNABORTED,
        config,
        req
      );
    }
    abortEmitter.once("abort", reject);
    const onFinished = () => {
      clearConnectPhaseTimer();
      if (config.cancelToken) {
        config.cancelToken.unsubscribe(abort);
      }
      if (config.signal) {
        config.signal.removeEventListener("abort", abort);
      }
      abortEmitter.removeAllListeners();
    };
    if (config.cancelToken || config.signal) {
      config.cancelToken && config.cancelToken.subscribe(abort);
      if (config.signal) {
        config.signal.aborted ? abort() : config.signal.addEventListener("abort", abort);
      }
    }
    onDone((response, isRejected) => {
      isDone = true;
      clearConnectPhaseTimer();
      if (isRejected) {
        rejected = true;
        onFinished();
        return;
      }
      const { data: data2 } = response;
      if (data2 instanceof import_stream4.default.Readable || data2 instanceof import_stream4.default.Duplex) {
        const offListeners = import_stream4.default.finished(data2, () => {
          offListeners();
          onFinished();
        });
      } else {
        onFinished();
      }
    });
    const fullPath = buildFullPath(config.baseURL, config.url, config.allowAbsoluteUrls);
    const parsed = new URL(fullPath, platform_default.hasBrowserEnv ? platform_default.origin : void 0);
    const protocol = parsed.protocol || supportedProtocols[0];
    if (protocol === "data:") {
      if (config.maxContentLength > -1) {
        const dataUrl = String(config.url || fullPath || "");
        const estimated = estimateDataURLDecodedBytes(dataUrl);
        if (estimated > config.maxContentLength) {
          return reject(
            new AxiosError_default(
              "maxContentLength size of " + config.maxContentLength + " exceeded",
              AxiosError_default.ERR_BAD_RESPONSE,
              config
            )
          );
        }
      }
      let convertedData;
      if (method !== "GET") {
        return settle(resolve2, reject, {
          status: 405,
          statusText: "method not allowed",
          headers: {},
          config
        });
      }
      try {
        convertedData = fromDataURI(config.url, responseType === "blob", {
          Blob: config.env && config.env.Blob
        });
      } catch (err) {
        throw AxiosError_default.from(err, AxiosError_default.ERR_BAD_REQUEST, config);
      }
      if (responseType === "text") {
        convertedData = convertedData.toString(responseEncoding);
        if (!responseEncoding || responseEncoding === "utf8") {
          convertedData = utils_default.stripBOM(convertedData);
        }
      } else if (responseType === "stream") {
        convertedData = import_stream4.default.Readable.from(convertedData);
      }
      return settle(resolve2, reject, {
        data: convertedData,
        status: 200,
        statusText: "OK",
        headers: new AxiosHeaders_default(),
        config
      });
    }
    if (supportedProtocols.indexOf(protocol) === -1) {
      return reject(
        new AxiosError_default("Unsupported protocol " + protocol, AxiosError_default.ERR_BAD_REQUEST, config)
      );
    }
    const headers = AxiosHeaders_default.from(config.headers).normalize();
    headers.set("User-Agent", "axios/" + VERSION, false);
    const { onUploadProgress, onDownloadProgress } = config;
    const maxRate = config.maxRate;
    let maxUploadRate = void 0;
    let maxDownloadRate = void 0;
    if (utils_default.isSpecCompliantForm(data)) {
      const userBoundary = headers.getContentType(/boundary=([-_\w\d]{10,70})/i);
      data = formDataToStream_default(
        data,
        (formHeaders) => {
          headers.set(formHeaders);
        },
        {
          tag: `axios-${VERSION}-boundary`,
          boundary: userBoundary && userBoundary[1] || void 0
        }
      );
    } else if (utils_default.isFormData(data) && utils_default.isFunction(data.getHeaders) && data.getHeaders !== Object.prototype.getHeaders) {
      setFormDataHeaders(headers, data.getHeaders(), own2("formDataHeaderPolicy"));
      if (!headers.hasContentLength()) {
        try {
          const knownLength = await import_util2.default.promisify(data.getLength).call(data);
          Number.isFinite(knownLength) && knownLength >= 0 && headers.setContentLength(knownLength);
        } catch (e) {
        }
      }
    } else if (utils_default.isBlob(data) || utils_default.isFile(data)) {
      data.size && headers.setContentType(data.type || "application/octet-stream");
      headers.setContentLength(data.size || 0);
      data = import_stream4.default.Readable.from(readBlob_default(data));
    } else if (data && !utils_default.isStream(data)) {
      if (Buffer.isBuffer(data)) {
      } else if (utils_default.isArrayBuffer(data)) {
        data = Buffer.from(new Uint8Array(data));
      } else if (utils_default.isString(data)) {
        data = Buffer.from(data, "utf-8");
      } else {
        return reject(
          new AxiosError_default(
            "Data after transformation must be a string, an ArrayBuffer, a Buffer, or a Stream",
            AxiosError_default.ERR_BAD_REQUEST,
            config
          )
        );
      }
      headers.setContentLength(data.length, false);
      if (config.maxBodyLength > -1 && data.length > config.maxBodyLength) {
        return reject(
          new AxiosError_default(
            "Request body larger than maxBodyLength limit",
            AxiosError_default.ERR_BAD_REQUEST,
            config
          )
        );
      }
    }
    const contentLength = utils_default.toFiniteNumber(headers.getContentLength());
    if (utils_default.isArray(maxRate)) {
      maxUploadRate = maxRate[0];
      maxDownloadRate = maxRate[1];
    } else {
      maxUploadRate = maxDownloadRate = maxRate;
    }
    if (data && (onUploadProgress || maxUploadRate)) {
      if (!utils_default.isStream(data)) {
        data = import_stream4.default.Readable.from(data, { objectMode: false });
      }
      data = import_stream4.default.pipeline(
        [
          data,
          new AxiosTransformStream_default({
            maxRate: utils_default.toFiniteNumber(maxUploadRate)
          })
        ],
        utils_default.noop
      );
      onUploadProgress && data.on(
        "progress",
        flushOnFinish(
          data,
          progressEventDecorator(
            contentLength,
            progressEventReducer(asyncDecorator(onUploadProgress), false, 3)
          )
        )
      );
    }
    let auth = void 0;
    const configAuth = own2("auth");
    if (configAuth) {
      const username = configAuth.username || "";
      const password = configAuth.password || "";
      auth = username + ":" + password;
    }
    if (!auth && parsed.username) {
      const urlUsername = decodeURIComponentSafe(parsed.username);
      const urlPassword = decodeURIComponentSafe(parsed.password);
      auth = urlUsername + ":" + urlPassword;
    }
    auth && headers.delete("authorization");
    let path8;
    try {
      path8 = buildURL(
        parsed.pathname + parsed.search,
        config.params,
        config.paramsSerializer
      ).replace(/^\?/, "");
    } catch (err) {
      const customErr = new Error(err.message);
      customErr.config = config;
      customErr.url = config.url;
      customErr.exists = true;
      return reject(customErr);
    }
    headers.set(
      "Accept-Encoding",
      "gzip, compress, deflate" + (isBrotliSupported ? ", br" : ""),
      false
    );
    const options = Object.assign(/* @__PURE__ */ Object.create(null), {
      path: path8,
      method,
      headers: toByteStringHeaderObject(headers),
      agents: { http: config.httpAgent, https: config.httpsAgent },
      auth,
      protocol,
      family,
      beforeRedirect: dispatchBeforeRedirect,
      beforeRedirects: /* @__PURE__ */ Object.create(null),
      http2Options
    });
    !utils_default.isUndefined(lookup2) && (options.lookup = lookup2);
    if (config.socketPath) {
      if (typeof config.socketPath !== "string") {
        return reject(
          new AxiosError_default("socketPath must be a string", AxiosError_default.ERR_BAD_OPTION_VALUE, config)
        );
      }
      if (config.allowedSocketPaths != null) {
        const allowed = Array.isArray(config.allowedSocketPaths) ? config.allowedSocketPaths : [config.allowedSocketPaths];
        const resolvedSocket = (0, import_path.resolve)(config.socketPath);
        const isAllowed = allowed.some(
          (entry) => typeof entry === "string" && (0, import_path.resolve)(entry) === resolvedSocket
        );
        if (!isAllowed) {
          return reject(
            new AxiosError_default(
              `socketPath "${config.socketPath}" is not permitted by allowedSocketPaths`,
              AxiosError_default.ERR_BAD_OPTION_VALUE,
              config
            )
          );
        }
      }
      options.socketPath = config.socketPath;
    } else {
      options.hostname = parsed.hostname.startsWith("[") ? parsed.hostname.slice(1, -1) : parsed.hostname;
      options.port = parsed.port;
      setProxy(
        options,
        config.proxy,
        protocol + "//" + parsed.hostname + (parsed.port ? ":" + parsed.port : "") + options.path,
        false,
        config.httpsAgent
      );
    }
    let transport;
    let isNativeTransport = false;
    const isHttpsRequest = isHttps.test(options.protocol);
    if (options.agent == null) {
      options.agent = isHttpsRequest ? config.httpsAgent : config.httpAgent;
    }
    if (isHttp2) {
      transport = http2Transport;
    } else {
      const configTransport = own2("transport");
      if (configTransport) {
        transport = configTransport;
      } else if (config.maxRedirects === 0) {
        transport = isHttpsRequest ? import_https.default : import_http.default;
        isNativeTransport = true;
      } else {
        if (config.maxRedirects) {
          options.maxRedirects = config.maxRedirects;
        }
        const configBeforeRedirect = own2("beforeRedirect");
        if (configBeforeRedirect) {
          options.beforeRedirects.config = configBeforeRedirect;
        }
        transport = isHttpsRequest ? httpsFollow : httpFollow;
      }
    }
    if (config.maxBodyLength > -1) {
      options.maxBodyLength = config.maxBodyLength;
    } else {
      options.maxBodyLength = Infinity;
    }
    options.insecureHTTPParser = Boolean(own2("insecureHTTPParser"));
    req = transport.request(options, function handleResponse(res) {
      clearConnectPhaseTimer();
      if (req.destroyed) return;
      const streams = [res];
      const responseLength = utils_default.toFiniteNumber(res.headers["content-length"]);
      if (onDownloadProgress || maxDownloadRate) {
        const transformStream = new AxiosTransformStream_default({
          maxRate: utils_default.toFiniteNumber(maxDownloadRate)
        });
        onDownloadProgress && transformStream.on(
          "progress",
          flushOnFinish(
            transformStream,
            progressEventDecorator(
              responseLength,
              progressEventReducer(asyncDecorator(onDownloadProgress), true, 3)
            )
          )
        );
        streams.push(transformStream);
      }
      let responseStream = res;
      const lastRequest = res.req || req;
      if (config.decompress !== false && res.headers["content-encoding"]) {
        if (method === "HEAD" || res.statusCode === 204) {
          delete res.headers["content-encoding"];
        }
        switch ((res.headers["content-encoding"] || "").toLowerCase()) {
          /*eslint default-case:0*/
          case "gzip":
          case "x-gzip":
          case "compress":
          case "x-compress":
            streams.push(import_zlib.default.createUnzip(zlibOptions));
            delete res.headers["content-encoding"];
            break;
          case "deflate":
            streams.push(new ZlibHeaderTransformStream_default());
            streams.push(import_zlib.default.createUnzip(zlibOptions));
            delete res.headers["content-encoding"];
            break;
          case "br":
            if (isBrotliSupported) {
              streams.push(import_zlib.default.createBrotliDecompress(brotliOptions));
              delete res.headers["content-encoding"];
            }
        }
      }
      responseStream = streams.length > 1 ? import_stream4.default.pipeline(streams, utils_default.noop) : streams[0];
      const response = {
        status: res.statusCode,
        statusText: res.statusMessage,
        headers: new AxiosHeaders_default(res.headers),
        config,
        request: lastRequest
      };
      if (responseType === "stream") {
        if (config.maxContentLength > -1) {
          const limit = config.maxContentLength;
          const source = responseStream;
          async function* enforceMaxContentLength() {
            let totalResponseBytes = 0;
            for await (const chunk of source) {
              totalResponseBytes += chunk.length;
              if (totalResponseBytes > limit) {
                throw new AxiosError_default(
                  "maxContentLength size of " + limit + " exceeded",
                  AxiosError_default.ERR_BAD_RESPONSE,
                  config,
                  lastRequest
                );
              }
              yield chunk;
            }
          }
          responseStream = import_stream4.default.Readable.from(enforceMaxContentLength(), {
            objectMode: false
          });
        }
        response.data = responseStream;
        settle(resolve2, reject, response);
      } else {
        const responseBuffer = [];
        let totalResponseBytes = 0;
        responseStream.on("data", function handleStreamData(chunk) {
          responseBuffer.push(chunk);
          totalResponseBytes += chunk.length;
          if (config.maxContentLength > -1 && totalResponseBytes > config.maxContentLength) {
            rejected = true;
            responseStream.destroy();
            abort(
              new AxiosError_default(
                "maxContentLength size of " + config.maxContentLength + " exceeded",
                AxiosError_default.ERR_BAD_RESPONSE,
                config,
                lastRequest
              )
            );
          }
        });
        responseStream.on("aborted", function handlerStreamAborted() {
          if (rejected) {
            return;
          }
          const err = new AxiosError_default(
            "stream has been aborted",
            AxiosError_default.ERR_BAD_RESPONSE,
            config,
            lastRequest,
            response
          );
          responseStream.destroy(err);
          reject(err);
        });
        responseStream.on("error", function handleStreamError(err) {
          if (rejected) return;
          reject(AxiosError_default.from(err, null, config, lastRequest, response));
        });
        responseStream.on("end", function handleStreamEnd() {
          try {
            let responseData = responseBuffer.length === 1 ? responseBuffer[0] : Buffer.concat(responseBuffer);
            if (responseType !== "arraybuffer") {
              responseData = responseData.toString(responseEncoding);
              if (!responseEncoding || responseEncoding === "utf8") {
                responseData = utils_default.stripBOM(responseData);
              }
            }
            response.data = responseData;
          } catch (err) {
            return reject(AxiosError_default.from(err, null, config, response.request, response));
          }
          settle(resolve2, reject, response);
        });
      }
      abortEmitter.once("abort", (err) => {
        if (!responseStream.destroyed) {
          responseStream.emit("error", err);
          responseStream.destroy();
        }
      });
    });
    abortEmitter.once("abort", (err) => {
      if (req.close) {
        req.close();
      } else {
        req.destroy(err);
      }
    });
    req.on("error", function handleRequestError(err) {
      reject(AxiosError_default.from(err, null, config, req));
    });
    const boundSockets = /* @__PURE__ */ new Set();
    req.on("socket", function handleRequestSocket(socket) {
      socket.setKeepAlive(true, 1e3 * 60);
      if (!socket[kAxiosSocketListener]) {
        socket.on("error", function handleSocketError(err) {
          const current = socket[kAxiosCurrentReq];
          if (current && !current.destroyed) {
            current.destroy(err);
          }
        });
        socket[kAxiosSocketListener] = true;
      }
      socket[kAxiosCurrentReq] = req;
      boundSockets.add(socket);
    });
    req.once("close", function clearCurrentReq() {
      clearConnectPhaseTimer();
      for (const socket of boundSockets) {
        if (socket[kAxiosCurrentReq] === req) {
          socket[kAxiosCurrentReq] = null;
        }
      }
      boundSockets.clear();
    });
    if (config.timeout) {
      const timeout = parseInt(config.timeout, 10);
      if (Number.isNaN(timeout)) {
        abort(
          new AxiosError_default(
            "error trying to parse `config.timeout` to int",
            AxiosError_default.ERR_BAD_OPTION_VALUE,
            config,
            req
          )
        );
        return;
      }
      const handleTimeout = function handleTimeout2() {
        if (isDone) return;
        abort(createTimeoutError());
      };
      if (isNativeTransport && timeout > 0) {
        connectPhaseTimer = setTimeout(handleTimeout, timeout);
      }
      req.setTimeout(timeout, handleTimeout);
    } else {
      req.setTimeout(0);
    }
    if (utils_default.isStream(data)) {
      let ended = false;
      let errored = false;
      data.on("end", () => {
        ended = true;
      });
      data.once("error", (err) => {
        errored = true;
        req.destroy(err);
      });
      data.on("close", () => {
        if (!ended && !errored) {
          abort(new CanceledError_default("Request stream has been aborted", config, req));
        }
      });
      let uploadStream = data;
      if (config.maxBodyLength > -1 && config.maxRedirects === 0) {
        const limit = config.maxBodyLength;
        let bytesSent = 0;
        uploadStream = import_stream4.default.pipeline(
          [
            data,
            new import_stream4.default.Transform({
              transform(chunk, _enc, cb) {
                bytesSent += chunk.length;
                if (bytesSent > limit) {
                  return cb(
                    new AxiosError_default(
                      "Request body larger than maxBodyLength limit",
                      AxiosError_default.ERR_BAD_REQUEST,
                      config,
                      req
                    )
                  );
                }
                cb(null, chunk);
              }
            })
          ],
          utils_default.noop
        );
        uploadStream.on("error", (err) => {
          if (!req.destroyed) req.destroy(err);
        });
      }
      uploadStream.pipe(req);
    } else {
      data && req.write(data);
      req.end();
    }
  });
};

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/isURLSameOrigin.js
var isURLSameOrigin_default = platform_default.hasStandardBrowserEnv ? /* @__PURE__ */ ((origin2, isMSIE) => (url2) => {
  url2 = new URL(url2, platform_default.origin);
  return origin2.protocol === url2.protocol && origin2.host === url2.host && (isMSIE || origin2.port === url2.port);
})(
  new URL(platform_default.origin),
  platform_default.navigator && /(msie|trident)/i.test(platform_default.navigator.userAgent)
) : () => true;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/cookies.js
var cookies_default = platform_default.hasStandardBrowserEnv ? (
  // Standard browser envs support document.cookie
  {
    write(name, value, expires, path8, domain, secure, sameSite) {
      if (typeof document === "undefined") return;
      const cookie = [`${name}=${encodeURIComponent(value)}`];
      if (utils_default.isNumber(expires)) {
        cookie.push(`expires=${new Date(expires).toUTCString()}`);
      }
      if (utils_default.isString(path8)) {
        cookie.push(`path=${path8}`);
      }
      if (utils_default.isString(domain)) {
        cookie.push(`domain=${domain}`);
      }
      if (secure === true) {
        cookie.push("secure");
      }
      if (utils_default.isString(sameSite)) {
        cookie.push(`SameSite=${sameSite}`);
      }
      document.cookie = cookie.join("; ");
    },
    read(name) {
      if (typeof document === "undefined") return null;
      const cookies = document.cookie.split(";");
      for (let i = 0; i < cookies.length; i++) {
        const cookie = cookies[i].replace(/^\s+/, "");
        const eq = cookie.indexOf("=");
        if (eq !== -1 && cookie.slice(0, eq) === name) {
          return decodeURIComponent(cookie.slice(eq + 1));
        }
      }
      return null;
    },
    remove(name) {
      this.write(name, "", Date.now() - 864e5, "/");
    }
  }
) : (
  // Non-standard browser env (web workers, react-native) lack needed support.
  {
    write() {
    },
    read() {
      return null;
    },
    remove() {
    }
  }
);

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/core/mergeConfig.js
var headersToObject = (thing) => thing instanceof AxiosHeaders_default ? { ...thing } : thing;
function mergeConfig(config1, config2) {
  config2 = config2 || {};
  const config = /* @__PURE__ */ Object.create(null);
  Object.defineProperty(config, "hasOwnProperty", {
    // Null-proto descriptor so a polluted Object.prototype.get cannot turn
    // this data descriptor into an accessor descriptor on the way in.
    __proto__: null,
    value: Object.prototype.hasOwnProperty,
    enumerable: false,
    writable: true,
    configurable: true
  });
  function getMergedValue(target, source, prop, caseless) {
    if (utils_default.isPlainObject(target) && utils_default.isPlainObject(source)) {
      return utils_default.merge.call({ caseless }, target, source);
    } else if (utils_default.isPlainObject(source)) {
      return utils_default.merge({}, source);
    } else if (utils_default.isArray(source)) {
      return source.slice();
    }
    return source;
  }
  function mergeDeepProperties(a, b, prop, caseless) {
    if (!utils_default.isUndefined(b)) {
      return getMergedValue(a, b, prop, caseless);
    } else if (!utils_default.isUndefined(a)) {
      return getMergedValue(void 0, a, prop, caseless);
    }
  }
  function valueFromConfig2(a, b) {
    if (!utils_default.isUndefined(b)) {
      return getMergedValue(void 0, b);
    }
  }
  function defaultToConfig2(a, b) {
    if (!utils_default.isUndefined(b)) {
      return getMergedValue(void 0, b);
    } else if (!utils_default.isUndefined(a)) {
      return getMergedValue(void 0, a);
    }
  }
  function mergeDirectKeys(a, b, prop) {
    if (utils_default.hasOwnProp(config2, prop)) {
      return getMergedValue(a, b);
    } else if (utils_default.hasOwnProp(config1, prop)) {
      return getMergedValue(void 0, a);
    }
  }
  const mergeMap = {
    url: valueFromConfig2,
    method: valueFromConfig2,
    data: valueFromConfig2,
    baseURL: defaultToConfig2,
    transformRequest: defaultToConfig2,
    transformResponse: defaultToConfig2,
    paramsSerializer: defaultToConfig2,
    timeout: defaultToConfig2,
    timeoutMessage: defaultToConfig2,
    withCredentials: defaultToConfig2,
    withXSRFToken: defaultToConfig2,
    adapter: defaultToConfig2,
    responseType: defaultToConfig2,
    xsrfCookieName: defaultToConfig2,
    xsrfHeaderName: defaultToConfig2,
    onUploadProgress: defaultToConfig2,
    onDownloadProgress: defaultToConfig2,
    decompress: defaultToConfig2,
    maxContentLength: defaultToConfig2,
    maxBodyLength: defaultToConfig2,
    beforeRedirect: defaultToConfig2,
    transport: defaultToConfig2,
    httpAgent: defaultToConfig2,
    httpsAgent: defaultToConfig2,
    cancelToken: defaultToConfig2,
    socketPath: defaultToConfig2,
    allowedSocketPaths: defaultToConfig2,
    responseEncoding: defaultToConfig2,
    validateStatus: mergeDirectKeys,
    headers: (a, b, prop) => mergeDeepProperties(headersToObject(a), headersToObject(b), prop, true)
  };
  utils_default.forEach(Object.keys({ ...config1, ...config2 }), function computeConfigValue(prop) {
    if (prop === "__proto__" || prop === "constructor" || prop === "prototype") return;
    const merge2 = utils_default.hasOwnProp(mergeMap, prop) ? mergeMap[prop] : mergeDeepProperties;
    const a = utils_default.hasOwnProp(config1, prop) ? config1[prop] : void 0;
    const b = utils_default.hasOwnProp(config2, prop) ? config2[prop] : void 0;
    const configValue = merge2(a, b, prop);
    utils_default.isUndefined(configValue) && merge2 !== mergeDirectKeys || (config[prop] = configValue);
  });
  return config;
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/resolveConfig.js
var FORM_DATA_CONTENT_HEADERS2 = ["content-type", "content-length"];
function setFormDataHeaders2(headers, formHeaders, policy) {
  if (policy !== "content-only") {
    headers.set(formHeaders);
    return;
  }
  Object.entries(formHeaders).forEach(([key, val]) => {
    if (FORM_DATA_CONTENT_HEADERS2.includes(key.toLowerCase())) {
      headers.set(key, val);
    }
  });
}
var encodeUTF8 = (str) => encodeURIComponent(str).replace(
  /%([0-9A-F]{2})/gi,
  (_, hex) => String.fromCharCode(parseInt(hex, 16))
);
var resolveConfig_default = (config) => {
  const newConfig = mergeConfig({}, config);
  const own2 = (key) => utils_default.hasOwnProp(newConfig, key) ? newConfig[key] : void 0;
  const data = own2("data");
  let withXSRFToken = own2("withXSRFToken");
  const xsrfHeaderName = own2("xsrfHeaderName");
  const xsrfCookieName = own2("xsrfCookieName");
  let headers = own2("headers");
  const auth = own2("auth");
  const baseURL = own2("baseURL");
  const allowAbsoluteUrls = own2("allowAbsoluteUrls");
  const url2 = own2("url");
  newConfig.headers = headers = AxiosHeaders_default.from(headers);
  newConfig.url = buildURL(
    buildFullPath(baseURL, url2, allowAbsoluteUrls),
    config.params,
    config.paramsSerializer
  );
  if (auth) {
    headers.set(
      "Authorization",
      "Basic " + btoa((auth.username || "") + ":" + (auth.password ? encodeUTF8(auth.password) : ""))
    );
  }
  if (utils_default.isFormData(data)) {
    if (platform_default.hasStandardBrowserEnv || platform_default.hasStandardBrowserWebWorkerEnv) {
      headers.setContentType(void 0);
    } else if (utils_default.isFunction(data.getHeaders)) {
      setFormDataHeaders2(headers, data.getHeaders(), own2("formDataHeaderPolicy"));
    }
  }
  if (platform_default.hasStandardBrowserEnv) {
    if (utils_default.isFunction(withXSRFToken)) {
      withXSRFToken = withXSRFToken(newConfig);
    }
    const shouldSendXSRF = withXSRFToken === true || withXSRFToken == null && isURLSameOrigin_default(newConfig.url);
    if (shouldSendXSRF) {
      const xsrfValue = xsrfHeaderName && xsrfCookieName && cookies_default.read(xsrfCookieName);
      if (xsrfValue) {
        headers.set(xsrfHeaderName, xsrfValue);
      }
    }
  }
  return newConfig;
};

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/adapters/xhr.js
var isXHRAdapterSupported = typeof XMLHttpRequest !== "undefined";
var xhr_default = isXHRAdapterSupported && function(config) {
  return new Promise(function dispatchXhrRequest(resolve2, reject) {
    const _config = resolveConfig_default(config);
    let requestData = _config.data;
    const requestHeaders = AxiosHeaders_default.from(_config.headers).normalize();
    let { responseType, onUploadProgress, onDownloadProgress } = _config;
    let onCanceled;
    let uploadThrottled, downloadThrottled;
    let flushUpload, flushDownload;
    function done() {
      flushUpload && flushUpload();
      flushDownload && flushDownload();
      _config.cancelToken && _config.cancelToken.unsubscribe(onCanceled);
      _config.signal && _config.signal.removeEventListener("abort", onCanceled);
    }
    let request = new XMLHttpRequest();
    request.open(_config.method.toUpperCase(), _config.url, true);
    request.timeout = _config.timeout;
    function onloadend() {
      if (!request) {
        return;
      }
      const responseHeaders = AxiosHeaders_default.from(
        "getAllResponseHeaders" in request && request.getAllResponseHeaders()
      );
      const responseData = !responseType || responseType === "text" || responseType === "json" ? request.responseText : request.response;
      const response = {
        data: responseData,
        status: request.status,
        statusText: request.statusText,
        headers: responseHeaders,
        config,
        request
      };
      settle(
        function _resolve(value) {
          resolve2(value);
          done();
        },
        function _reject(err) {
          reject(err);
          done();
        },
        response
      );
      request = null;
    }
    if ("onloadend" in request) {
      request.onloadend = onloadend;
    } else {
      request.onreadystatechange = function handleLoad() {
        if (!request || request.readyState !== 4) {
          return;
        }
        if (request.status === 0 && !(request.responseURL && request.responseURL.startsWith("file:"))) {
          return;
        }
        setTimeout(onloadend);
      };
    }
    request.onabort = function handleAbort() {
      if (!request) {
        return;
      }
      reject(new AxiosError_default("Request aborted", AxiosError_default.ECONNABORTED, config, request));
      done();
      request = null;
    };
    request.onerror = function handleError(event) {
      const msg = event && event.message ? event.message : "Network Error";
      const err = new AxiosError_default(msg, AxiosError_default.ERR_NETWORK, config, request);
      err.event = event || null;
      reject(err);
      done();
      request = null;
    };
    request.ontimeout = function handleTimeout() {
      let timeoutErrorMessage = _config.timeout ? "timeout of " + _config.timeout + "ms exceeded" : "timeout exceeded";
      const transitional2 = _config.transitional || transitional_default;
      if (_config.timeoutErrorMessage) {
        timeoutErrorMessage = _config.timeoutErrorMessage;
      }
      reject(
        new AxiosError_default(
          timeoutErrorMessage,
          transitional2.clarifyTimeoutError ? AxiosError_default.ETIMEDOUT : AxiosError_default.ECONNABORTED,
          config,
          request
        )
      );
      done();
      request = null;
    };
    requestData === void 0 && requestHeaders.setContentType(null);
    if ("setRequestHeader" in request) {
      utils_default.forEach(toByteStringHeaderObject(requestHeaders), function setRequestHeader(val, key) {
        request.setRequestHeader(key, val);
      });
    }
    if (!utils_default.isUndefined(_config.withCredentials)) {
      request.withCredentials = !!_config.withCredentials;
    }
    if (responseType && responseType !== "json") {
      request.responseType = _config.responseType;
    }
    if (onDownloadProgress) {
      [downloadThrottled, flushDownload] = progressEventReducer(onDownloadProgress, true);
      request.addEventListener("progress", downloadThrottled);
    }
    if (onUploadProgress && request.upload) {
      [uploadThrottled, flushUpload] = progressEventReducer(onUploadProgress);
      request.upload.addEventListener("progress", uploadThrottled);
      request.upload.addEventListener("loadend", flushUpload);
    }
    if (_config.cancelToken || _config.signal) {
      onCanceled = (cancel) => {
        if (!request) {
          return;
        }
        reject(!cancel || cancel.type ? new CanceledError_default(null, config, request) : cancel);
        request.abort();
        done();
        request = null;
      };
      _config.cancelToken && _config.cancelToken.subscribe(onCanceled);
      if (_config.signal) {
        _config.signal.aborted ? onCanceled() : _config.signal.addEventListener("abort", onCanceled);
      }
    }
    const protocol = parseProtocol(_config.url);
    if (protocol && !platform_default.protocols.includes(protocol)) {
      reject(
        new AxiosError_default(
          "Unsupported protocol " + protocol + ":",
          AxiosError_default.ERR_BAD_REQUEST,
          config
        )
      );
      return;
    }
    request.send(requestData || null);
  });
};

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/composeSignals.js
var composeSignals = (signals, timeout) => {
  signals = signals ? signals.filter(Boolean) : [];
  if (!timeout && !signals.length) {
    return;
  }
  const controller = new AbortController();
  let aborted = false;
  const onabort = function(reason) {
    if (!aborted) {
      aborted = true;
      unsubscribe();
      const err = reason instanceof Error ? reason : this.reason;
      controller.abort(
        err instanceof AxiosError_default ? err : new CanceledError_default(err instanceof Error ? err.message : err)
      );
    }
  };
  let timer = timeout && setTimeout(() => {
    timer = null;
    onabort(new AxiosError_default(`timeout of ${timeout}ms exceeded`, AxiosError_default.ETIMEDOUT));
  }, timeout);
  const unsubscribe = () => {
    if (!signals) {
      return;
    }
    timer && clearTimeout(timer);
    timer = null;
    signals.forEach((signal2) => {
      signal2.unsubscribe ? signal2.unsubscribe(onabort) : signal2.removeEventListener("abort", onabort);
    });
    signals = null;
  };
  signals.forEach((signal2) => signal2.addEventListener("abort", onabort));
  const { signal } = controller;
  signal.unsubscribe = () => utils_default.asap(unsubscribe);
  return signal;
};
var composeSignals_default = composeSignals;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/trackStream.js
var streamChunk = function* (chunk, chunkSize) {
  let len = chunk.byteLength;
  if (!chunkSize || len < chunkSize) {
    yield chunk;
    return;
  }
  let pos = 0;
  let end;
  while (pos < len) {
    end = pos + chunkSize;
    yield chunk.slice(pos, end);
    pos = end;
  }
};
var readBytes = async function* (iterable, chunkSize) {
  for await (const chunk of readStream(iterable)) {
    yield* streamChunk(chunk, chunkSize);
  }
};
var readStream = async function* (stream4) {
  if (stream4[Symbol.asyncIterator]) {
    yield* stream4;
    return;
  }
  const reader = stream4.getReader();
  try {
    for (; ; ) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      yield value;
    }
  } finally {
    await reader.cancel();
  }
};
var trackStream = (stream4, chunkSize, onProgress, onFinish) => {
  const iterator2 = readBytes(stream4, chunkSize);
  let bytes = 0;
  let done;
  let _onFinish = (e) => {
    if (!done) {
      done = true;
      onFinish && onFinish(e);
    }
  };
  return new ReadableStream(
    {
      async pull(controller) {
        try {
          const { done: done2, value } = await iterator2.next();
          if (done2) {
            _onFinish();
            controller.close();
            return;
          }
          let len = value.byteLength;
          if (onProgress) {
            let loadedBytes = bytes += len;
            onProgress(loadedBytes);
          }
          controller.enqueue(new Uint8Array(value));
        } catch (err) {
          _onFinish(err);
          throw err;
        }
      },
      cancel(reason) {
        _onFinish(reason);
        return iterator2.return();
      }
    },
    {
      highWaterMark: 2
    }
  );
};

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/adapters/fetch.js
var DEFAULT_CHUNK_SIZE = 64 * 1024;
var { isFunction: isFunction2 } = utils_default;
var test = (fn, ...args) => {
  try {
    return !!fn(...args);
  } catch (e) {
    return false;
  }
};
var factory = (env2) => {
  const globalObject = utils_default.global !== void 0 && utils_default.global !== null ? utils_default.global : globalThis;
  const { ReadableStream: ReadableStream2, TextEncoder: TextEncoder2 } = globalObject;
  env2 = utils_default.merge.call(
    {
      skipUndefined: true
    },
    {
      Request: globalObject.Request,
      Response: globalObject.Response
    },
    env2
  );
  const { fetch: envFetch, Request, Response } = env2;
  const isFetchSupported = envFetch ? isFunction2(envFetch) : typeof fetch === "function";
  const isRequestSupported = isFunction2(Request);
  const isResponseSupported = isFunction2(Response);
  if (!isFetchSupported) {
    return false;
  }
  const isReadableStreamSupported = isFetchSupported && isFunction2(ReadableStream2);
  const encodeText = isFetchSupported && (typeof TextEncoder2 === "function" ? /* @__PURE__ */ ((encoder) => (str) => encoder.encode(str))(new TextEncoder2()) : async (str) => new Uint8Array(await new Request(str).arrayBuffer()));
  const supportsRequestStream = isRequestSupported && isReadableStreamSupported && test(() => {
    let duplexAccessed = false;
    const request = new Request(platform_default.origin, {
      body: new ReadableStream2(),
      method: "POST",
      get duplex() {
        duplexAccessed = true;
        return "half";
      }
    });
    const hasContentType = request.headers.has("Content-Type");
    if (request.body != null) {
      request.body.cancel();
    }
    return duplexAccessed && !hasContentType;
  });
  const supportsResponseStream = isResponseSupported && isReadableStreamSupported && test(() => utils_default.isReadableStream(new Response("").body));
  const resolvers = {
    stream: supportsResponseStream && ((res) => res.body)
  };
  isFetchSupported && (() => {
    ["text", "arrayBuffer", "blob", "formData", "stream"].forEach((type) => {
      !resolvers[type] && (resolvers[type] = (res, config) => {
        let method = res && res[type];
        if (method) {
          return method.call(res);
        }
        throw new AxiosError_default(
          `Response type '${type}' is not supported`,
          AxiosError_default.ERR_NOT_SUPPORT,
          config
        );
      });
    });
  })();
  const getBodyLength = async (body) => {
    if (body == null) {
      return 0;
    }
    if (utils_default.isBlob(body)) {
      return body.size;
    }
    if (utils_default.isSpecCompliantForm(body)) {
      const _request = new Request(platform_default.origin, {
        method: "POST",
        body
      });
      return (await _request.arrayBuffer()).byteLength;
    }
    if (utils_default.isArrayBufferView(body) || utils_default.isArrayBuffer(body)) {
      return body.byteLength;
    }
    if (utils_default.isURLSearchParams(body)) {
      body = body + "";
    }
    if (utils_default.isString(body)) {
      return (await encodeText(body)).byteLength;
    }
  };
  const resolveBodyLength = async (headers, body) => {
    const length = utils_default.toFiniteNumber(headers.getContentLength());
    return length == null ? getBodyLength(body) : length;
  };
  return async (config) => {
    let {
      url: url2,
      method,
      data,
      signal,
      cancelToken,
      timeout,
      onDownloadProgress,
      onUploadProgress,
      responseType,
      headers,
      withCredentials = "same-origin",
      fetchOptions,
      maxContentLength,
      maxBodyLength
    } = resolveConfig_default(config);
    const hasMaxContentLength = utils_default.isNumber(maxContentLength) && maxContentLength > -1;
    const hasMaxBodyLength = utils_default.isNumber(maxBodyLength) && maxBodyLength > -1;
    let _fetch = envFetch || fetch;
    responseType = responseType ? (responseType + "").toLowerCase() : "text";
    let composedSignal = composeSignals_default(
      [signal, cancelToken && cancelToken.toAbortSignal()],
      timeout
    );
    let request = null;
    const unsubscribe = composedSignal && composedSignal.unsubscribe && (() => {
      composedSignal.unsubscribe();
    });
    let requestContentLength;
    try {
      if (hasMaxContentLength && typeof url2 === "string" && url2.startsWith("data:")) {
        const estimated = estimateDataURLDecodedBytes(url2);
        if (estimated > maxContentLength) {
          throw new AxiosError_default(
            "maxContentLength size of " + maxContentLength + " exceeded",
            AxiosError_default.ERR_BAD_RESPONSE,
            config,
            request
          );
        }
      }
      if (hasMaxBodyLength && method !== "get" && method !== "head") {
        const outboundLength = await resolveBodyLength(headers, data);
        if (typeof outboundLength === "number" && isFinite(outboundLength) && outboundLength > maxBodyLength) {
          throw new AxiosError_default(
            "Request body larger than maxBodyLength limit",
            AxiosError_default.ERR_BAD_REQUEST,
            config,
            request
          );
        }
      }
      if (onUploadProgress && supportsRequestStream && method !== "get" && method !== "head" && (requestContentLength = await resolveBodyLength(headers, data)) !== 0) {
        let _request = new Request(url2, {
          method: "POST",
          body: data,
          duplex: "half"
        });
        let contentTypeHeader;
        if (utils_default.isFormData(data) && (contentTypeHeader = _request.headers.get("content-type"))) {
          headers.setContentType(contentTypeHeader);
        }
        if (_request.body) {
          const [onProgress, flush] = progressEventDecorator(
            requestContentLength,
            progressEventReducer(asyncDecorator(onUploadProgress))
          );
          data = trackStream(_request.body, DEFAULT_CHUNK_SIZE, onProgress, flush);
        }
      }
      if (!utils_default.isString(withCredentials)) {
        withCredentials = withCredentials ? "include" : "omit";
      }
      const isCredentialsSupported = isRequestSupported && "credentials" in Request.prototype;
      if (utils_default.isFormData(data)) {
        const contentType = headers.getContentType();
        if (contentType && /^multipart\/form-data/i.test(contentType) && !/boundary=/i.test(contentType)) {
          headers.delete("content-type");
        }
      }
      headers.set("User-Agent", "axios/" + VERSION, false);
      const resolvedOptions = {
        ...fetchOptions,
        signal: composedSignal,
        method: method.toUpperCase(),
        headers: toByteStringHeaderObject(headers.normalize()),
        body: data,
        duplex: "half",
        credentials: isCredentialsSupported ? withCredentials : void 0
      };
      request = isRequestSupported && new Request(url2, resolvedOptions);
      let response = await (isRequestSupported ? _fetch(request, fetchOptions) : _fetch(url2, resolvedOptions));
      if (hasMaxContentLength) {
        const declaredLength = utils_default.toFiniteNumber(response.headers.get("content-length"));
        if (declaredLength != null && declaredLength > maxContentLength) {
          throw new AxiosError_default(
            "maxContentLength size of " + maxContentLength + " exceeded",
            AxiosError_default.ERR_BAD_RESPONSE,
            config,
            request
          );
        }
      }
      const isStreamResponse = supportsResponseStream && (responseType === "stream" || responseType === "response");
      if (supportsResponseStream && response.body && (onDownloadProgress || hasMaxContentLength || isStreamResponse && unsubscribe)) {
        const options = {};
        ["status", "statusText", "headers"].forEach((prop) => {
          options[prop] = response[prop];
        });
        const responseContentLength = utils_default.toFiniteNumber(response.headers.get("content-length"));
        const [onProgress, flush] = onDownloadProgress && progressEventDecorator(
          responseContentLength,
          progressEventReducer(asyncDecorator(onDownloadProgress), true)
        ) || [];
        let bytesRead = 0;
        const onChunkProgress = (loadedBytes) => {
          if (hasMaxContentLength) {
            bytesRead = loadedBytes;
            if (bytesRead > maxContentLength) {
              throw new AxiosError_default(
                "maxContentLength size of " + maxContentLength + " exceeded",
                AxiosError_default.ERR_BAD_RESPONSE,
                config,
                request
              );
            }
          }
          onProgress && onProgress(loadedBytes);
        };
        response = new Response(
          trackStream(response.body, DEFAULT_CHUNK_SIZE, onChunkProgress, () => {
            flush && flush();
            unsubscribe && unsubscribe();
          }),
          options
        );
      }
      responseType = responseType || "text";
      let responseData = await resolvers[utils_default.findKey(resolvers, responseType) || "text"](
        response,
        config
      );
      if (hasMaxContentLength && !supportsResponseStream && !isStreamResponse) {
        let materializedSize;
        if (responseData != null) {
          if (typeof responseData.byteLength === "number") {
            materializedSize = responseData.byteLength;
          } else if (typeof responseData.size === "number") {
            materializedSize = responseData.size;
          } else if (typeof responseData === "string") {
            materializedSize = typeof TextEncoder2 === "function" ? new TextEncoder2().encode(responseData).byteLength : responseData.length;
          }
        }
        if (typeof materializedSize === "number" && materializedSize > maxContentLength) {
          throw new AxiosError_default(
            "maxContentLength size of " + maxContentLength + " exceeded",
            AxiosError_default.ERR_BAD_RESPONSE,
            config,
            request
          );
        }
      }
      !isStreamResponse && unsubscribe && unsubscribe();
      return await new Promise((resolve2, reject) => {
        settle(resolve2, reject, {
          data: responseData,
          headers: AxiosHeaders_default.from(response.headers),
          status: response.status,
          statusText: response.statusText,
          config,
          request
        });
      });
    } catch (err) {
      unsubscribe && unsubscribe();
      if (composedSignal && composedSignal.aborted && composedSignal.reason instanceof AxiosError_default) {
        const canceledError = composedSignal.reason;
        canceledError.config = config;
        request && (canceledError.request = request);
        err !== canceledError && (canceledError.cause = err);
        throw canceledError;
      }
      if (err && err.name === "TypeError" && /Load failed|fetch/i.test(err.message)) {
        throw Object.assign(
          new AxiosError_default(
            "Network Error",
            AxiosError_default.ERR_NETWORK,
            config,
            request,
            err && err.response
          ),
          {
            cause: err.cause || err
          }
        );
      }
      throw AxiosError_default.from(err, err && err.code, config, request, err && err.response);
    }
  };
};
var seedCache = /* @__PURE__ */ new Map();
var getFetch = (config) => {
  let env2 = config && config.env || {};
  const { fetch: fetch2, Request, Response } = env2;
  const seeds = [Request, Response, fetch2];
  let len = seeds.length, i = len, seed, target, map = seedCache;
  while (i--) {
    seed = seeds[i];
    target = map.get(seed);
    target === void 0 && map.set(seed, target = i ? /* @__PURE__ */ new Map() : factory(env2));
    map = target;
  }
  return target;
};
var adapter = getFetch();

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/adapters/adapters.js
var knownAdapters = {
  http: http_default,
  xhr: xhr_default,
  fetch: {
    get: getFetch
  }
};
utils_default.forEach(knownAdapters, (fn, value) => {
  if (fn) {
    try {
      Object.defineProperty(fn, "name", { __proto__: null, value });
    } catch (e) {
    }
    Object.defineProperty(fn, "adapterName", { __proto__: null, value });
  }
});
var renderReason = (reason) => `- ${reason}`;
var isResolvedHandle = (adapter2) => utils_default.isFunction(adapter2) || adapter2 === null || adapter2 === false;
function getAdapter(adapters, config) {
  adapters = utils_default.isArray(adapters) ? adapters : [adapters];
  const { length } = adapters;
  let nameOrAdapter;
  let adapter2;
  const rejectedReasons = {};
  for (let i = 0; i < length; i++) {
    nameOrAdapter = adapters[i];
    let id;
    adapter2 = nameOrAdapter;
    if (!isResolvedHandle(nameOrAdapter)) {
      adapter2 = knownAdapters[(id = String(nameOrAdapter)).toLowerCase()];
      if (adapter2 === void 0) {
        throw new AxiosError_default(`Unknown adapter '${id}'`);
      }
    }
    if (adapter2 && (utils_default.isFunction(adapter2) || (adapter2 = adapter2.get(config)))) {
      break;
    }
    rejectedReasons[id || "#" + i] = adapter2;
  }
  if (!adapter2) {
    const reasons = Object.entries(rejectedReasons).map(
      ([id, state]) => `adapter ${id} ` + (state === false ? "is not supported by the environment" : "is not available in the build")
    );
    let s = length ? reasons.length > 1 ? "since :\n" + reasons.map(renderReason).join("\n") : " " + renderReason(reasons[0]) : "as no adapter specified";
    throw new AxiosError_default(
      `There is no suitable adapter to dispatch the request ` + s,
      "ERR_NOT_SUPPORT"
    );
  }
  return adapter2;
}
var adapters_default = {
  /**
   * Resolve an adapter from a list of adapter names or functions.
   * @type {Function}
   */
  getAdapter,
  /**
   * Exposes all known adapters
   * @type {Object<string, Function|Object>}
   */
  adapters: knownAdapters
};

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/core/dispatchRequest.js
function throwIfCancellationRequested(config) {
  if (config.cancelToken) {
    config.cancelToken.throwIfRequested();
  }
  if (config.signal && config.signal.aborted) {
    throw new CanceledError_default(null, config);
  }
}
function dispatchRequest(config) {
  throwIfCancellationRequested(config);
  config.headers = AxiosHeaders_default.from(config.headers);
  config.data = transformData.call(config, config.transformRequest);
  if (["post", "put", "patch"].indexOf(config.method) !== -1) {
    config.headers.setContentType("application/x-www-form-urlencoded", false);
  }
  const adapter2 = adapters_default.getAdapter(config.adapter || defaults_default.adapter, config);
  return adapter2(config).then(
    function onAdapterResolution(response) {
      throwIfCancellationRequested(config);
      config.response = response;
      try {
        response.data = transformData.call(config, config.transformResponse, response);
      } finally {
        delete config.response;
      }
      response.headers = AxiosHeaders_default.from(response.headers);
      return response;
    },
    function onAdapterRejection(reason) {
      if (!isCancel(reason)) {
        throwIfCancellationRequested(config);
        if (reason && reason.response) {
          config.response = reason.response;
          try {
            reason.response.data = transformData.call(
              config,
              config.transformResponse,
              reason.response
            );
          } finally {
            delete config.response;
          }
          reason.response.headers = AxiosHeaders_default.from(reason.response.headers);
        }
      }
      return Promise.reject(reason);
    }
  );
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/validator.js
var validators = {};
["object", "boolean", "number", "function", "string", "symbol"].forEach((type, i) => {
  validators[type] = function validator(thing) {
    return typeof thing === type || "a" + (i < 1 ? "n " : " ") + type;
  };
});
var deprecatedWarnings = {};
validators.transitional = function transitional(validator, version, message) {
  function formatMessage(opt, desc) {
    return "[Axios v" + VERSION + "] Transitional option '" + opt + "'" + desc + (message ? ". " + message : "");
  }
  return (value, opt, opts) => {
    if (validator === false) {
      throw new AxiosError_default(
        formatMessage(opt, " has been removed" + (version ? " in " + version : "")),
        AxiosError_default.ERR_DEPRECATED
      );
    }
    if (version && !deprecatedWarnings[opt]) {
      deprecatedWarnings[opt] = true;
      console.warn(
        formatMessage(
          opt,
          " has been deprecated since v" + version + " and will be removed in the near future"
        )
      );
    }
    return validator ? validator(value, opt, opts) : true;
  };
};
validators.spelling = function spelling(correctSpelling) {
  return (value, opt) => {
    console.warn(`${opt} is likely a misspelling of ${correctSpelling}`);
    return true;
  };
};
function assertOptions(options, schema, allowUnknown) {
  if (typeof options !== "object") {
    throw new AxiosError_default("options must be an object", AxiosError_default.ERR_BAD_OPTION_VALUE);
  }
  const keys = Object.keys(options);
  let i = keys.length;
  while (i-- > 0) {
    const opt = keys[i];
    const validator = Object.prototype.hasOwnProperty.call(schema, opt) ? schema[opt] : void 0;
    if (validator) {
      const value = options[opt];
      const result = value === void 0 || validator(value, opt, options);
      if (result !== true) {
        throw new AxiosError_default(
          "option " + opt + " must be " + result,
          AxiosError_default.ERR_BAD_OPTION_VALUE
        );
      }
      continue;
    }
    if (allowUnknown !== true) {
      throw new AxiosError_default("Unknown option " + opt, AxiosError_default.ERR_BAD_OPTION);
    }
  }
}
var validator_default = {
  assertOptions,
  validators
};

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/core/Axios.js
var validators2 = validator_default.validators;
var Axios = class {
  constructor(instanceConfig) {
    this.defaults = instanceConfig || {};
    this.interceptors = {
      request: new InterceptorManager_default(),
      response: new InterceptorManager_default()
    };
  }
  /**
   * Dispatch a request
   *
   * @param {String|Object} configOrUrl The config specific for this request (merged with this.defaults)
   * @param {?Object} config
   *
   * @returns {Promise} The Promise to be fulfilled
   */
  async request(configOrUrl, config) {
    try {
      return await this._request(configOrUrl, config);
    } catch (err) {
      if (err instanceof Error) {
        let dummy = {};
        Error.captureStackTrace ? Error.captureStackTrace(dummy) : dummy = new Error();
        const stack = (() => {
          if (!dummy.stack) {
            return "";
          }
          const firstNewlineIndex = dummy.stack.indexOf("\n");
          return firstNewlineIndex === -1 ? "" : dummy.stack.slice(firstNewlineIndex + 1);
        })();
        try {
          if (!err.stack) {
            err.stack = stack;
          } else if (stack) {
            const firstNewlineIndex = stack.indexOf("\n");
            const secondNewlineIndex = firstNewlineIndex === -1 ? -1 : stack.indexOf("\n", firstNewlineIndex + 1);
            const stackWithoutTwoTopLines = secondNewlineIndex === -1 ? "" : stack.slice(secondNewlineIndex + 1);
            if (!String(err.stack).endsWith(stackWithoutTwoTopLines)) {
              err.stack += "\n" + stack;
            }
          }
        } catch (e) {
        }
      }
      throw err;
    }
  }
  _request(configOrUrl, config) {
    if (typeof configOrUrl === "string") {
      config = config || {};
      config.url = configOrUrl;
    } else {
      config = configOrUrl || {};
    }
    config = mergeConfig(this.defaults, config);
    const { transitional: transitional2, paramsSerializer, headers } = config;
    if (transitional2 !== void 0) {
      validator_default.assertOptions(
        transitional2,
        {
          silentJSONParsing: validators2.transitional(validators2.boolean),
          forcedJSONParsing: validators2.transitional(validators2.boolean),
          clarifyTimeoutError: validators2.transitional(validators2.boolean),
          legacyInterceptorReqResOrdering: validators2.transitional(validators2.boolean)
        },
        false
      );
    }
    if (paramsSerializer != null) {
      if (utils_default.isFunction(paramsSerializer)) {
        config.paramsSerializer = {
          serialize: paramsSerializer
        };
      } else {
        validator_default.assertOptions(
          paramsSerializer,
          {
            encode: validators2.function,
            serialize: validators2.function
          },
          true
        );
      }
    }
    if (config.allowAbsoluteUrls !== void 0) {
    } else if (this.defaults.allowAbsoluteUrls !== void 0) {
      config.allowAbsoluteUrls = this.defaults.allowAbsoluteUrls;
    } else {
      config.allowAbsoluteUrls = true;
    }
    validator_default.assertOptions(
      config,
      {
        baseUrl: validators2.spelling("baseURL"),
        withXsrfToken: validators2.spelling("withXSRFToken")
      },
      true
    );
    config.method = (config.method || this.defaults.method || "get").toLowerCase();
    let contextHeaders = headers && utils_default.merge(headers.common, headers[config.method]);
    headers && utils_default.forEach(["delete", "get", "head", "post", "put", "patch", "query", "common"], (method) => {
      delete headers[method];
    });
    config.headers = AxiosHeaders_default.concat(contextHeaders, headers);
    const requestInterceptorChain = [];
    let synchronousRequestInterceptors = true;
    this.interceptors.request.forEach(function unshiftRequestInterceptors(interceptor) {
      if (typeof interceptor.runWhen === "function" && interceptor.runWhen(config) === false) {
        return;
      }
      synchronousRequestInterceptors = synchronousRequestInterceptors && interceptor.synchronous;
      const transitional3 = config.transitional || transitional_default;
      const legacyInterceptorReqResOrdering = transitional3 && transitional3.legacyInterceptorReqResOrdering;
      if (legacyInterceptorReqResOrdering) {
        requestInterceptorChain.unshift(interceptor.fulfilled, interceptor.rejected);
      } else {
        requestInterceptorChain.push(interceptor.fulfilled, interceptor.rejected);
      }
    });
    const responseInterceptorChain = [];
    this.interceptors.response.forEach(function pushResponseInterceptors(interceptor) {
      responseInterceptorChain.push(interceptor.fulfilled, interceptor.rejected);
    });
    let promise;
    let i = 0;
    let len;
    if (!synchronousRequestInterceptors) {
      const chain = [dispatchRequest.bind(this), void 0];
      chain.unshift(...requestInterceptorChain);
      chain.push(...responseInterceptorChain);
      len = chain.length;
      promise = Promise.resolve(config);
      while (i < len) {
        promise = promise.then(chain[i++], chain[i++]);
      }
      return promise;
    }
    len = requestInterceptorChain.length;
    let newConfig = config;
    while (i < len) {
      const onFulfilled = requestInterceptorChain[i++];
      const onRejected = requestInterceptorChain[i++];
      try {
        newConfig = onFulfilled(newConfig);
      } catch (error) {
        onRejected.call(this, error);
        break;
      }
    }
    try {
      promise = dispatchRequest.call(this, newConfig);
    } catch (error) {
      return Promise.reject(error);
    }
    i = 0;
    len = responseInterceptorChain.length;
    while (i < len) {
      promise = promise.then(responseInterceptorChain[i++], responseInterceptorChain[i++]);
    }
    return promise;
  }
  getUri(config) {
    config = mergeConfig(this.defaults, config);
    const fullPath = buildFullPath(config.baseURL, config.url, config.allowAbsoluteUrls);
    return buildURL(fullPath, config.params, config.paramsSerializer);
  }
};
utils_default.forEach(["delete", "get", "head", "options"], function forEachMethodNoData(method) {
  Axios.prototype[method] = function(url2, config) {
    return this.request(
      mergeConfig(config || {}, {
        method,
        url: url2,
        data: (config || {}).data
      })
    );
  };
});
utils_default.forEach(["post", "put", "patch", "query"], function forEachMethodWithData(method) {
  function generateHTTPMethod(isForm) {
    return function httpMethod(url2, data, config) {
      return this.request(
        mergeConfig(config || {}, {
          method,
          headers: isForm ? {
            "Content-Type": "multipart/form-data"
          } : {},
          url: url2,
          data
        })
      );
    };
  }
  Axios.prototype[method] = generateHTTPMethod();
  if (method !== "query") {
    Axios.prototype[method + "Form"] = generateHTTPMethod(true);
  }
});
var Axios_default = Axios;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/cancel/CancelToken.js
var CancelToken = class _CancelToken {
  constructor(executor) {
    if (typeof executor !== "function") {
      throw new TypeError("executor must be a function.");
    }
    let resolvePromise;
    this.promise = new Promise(function promiseExecutor(resolve2) {
      resolvePromise = resolve2;
    });
    const token = this;
    this.promise.then((cancel) => {
      if (!token._listeners) return;
      let i = token._listeners.length;
      while (i-- > 0) {
        token._listeners[i](cancel);
      }
      token._listeners = null;
    });
    this.promise.then = (onfulfilled) => {
      let _resolve;
      const promise = new Promise((resolve2) => {
        token.subscribe(resolve2);
        _resolve = resolve2;
      }).then(onfulfilled);
      promise.cancel = function reject() {
        token.unsubscribe(_resolve);
      };
      return promise;
    };
    executor(function cancel(message, config, request) {
      if (token.reason) {
        return;
      }
      token.reason = new CanceledError_default(message, config, request);
      resolvePromise(token.reason);
    });
  }
  /**
   * Throws a `CanceledError` if cancellation has been requested.
   */
  throwIfRequested() {
    if (this.reason) {
      throw this.reason;
    }
  }
  /**
   * Subscribe to the cancel signal
   */
  subscribe(listener) {
    if (this.reason) {
      listener(this.reason);
      return;
    }
    if (this._listeners) {
      this._listeners.push(listener);
    } else {
      this._listeners = [listener];
    }
  }
  /**
   * Unsubscribe from the cancel signal
   */
  unsubscribe(listener) {
    if (!this._listeners) {
      return;
    }
    const index = this._listeners.indexOf(listener);
    if (index !== -1) {
      this._listeners.splice(index, 1);
    }
  }
  toAbortSignal() {
    const controller = new AbortController();
    const abort = (err) => {
      controller.abort(err);
    };
    this.subscribe(abort);
    controller.signal.unsubscribe = () => this.unsubscribe(abort);
    return controller.signal;
  }
  /**
   * Returns an object that contains a new `CancelToken` and a function that, when called,
   * cancels the `CancelToken`.
   */
  static source() {
    let cancel;
    const token = new _CancelToken(function executor(c) {
      cancel = c;
    });
    return {
      token,
      cancel
    };
  }
};
var CancelToken_default = CancelToken;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/spread.js
function spread(callback) {
  return function wrap(arr) {
    return callback.apply(null, arr);
  };
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/isAxiosError.js
function isAxiosError(payload) {
  return utils_default.isObject(payload) && payload.isAxiosError === true;
}

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/helpers/HttpStatusCode.js
var HttpStatusCode = {
  Continue: 100,
  SwitchingProtocols: 101,
  Processing: 102,
  EarlyHints: 103,
  Ok: 200,
  Created: 201,
  Accepted: 202,
  NonAuthoritativeInformation: 203,
  NoContent: 204,
  ResetContent: 205,
  PartialContent: 206,
  MultiStatus: 207,
  AlreadyReported: 208,
  ImUsed: 226,
  MultipleChoices: 300,
  MovedPermanently: 301,
  Found: 302,
  SeeOther: 303,
  NotModified: 304,
  UseProxy: 305,
  Unused: 306,
  TemporaryRedirect: 307,
  PermanentRedirect: 308,
  BadRequest: 400,
  Unauthorized: 401,
  PaymentRequired: 402,
  Forbidden: 403,
  NotFound: 404,
  MethodNotAllowed: 405,
  NotAcceptable: 406,
  ProxyAuthenticationRequired: 407,
  RequestTimeout: 408,
  Conflict: 409,
  Gone: 410,
  LengthRequired: 411,
  PreconditionFailed: 412,
  PayloadTooLarge: 413,
  UriTooLong: 414,
  UnsupportedMediaType: 415,
  RangeNotSatisfiable: 416,
  ExpectationFailed: 417,
  ImATeapot: 418,
  MisdirectedRequest: 421,
  UnprocessableEntity: 422,
  Locked: 423,
  FailedDependency: 424,
  TooEarly: 425,
  UpgradeRequired: 426,
  PreconditionRequired: 428,
  TooManyRequests: 429,
  RequestHeaderFieldsTooLarge: 431,
  UnavailableForLegalReasons: 451,
  InternalServerError: 500,
  NotImplemented: 501,
  BadGateway: 502,
  ServiceUnavailable: 503,
  GatewayTimeout: 504,
  HttpVersionNotSupported: 505,
  VariantAlsoNegotiates: 506,
  InsufficientStorage: 507,
  LoopDetected: 508,
  NotExtended: 510,
  NetworkAuthenticationRequired: 511,
  WebServerIsDown: 521,
  ConnectionTimedOut: 522,
  OriginIsUnreachable: 523,
  TimeoutOccurred: 524,
  SslHandshakeFailed: 525,
  InvalidSslCertificate: 526
};
Object.entries(HttpStatusCode).forEach(([key, value]) => {
  HttpStatusCode[value] = key;
});
var HttpStatusCode_default = HttpStatusCode;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/lib/axios.js
function createInstance(defaultConfig) {
  const context = new Axios_default(defaultConfig);
  const instance = bind(Axios_default.prototype.request, context);
  utils_default.extend(instance, Axios_default.prototype, context, { allOwnKeys: true });
  utils_default.extend(instance, context, null, { allOwnKeys: true });
  instance.create = function create2(instanceConfig) {
    return createInstance(mergeConfig(defaultConfig, instanceConfig));
  };
  return instance;
}
var axios = createInstance(defaults_default);
axios.Axios = Axios_default;
axios.CanceledError = CanceledError_default;
axios.CancelToken = CancelToken_default;
axios.isCancel = isCancel;
axios.VERSION = VERSION;
axios.toFormData = toFormData_default;
axios.AxiosError = AxiosError_default;
axios.Cancel = axios.CanceledError;
axios.all = function all(promises) {
  return Promise.all(promises);
};
axios.spread = spread;
axios.isAxiosError = isAxiosError;
axios.mergeConfig = mergeConfig;
axios.AxiosHeaders = AxiosHeaders_default;
axios.formToJSON = (thing) => formDataToJSON_default(utils_default.isHTMLForm(thing) ? new FormData(thing) : thing);
axios.getAdapter = adapters_default.getAdapter;
axios.HttpStatusCode = HttpStatusCode_default;
axios.default = axios;
var axios_default = axios;

// ../../node_modules/.pnpm/axios@1.16.1_debug@4.4.3_supports-color@10.2.2__supports-color@10.2.2/node_modules/axios/index.js
var {
  Axios: Axios2,
  AxiosError: AxiosError2,
  CanceledError: CanceledError2,
  isCancel: isCancel2,
  CancelToken: CancelToken2,
  VERSION: VERSION2,
  all: all2,
  Cancel,
  isAxiosError: isAxiosError2,
  spread: spread2,
  toFormData: toFormData2,
  AxiosHeaders: AxiosHeaders2,
  HttpStatusCode: HttpStatusCode2,
  formToJSON,
  getAdapter: getAdapter2,
  mergeConfig: mergeConfig2,
  create
} = axios_default;

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/media/miora-proxy-client.ts
var MIORA_HTTP_TIMEOUT_MS = 3e4;
var MIORA_SUBMIT_HTTP_TIMEOUT_MS = 39e4;
function log(event, fields = {}) {
  const parts = Object.entries(fields).filter(([, v]) => v !== void 0).map(([k, v]) => `${k}=${typeof v === "string" ? v : JSON.stringify(v)}`).join(" ");
  console.error(`[miora-mcp] ${event}${parts ? " " + parts : ""}`);
}
var PASSTHROUGH_UPSTREAM_CODES = /* @__PURE__ */ new Set([
  "FEATURE_DISABLED",
  "TOOL_DISABLED",
  "AUTH_SCOPE_DENIED",
  "MODEL_NOT_ALLOWED",
  "MISSING_INPUT",
  "UPSTREAM_SUBMIT_FAILED",
  "COPYRIGHT_VIOLATION",
  "CONTENT_SAFETY"
]);
var AUTH_UPSTREAM_CODES = /* @__PURE__ */ new Set([
  "AUTH_INVALID_TEMP_KEY",
  "AUTH_MISSING_TEMP_KEY",
  "AUTH_EXPIRED"
]);
var MioraToolError = class extends Error {
  constructor(code, message) {
    super(message);
    this.code = code;
    this.name = "MioraToolError";
  }
};
async function mioraPost(deps, path8, body, tempKey, extraHeaders, timeoutMs) {
  const url2 = `${trimTrailingSlash(deps.mioraEndpoint)}${path8}`;
  let response;
  try {
    response = await axios_default.post(url2, body, {
      headers: {
        Authorization: `Bearer ${tempKey}`,
        "Content-Type": "application/json",
        // 先 defaultHeaders 再 extraHeaders：调用点显式传的 header 覆盖 client 级默认值。
        // Authorization / Content-Type 在最前面，被后面同名 key 覆盖是刻意的（未来若需
        // 让特定 tool 换成非 Bearer 授权，仍可通过 extraHeaders 覆盖）。
        ...deps.defaultHeaders ?? {},
        ...extraHeaders ?? {}
      },
      timeout: timeoutMs ?? MIORA_HTTP_TIMEOUT_MS
    });
  } catch (err) {
    throw mapAxiosError(err, path8);
  }
  const bodyError = detectBodyError(response.data);
  if (bodyError) {
    throw mapUpstreamError(bodyError, path8);
  }
  return unwrapEnvelope(response.data);
}
function unwrapEnvelope(body) {
  if (body !== null && typeof body === "object" && "code" in body && "failed" in body && "data" in body) {
    return body.data;
  }
  return body;
}
async function pollUntilTerminal(deps, queryPath, tempKey, upstreamTaskId, opts = {}) {
  const intervalMs = opts.intervalMs ?? 3e3;
  const intervalMaxMs = Math.max(opts.intervalMaxMs ?? intervalMs, intervalMs);
  const timeoutMs = opts.timeoutMs ?? 6e4;
  const sleep = opts.sleep ?? defaultSleep;
  const start = Date.now();
  const deadline = start + timeoutMs;
  let attempt = 0;
  for (; ; ) {
    attempt += 1;
    const nextIntervalMs = Math.min(
      Math.round(intervalMs * Math.pow(1.5, attempt - 1)),
      intervalMaxMs
    );
    let result;
    try {
      result = await mioraPost(
        deps,
        queryPath,
        { upstreamTaskIds: [upstreamTaskId] },
        tempKey
      );
    } catch (err) {
      if (err instanceof MioraToolError) {
        err.upstreamTaskId = upstreamTaskId;
        log("poll_error", {
          taskId: upstreamTaskId,
          attempt,
          elapsedMs: Date.now() - start,
          code: err.code
        });
      }
      throw err;
    }
    const entry = result[upstreamTaskId];
    if (!entry) {
      const missing = new MioraToolError(
        "UPSTREAM_ERROR",
        `Miora ${queryPath} returned no entry for taskId=${upstreamTaskId}.`
      );
      missing.upstreamTaskId = upstreamTaskId;
      log("poll_missing_entry", { taskId: upstreamTaskId, attempt, elapsedMs: Date.now() - start });
      throw missing;
    }
    log("poll_tick", {
      taskId: upstreamTaskId,
      attempt,
      status: entry.status,
      elapsedMs: Date.now() - start,
      nextIntervalMs: isTerminal(entry.status) ? 0 : nextIntervalMs
    });
    if (isTerminal(entry.status)) return entry;
    opts.onProgress?.(
      attempt,
      void 0,
      `${entry.status} \xB7 \u5DF2\u7B49\u5F85 ${Math.round((Date.now() - start) / 1e3)}s`
    );
    if (Date.now() + nextIntervalMs >= deadline) {
      const timeout = new MioraToolError(
        "POLL_TIMEOUT",
        `Timed out after ${timeoutMs}ms waiting for taskId=${upstreamTaskId} (last status=${entry.status}).`
      );
      timeout.upstreamTaskId = upstreamTaskId;
      log("poll_timeout", {
        taskId: upstreamTaskId,
        attempt,
        elapsedMs: Date.now() - start,
        lastStatus: entry.status
      });
      throw timeout;
    }
    await sleep(nextIntervalMs);
  }
}
async function runMioraTask(deps, tempKey, args) {
  if (!tempKey) {
    throw new MioraToolError(
      "AUTH_MISSING_TEMP_KEY",
      "Missing tempKey. Call `connect_cloud_service` first and pass the returned `clientTempToken` as `tempKey`."
    );
  }
  const submitStart = Date.now();
  log("submit_start", { path: args.submitPath });
  const submit = await mioraPost(
    deps,
    args.submitPath,
    args.body,
    tempKey,
    args.extraHeaders,
    MIORA_SUBMIT_HTTP_TIMEOUT_MS
  );
  if (!submit?.upstreamTaskId) {
    log("submit_no_task_id", { path: args.submitPath, elapsedMs: Date.now() - submitStart });
    throw new MioraToolError(
      "UPSTREAM_ERROR",
      `Miora ${args.submitPath} did not return an upstreamTaskId.`
    );
  }
  log("submit_ok", {
    taskId: submit.upstreamTaskId,
    model: submit.model,
    provider: submit.provider,
    mode: submit.mode ?? "async",
    elapsedMs: Date.now() - submitStart
  });
  if (submit.mode === "sync") {
    const entry2 = {
      status: submit.status ?? "failed",
      resultFiles: submit.resultFiles,
      errorMessage: submit.errorMessage ?? void 0
    };
    log("sync_done", {
      taskId: submit.upstreamTaskId,
      status: entry2.status,
      totalElapsedMs: Date.now() - submitStart,
      hasResultFiles: (entry2.resultFiles?.length ?? 0) > 0
    });
    return { submit, entry: entry2 };
  }
  const entry = await pollUntilTerminal(deps, args.queryPath, tempKey, submit.upstreamTaskId, {
    intervalMs: args.pollIntervalMs,
    intervalMaxMs: args.pollIntervalMaxMs,
    timeoutMs: args.pollTimeoutMs,
    onProgress: args.onProgress
  });
  log("task_done", {
    taskId: submit.upstreamTaskId,
    status: entry.status,
    totalElapsedMs: Date.now() - submitStart,
    hasResultFiles: (entry.resultFiles?.length ?? 0) > 0
  });
  return { submit, entry };
}
function isTerminal(status) {
  return status === "completed" || status === "failed" || status === "cancelled";
}
function trimTrailingSlash(url2) {
  return url2.endsWith("/") ? url2.slice(0, -1) : url2;
}
function defaultSleep(ms) {
  return new Promise((resolve2) => setTimeout(resolve2, ms));
}
function mapAxiosError(err, path8) {
  if (!axios_default.isAxiosError(err)) {
    const message = err instanceof Error ? err.message : String(err);
    return new MioraToolError("MIORA_UNAVAILABLE", `Miora ${path8} unreachable: ${message}`);
  }
  if (!err.response) {
    return new MioraToolError(
      "MIORA_UNAVAILABLE",
      `Miora ${path8} unreachable: ${err.code ?? err.message}. Try again shortly.`
    );
  }
  return mapUpstreamError(
    {
      httpStatus: err.response.status,
      code: extractUpstreamCode(err.response.data),
      message: extractUpstreamMessage(err.response.data) ?? err.message
    },
    path8
  );
}
function detectBodyError(body) {
  if (!body || typeof body !== "object") return void 0;
  const b = body;
  if (b.failed !== true) return void 0;
  return {
    httpStatus: typeof b.httpStatus === "number" ? b.httpStatus : void 0,
    code: extractUpstreamCode(b),
    message: extractUpstreamMessage(b)
  };
}
function mapUpstreamError(err, path8) {
  const { httpStatus, code, message } = err;
  if (code && PASSTHROUGH_UPSTREAM_CODES.has(code)) {
    return new MioraToolError(
      code,
      message ?? `Miora ${path8} rejected the request (${code}).`
    );
  }
  if (code && AUTH_UPSTREAM_CODES.has(code)) {
    return new MioraToolError(
      "AUTH_INVALID_TEMP_KEY",
      "Miora rejected the temp key. Re-call `connect_cloud_service` to obtain a fresh `clientTempToken`, then retry."
    );
  }
  if (httpStatus === 401) {
    return new MioraToolError(
      "AUTH_INVALID_TEMP_KEY",
      "Miora rejected the temp key. Re-call `connect_cloud_service` to obtain a fresh `clientTempToken`, then retry."
    );
  }
  if (httpStatus === 403) {
    return new MioraToolError(
      "AUTH_SCOPE_DENIED",
      message ?? "Miora denied the request: current account lacks the required scope."
    );
  }
  if (httpStatus === 429) {
    return new MioraToolError(
      "RATE_LIMITED",
      "Miora rate limit hit. Please retry after a short delay."
    );
  }
  if (httpStatus === 400 && !code) {
    return new MioraToolError(
      "INVALID_INPUT",
      message ?? `Miora ${path8} rejected the request as invalid.`
    );
  }
  if (httpStatus && httpStatus >= 500) {
    return new MioraToolError(
      "MIORA_UNAVAILABLE",
      `Miora ${path8} returned ${httpStatus}. Try again shortly.`
    );
  }
  return new MioraToolError(
    "UPSTREAM_ERROR",
    `Miora ${path8} failed: ${message ?? "(no message)"}${code ? ` (${code})` : ""}.`
  );
}
function extractUpstreamCode(body) {
  if (!body || typeof body !== "object") return void 0;
  const code = body.code;
  if (typeof code === "string") return code;
  if (typeof code === "number") return String(code);
  return void 0;
}
function extractUpstreamMessage(body) {
  if (!body || typeof body !== "object") return void 0;
  const message = body.message;
  if (typeof message === "string" && message.length > 0) return message;
  if (Array.isArray(message)) {
    const joined = message.filter((item) => typeof item === "string" && item.length > 0).join("; ");
    if (joined.length > 0) return joined;
  }
  return void 0;
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/media/image-input.ts
var FILE_KEY_PATTERN = /^(?:bga|file)\/[A-Za-z0-9_.\-\/]+$/;
function isSafeFileKey(key) {
  if (!FILE_KEY_PATTERN.test(key)) return false;
  const segments = key.split("/");
  if (segments.length < 2) return false;
  for (let i = 1; i < segments.length; i += 1) {
    const seg = segments[i];
    if (seg === "" || seg === "." || seg === "..") return false;
  }
  return true;
}
var EXT_TO_MIME = {
  ".png": "image/png",
  ".jpg": "image/jpeg",
  ".jpeg": "image/jpeg",
  ".gif": "image/gif",
  ".webp": "image/webp",
  ".bmp": "image/bmp",
  ".svg": "image/svg+xml"
};
function defaultReadFile() {
  return (p) => import_node_fs2.promises.readFile(p);
}
async function resolveImageInput(raw, options = {}) {
  const field = options.fieldName ?? "image input";
  const trimmed = typeof raw === "string" ? raw.trim() : "";
  if (!trimmed) {
    throw new MioraToolError("INVALID_INPUT", `${field} is empty.`);
  }
  if (trimmed.startsWith("data:")) {
    assertAllowed("dataUri", options.allow, field);
    return parseDataUri(trimmed, options.maxBytes, field);
  }
  if (/^https?:\/\//i.test(trimmed)) {
    assertAllowed("url", options.allow, field);
    try {
      await assertSafeExternalUrl(trimmed);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new MioraToolError(
        "INVALID_INPUT",
        `${field} must be a public https URL (no private/reserved hosts). Rejected "${trimmed}": ${message}`
      );
    }
    return { origin: "url", url: trimmed };
  }
  if (FILE_KEY_PATTERN.test(trimmed)) {
    if (!isSafeFileKey(trimmed)) {
      throw new MioraToolError(
        "INVALID_INPUT",
        `${field} looks like a miora fileKey but contains path-traversal segments ("..", ".", or empty). Got "${trimmed}".`
      );
    }
    assertAllowed("fileKey", options.allow, field);
    return { origin: "fileKey", fileKey: trimmed };
  }
  assertAllowed("path", options.allow, field);
  if (!path2.isAbsolute(trimmed)) {
    throw new MioraToolError(
      "INVALID_INPUT",
      `${field} must be one of: absolute local path, miora fileKey (bga/... or file/...), public https URL, or data URI. Got "${trimmed}".`
    );
  }
  return await readLocalFile(trimmed, options.readFile ?? defaultReadFile(), options.maxBytes, field);
}
async function resolveImageInputs(raws, options = {}) {
  const results = [];
  for (let i = 0; i < raws.length; i += 1) {
    const perItemOptions = {
      ...options,
      fieldName: options.fieldName ? `${options.fieldName}[${i}]` : `input[${i}]`
    };
    results.push(await resolveImageInput(raws[i], perItemOptions));
  }
  return results;
}
function assertAllowed(origin2, allow, field) {
  if (allow && !allow.includes(origin2)) {
    throw new MioraToolError(
      "INVALID_INPUT",
      `${field} origin "${origin2}" is not accepted here (accepted: ${allow.join(", ")}).`
    );
  }
}
function parseDataUri(raw, maxBytes, field) {
  const match = /^data:([^;,]+)?(;base64)?,(.*)$/s.exec(raw);
  if (!match) {
    throw new MioraToolError("INVALID_INPUT", `${field} is not a valid data URI.`);
  }
  const mimeType = match[1]?.trim() || void 0;
  const isBase64 = !!match[2];
  const payload = match[3] ?? "";
  if (!isBase64) {
    throw new MioraToolError(
      "INVALID_INPUT",
      `${field} data URI must use ;base64 encoding.`
    );
  }
  const buffer2 = Buffer.from(payload, "base64");
  if (buffer2.length === 0) {
    throw new MioraToolError("INVALID_INPUT", `${field} data URI has empty payload.`);
  }
  if (maxBytes && buffer2.length > maxBytes) {
    throw new MioraToolError(
      "INVALID_INPUT",
      `${field} exceeds ${maxBytes}B (${buffer2.length}B).`
    );
  }
  return {
    origin: "dataUri",
    base64: payload,
    mimeType,
    sizeBytes: buffer2.length
  };
}
async function readLocalFile(filePath, readFile2, maxBytes, field) {
  let buffer2;
  try {
    buffer2 = await readFile2(filePath);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    throw new MioraToolError("INVALID_INPUT", `${field} could not be read: ${filePath} (${message})`);
  }
  if (buffer2.length === 0) {
    throw new MioraToolError("INVALID_INPUT", `${field} is empty: ${filePath}`);
  }
  if (maxBytes && buffer2.length > maxBytes) {
    throw new MioraToolError(
      "INVALID_INPUT",
      `${field} exceeds ${maxBytes}B (${buffer2.length}B): ${filePath}`
    );
  }
  const ext = path2.extname(filePath).toLowerCase();
  return {
    origin: "path",
    base64: buffer2.toString("base64"),
    mimeType: EXT_TO_MIME[ext],
    sizeBytes: buffer2.length
  };
}
function toMediaPayload(resolved) {
  switch (resolved.origin) {
    case "path":
    case "dataUri":
      return { base64: resolved.base64 };
    case "url":
      return { url: resolved.url };
    case "fileKey":
      return { file_key: resolved.fileKey };
  }
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/media/miora-paths.ts
var MIORA_API_BASE = "/api/ai/workbuddy-proxy";
var IMAGE_GENERATE_SUBMIT_PATH = `${MIORA_API_BASE}/image/generate/submit`;
var IMAGE_EDIT_SUBMIT_PATH = `${MIORA_API_BASE}/image/edit/submit`;
var IMAGE_QUERY_PATH = `${MIORA_API_BASE}/image/query-task`;
var VIDEO_SUBMIT_PATH = `${MIORA_API_BASE}/video/submit`;
var VIDEO_QUERY_PATH = `${MIORA_API_BASE}/video/query-task`;

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/tools/tool-definition.ts
function optionalString(value) {
  return typeof value === "string" ? value.trim() : "";
}
function logWarn(message) {
  console.error(`[miora-mcp] ${message}`);
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/tools/edit-image-tool.ts
var POLL_INTERVAL_MS = 2500;
var POLL_TIMEOUT_MS = 9e4;
var MAX_IMAGE_BYTES = 20 * 1024 * 1024;
function createEditImageTool(deps, options = {}) {
  const readFile2 = options.readFile ?? defaultReadFile();
  return {
    definition: {
      name: "miora_edit_image",
      description: "Edit / re-imagine an existing image based on a natural-language instruction, using the Miora multimodal service. " + SCENARIO_GATE_DESCRIPTION + " " + TEMP_KEY_PREFLIGHT_DESCRIPTION + " Provide at least one reference image via `images[]` (any of: absolute local path, miora fileKey, public https URL, data URI). PREFER fileKey when chaining from another miora tool \u2014 it avoids a base64 round-trip. " + DELIVERY_DESCRIPTION + " Do NOT expose tokens or credentials to the user.",
      inputSchema: {
        type: "object",
        properties: {
          tempKey: TEMP_KEY_INPUT_SCHEMA,
          prompt: {
            type: "string",
            description: 'Instruction describing how to edit / re-imagine the reference image (e.g. "make the sky purple, keep composition").'
          },
          images: {
            type: "array",
            items: { type: "string" },
            description: "One or more reference images. Each item accepts: absolute local path | miora fileKey (bga/... or file/...) | public https URL | data:image/...;base64,... URI. Prefer fileKey when available."
          },
          image_paths: {
            type: "array",
            items: { type: "string" },
            description: "DEPRECATED \u2014 use `images` instead. Legacy absolute-local-path-only channel; merged with `images` when both are present."
          },
          image_urls: {
            type: "array",
            items: { type: "string" },
            description: "DEPRECATED \u2014 use `images` instead. Legacy public-https-URL-only channel; merged with `images` when both are present."
          },
          size: {
            type: "string",
            description: "Optional output size `WIDTHxHEIGHT`. Leave empty for default."
          }
        },
        required: ["tempKey", "prompt"]
      }
    },
    handler: async (args, context) => {
      const tempKey = typeof args.tempKey === "string" ? args.tempKey.trim() : "";
      if (!tempKey) {
        return buildMediaToolError(
          "AUTH_MISSING_TEMP_KEY",
          "Missing `tempKey`. Call `connect_cloud_service` first and pass its `clientTempToken` as `tempKey`."
        );
      }
      const prompt = typeof args.prompt === "string" ? args.prompt.trim() : "";
      if (!prompt) {
        return buildMediaToolError("INVALID_INPUT", "The `prompt` field is required and cannot be empty.");
      }
      const merged = [
        ...normalizeStringArray(args.images),
        ...normalizeStringArray(args.image_paths),
        ...normalizeStringArray(args.image_urls)
      ];
      if (merged.length === 0) {
        return buildMediaToolError(
          "INVALID_INPUT",
          "At least one image must be provided via `images` (or the legacy `image_paths` / `image_urls`)."
        );
      }
      let resolved;
      try {
        resolved = await resolveImageInputs(merged, {
          readFile: readFile2,
          maxBytes: MAX_IMAGE_BYTES,
          fieldName: "images"
        });
      } catch (err) {
        if (err instanceof MioraToolError) return buildMediaToolError(err.code, err.message);
        const message = err instanceof Error ? err.message : String(err);
        return buildMediaToolError("INVALID_INPUT", `Failed to resolve image: ${message}`);
      }
      const body = {
        prompt,
        images: resolved.map(toMediaPayload)
      };
      if (typeof args.size === "string" && args.size) body.size = args.size;
      try {
        const { submit, entry } = await runMioraTask(deps, tempKey, {
          submitPath: IMAGE_EDIT_SUBMIT_PATH,
          queryPath: IMAGE_QUERY_PATH,
          body,
          pollIntervalMs: POLL_INTERVAL_MS,
          pollTimeoutMs: POLL_TIMEOUT_MS,
          onProgress: context?.reportProgress
        });
        return await buildMediaToolSuccess({
          tool: "miora_edit_image",
          upstreamTaskId: submit.upstreamTaskId,
          model: submit.model,
          provider: submit.provider,
          entry,
          onWarn: logWarn
        });
      } catch (err) {
        if (err instanceof MioraToolError) {
          return buildMediaToolError(err.code, err.message, { upstreamTaskId: err.upstreamTaskId });
        }
        const message = err instanceof Error ? err.message : String(err);
        return buildMediaToolError("UPSTREAM_ERROR", `miora_edit_image failed: ${message}`);
      }
    }
  };
}
function normalizeStringArray(value) {
  if (!Array.isArray(value)) return [];
  return value.filter((v) => typeof v === "string" && v.length > 0);
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/media/video-common.ts
var VIDEO_POLL_INTERVAL_MS = 3e3;
var VIDEO_POLL_INTERVAL_MAX_MS = 15e3;
var VIDEO_POLL_TIMEOUT_MS = 15 * 6e4;
var MAX_FRAME_BYTES = 10 * 1024 * 1024;
function fillVideoCommonFields(body, args) {
  if (args.prompt) body.prompt = args.prompt;
  if (typeof args.duration === "number") body.duration = args.duration;
  if (args.resolution) body.resolution = args.resolution;
  if (args.aspect_ratio) body.aspect_ratio = args.aspect_ratio;
}
function pickString(v) {
  return typeof v === "string" && v.length > 0 ? v : void 0;
}
function pickNumber(v) {
  return typeof v === "number" && Number.isFinite(v) ? v : void 0;
}
function normalizeStringArray2(v) {
  if (!Array.isArray(v)) return [];
  return v.filter((x) => typeof x === "string" && x.length > 0);
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/tools/frame-to-video-tool.ts
function createFrameToVideoTool(deps, options = {}) {
  const readFile2 = options.readFile ?? defaultReadFile();
  return {
    definition: {
      name: "miora_frame_to_video",
      description: "Generate a video from a starting frame (and optionally an ending frame) using the Miora multimodal service. " + SCENARIO_GATE_DESCRIPTION + " " + TEMP_KEY_PREFLIGHT_DESCRIPTION + " Provide `first_frame` (required; optionally `last_frame`). Each may be an absolute local path, a miora fileKey (`bga/...` or `file/...`), a public https URL, or a `data:image/...;base64,...` URI. PREFER fileKey when chaining from another miora tool (e.g. `miora_text_to_image` returned `resultFiles[].fileKey`) \u2014 it avoids a base64 round-trip and is significantly faster / cheaper. " + DELIVERY_DESCRIPTION + " Do NOT expose tokens or credentials to the user.",
      inputSchema: {
        type: "object",
        properties: {
          tempKey: TEMP_KEY_INPUT_SCHEMA,
          prompt: {
            type: "string",
            description: "Natural-language description of the motion / style you want between the frames."
          },
          first_frame: {
            type: "string",
            description: "Starting frame. Accepts: absolute local path | miora fileKey (bga/... or file/...) | public https URL | data:image/...;base64,... URI. Prefer fileKey when available."
          },
          last_frame: {
            type: "string",
            description: "Optional ending frame. Same formats as `first_frame`."
          },
          first_frame_path: {
            type: "string",
            description: "DEPRECATED \u2014 use `first_frame` instead. Legacy absolute-local-path-only channel; only read when `first_frame` is absent."
          },
          last_frame_path: {
            type: "string",
            description: "DEPRECATED \u2014 use `last_frame` instead. Legacy absolute-local-path-only channel; only read when `last_frame` is absent."
          },
          duration: {
            type: "number",
            description: "Optional target duration in seconds."
          },
          resolution: {
            type: "string",
            description: "Optional resolution such as `720p` or `1080p`."
          },
          aspect_ratio: {
            type: "string",
            description: "Optional aspect ratio."
          }
        },
        required: ["tempKey"]
      }
    },
    handler: async (args, context) => {
      const tempKey = pickString(args.tempKey);
      if (!tempKey) {
        return buildMediaToolError(
          "AUTH_MISSING_TEMP_KEY",
          "Missing `tempKey`. Call `connect_cloud_service` first and pass its `clientTempToken` as `tempKey`."
        );
      }
      const firstRaw = pickString(args.first_frame) ?? pickString(args.first_frame_path);
      const lastRaw = pickString(args.last_frame) ?? pickString(args.last_frame_path);
      if (!firstRaw) {
        return buildMediaToolError(
          "INVALID_INPUT",
          "The `first_frame` field is required (may also be provided via the legacy `first_frame_path`)."
        );
      }
      let first;
      let last;
      try {
        first = await resolveImageInput(firstRaw, {
          readFile: readFile2,
          maxBytes: MAX_FRAME_BYTES,
          fieldName: "first_frame"
        });
        if (lastRaw) {
          last = await resolveImageInput(lastRaw, {
            readFile: readFile2,
            maxBytes: MAX_FRAME_BYTES,
            fieldName: "last_frame"
          });
        }
      } catch (err) {
        if (err instanceof MioraToolError) return buildMediaToolError(err.code, err.message);
        const message = err instanceof Error ? err.message : String(err);
        return buildMediaToolError("INVALID_INPUT", `Failed to resolve frame: ${message}`);
      }
      const input = {
        first_frame_image: toMediaPayload(first)
      };
      if (last) input.last_frame_image = toMediaPayload(last);
      const body = { input };
      fillVideoCommonFields(body, {
        prompt: pickString(args.prompt),
        duration: pickNumber(args.duration),
        resolution: pickString(args.resolution),
        aspect_ratio: pickString(args.aspect_ratio)
      });
      try {
        const { submit, entry } = await runMioraTask(deps, tempKey, {
          submitPath: VIDEO_SUBMIT_PATH,
          queryPath: VIDEO_QUERY_PATH,
          body,
          pollIntervalMs: VIDEO_POLL_INTERVAL_MS,
          pollIntervalMaxMs: VIDEO_POLL_INTERVAL_MAX_MS,
          pollTimeoutMs: VIDEO_POLL_TIMEOUT_MS,
          onProgress: context?.reportProgress
        });
        return await buildMediaToolSuccess({
          tool: "miora_frame_to_video",
          upstreamTaskId: submit.upstreamTaskId,
          model: submit.model,
          provider: submit.provider,
          entry,
          onWarn: logWarn
        });
      } catch (err) {
        if (err instanceof MioraToolError) {
          return buildMediaToolError(err.code, err.message, { upstreamTaskId: err.upstreamTaskId });
        }
        const message = err instanceof Error ? err.message : String(err);
        return buildMediaToolError("UPSTREAM_ERROR", `miora_frame_to_video failed: ${message}`);
      }
    }
  };
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/tools/open-canvas-tool.ts
var OPEN_TOOL_NAME = "miora_open_canvas";
var OPEN_TOOL_DEFINITION = {
  name: OPEN_TOOL_NAME,
  description: "Open a `.miora` canvas file \u2014 call this after `miora_write_canvas` so the user can see the canvas immediately. The canvas is registered as an artifact card and opened automatically; it renders as an interactive read-only board.\nYou do NOT need to also call `present_files` for this canvas \u2014 doing so would just register the same file twice.",
  inputSchema: {
    type: "object",
    properties: {
      file_path: {
        type: "string",
        description: "Absolute path of the .miora file to open."
      }
    },
    required: ["file_path"]
  }
};
function errorResult(text) {
  return { isError: true, content: [{ type: "text", text }] };
}
function createOpenCanvasTool() {
  return {
    definition: OPEN_TOOL_DEFINITION,
    handler(args) {
      const filePath = optionalString(args.file_path);
      if (!filePath) {
        return errorResult("file_path \u662F\u5FC5\u586B\u53C2\u6570\uFF08.miora \u6587\u4EF6\u7684\u7EDD\u5BF9\u8DEF\u5F84\uFF09");
      }
      if (!filePath.toLowerCase().endsWith(".miora")) {
        return errorResult(`file_path \u5FC5\u987B\u4EE5 .miora \u7ED3\u5C3E\uFF0C\u5B9E\u9645\u6536\u5230 ${filePath}`);
      }
      setCurrentCanvasPath(filePath);
      return {
        content: [{ type: "text", text: `Canvas opened: ${filePath}` }],
        // daemon 侧按 `file_path` 登记文件卡（见头注释），故这是必需字段。
        structuredContent: { file_path: filePath }
      };
    }
  };
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/tools/query-task-tool.ts
var QUERY_TOOL_NAME = "miora_query_task";
function createQueryTaskTool(deps) {
  return {
    definition: {
      name: QUERY_TOOL_NAME,
      description: "Query the CURRENT status of a Miora image/video generation task by its `upstream_task_id`. Use this when a Miora generation tool (miora_text_to_image / miora_edit_image / miora_text_to_video / miora_frame_to_video / miora_reference_to_video) has returned an `upstreamTaskId` in an error/timeout payload \u2014 the backend task is likely still running or already finished, so resubmitting would charge the user a second time and orphan the paid task. This tool returns IMMEDIATELY with the backend status (does NOT wait for completion, does NOT download artifacts). If the status is `pending`/`processing`, tell the user it is still generating and check again on a later turn \u2014 do NOT poll this tool in a loop. " + TEMP_KEY_PREFLIGHT_DESCRIPTION,
      inputSchema: {
        type: "object",
        properties: {
          tempKey: TEMP_KEY_INPUT_SCHEMA,
          upstream_task_id: {
            type: "string",
            description: "The `upstreamTaskId` copied verbatim from a Miora generation tool response."
          }
        },
        required: ["tempKey", "upstream_task_id"]
      }
    },
    handler: async (args) => {
      const tempKey = typeof args.tempKey === "string" ? args.tempKey.trim() : "";
      if (!tempKey) {
        return buildMediaToolError(
          "AUTH_MISSING_TEMP_KEY",
          "Missing `tempKey`. Call `connect_cloud_service` first and pass its `clientTempToken` as `tempKey`."
        );
      }
      const taskId = typeof args.upstream_task_id === "string" ? args.upstream_task_id.trim() : "";
      if (!taskId) {
        return buildMediaToolError("INVALID_INPUT", "The `upstream_task_id` field is required.");
      }
      let data;
      try {
        data = await mioraPost(
          deps,
          IMAGE_QUERY_PATH,
          { upstreamTaskIds: [taskId] },
          tempKey
        );
      } catch (err) {
        if (err instanceof MioraToolError) {
          return buildMediaToolError(err.code, err.message, { upstreamTaskId: taskId });
        }
        const message = err instanceof Error ? err.message : String(err);
        return buildMediaToolError("UPSTREAM_ERROR", `miora_query_task failed: ${message}`, {
          upstreamTaskId: taskId
        });
      }
      const entry = data && typeof data === "object" && !Array.isArray(data) ? data[taskId] : void 0;
      if (!entry) {
        return textResult({
          type: "miora_query_task_result",
          upstreamTaskId: taskId,
          status: "not_found",
          instruction: `Miora does not recognize taskId=${taskId}. Verify the id was copied verbatim from the earlier tool result; do NOT silently regenerate \u2014 ask the user first.`
        });
      }
      const status = typeof entry.status === "string" ? entry.status : "unknown";
      return textResult({
        type: "miora_query_task_result",
        upstreamTaskId: taskId,
        status,
        ...entry.resultFiles ? { resultFiles: entry.resultFiles } : {},
        ...entry.errorMessage ? { errorMessage: entry.errorMessage } : {},
        instruction: buildStatusInstruction(status)
      });
    }
  };
}
function textResult(payload) {
  return {
    content: [{ type: "text", text: JSON.stringify(payload) }]
  };
}
function buildStatusInstruction(status) {
  switch (status) {
    case "pending":
    case "processing":
      return "The task is STILL RUNNING upstream. Do NOT resubmit and do NOT call this tool again in a loop \u2014 tell the user it is still generating and check again on a later turn.";
    case "completed":
      return "The task is COMPLETED. `resultFiles[].signedUrl` is the artifact URL from miora (time-limited). Deliver the result to the user; do NOT regenerate.";
    case "failed":
      return "The task FAILED upstream (see `errorMessage`). Report this to the user and ask whether to regenerate \u2014 a new generation is a new charge.";
    case "cancelled":
      return "The task was CANCELLED (this is different from a failure \u2014 it produced nothing). Tell the user and ask before regenerating.";
    default:
      return `Miora returned an unrecognized status \`${status}\`. Report it to the user and do NOT resubmit blindly.`;
  }
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/tools/reference-to-video-tool.ts
function createReferenceToVideoTool(deps, options = {}) {
  const readFile2 = options.readFile ?? defaultReadFile();
  return {
    definition: {
      name: "miora_reference_to_video",
      description: "Generate a video guided by one or more reference images (character / style / composition) using the Miora multimodal service. " + SCENARIO_GATE_DESCRIPTION + " " + TEMP_KEY_PREFLIGHT_DESCRIPTION + " Provide `references` (required). Each item may be an absolute local path, a miora fileKey (`bga/...` or `file/...`), a public https URL, or a `data:image/...;base64,...` URI. PREFER fileKey when chaining from another miora tool. " + DELIVERY_DESCRIPTION + " Do NOT expose tokens or credentials to the user.",
      inputSchema: {
        type: "object",
        properties: {
          tempKey: TEMP_KEY_INPUT_SCHEMA,
          prompt: {
            type: "string",
            description: "Natural-language description of what should happen in the video."
          },
          references: {
            type: "array",
            items: { type: "string" },
            description: "One or more reference images. Each item accepts: absolute local path | miora fileKey (bga/... or file/...) | public https URL | data:image/...;base64,... URI. Prefer fileKey when available."
          },
          reference_paths: {
            type: "array",
            items: { type: "string" },
            description: "DEPRECATED \u2014 use `references` instead. Legacy absolute-local-path-only channel; only read when `references` is absent."
          },
          duration: {
            type: "number",
            description: "Optional target duration in seconds."
          },
          resolution: {
            type: "string",
            description: "Optional resolution."
          },
          aspect_ratio: {
            type: "string",
            description: "Optional aspect ratio."
          }
        },
        required: ["tempKey"]
      }
    },
    handler: async (args, context) => {
      const tempKey = pickString(args.tempKey);
      if (!tempKey) {
        return buildMediaToolError(
          "AUTH_MISSING_TEMP_KEY",
          "Missing `tempKey`. Call `connect_cloud_service` first and pass its `clientTempToken` as `tempKey`."
        );
      }
      const referencesRaw = normalizeStringArray2(args.references);
      const legacyPaths = normalizeStringArray2(args.reference_paths);
      const inputs = referencesRaw.length > 0 ? referencesRaw : legacyPaths;
      if (inputs.length === 0) {
        return buildMediaToolError(
          "INVALID_INPUT",
          "The `references` field must contain at least one item (may also be provided via the legacy `reference_paths`)."
        );
      }
      let resolved;
      try {
        resolved = await resolveImageInputs(inputs, {
          readFile: readFile2,
          maxBytes: MAX_FRAME_BYTES,
          fieldName: "references"
        });
      } catch (err) {
        if (err instanceof MioraToolError) return buildMediaToolError(err.code, err.message);
        const message = err instanceof Error ? err.message : String(err);
        return buildMediaToolError("INVALID_INPUT", `Failed to resolve reference: ${message}`);
      }
      const input = {
        reference_images: resolved.map(toMediaPayload)
      };
      const body = { input };
      fillVideoCommonFields(body, {
        prompt: pickString(args.prompt),
        duration: pickNumber(args.duration),
        resolution: pickString(args.resolution),
        aspect_ratio: pickString(args.aspect_ratio)
      });
      try {
        const { submit, entry } = await runMioraTask(deps, tempKey, {
          submitPath: VIDEO_SUBMIT_PATH,
          queryPath: VIDEO_QUERY_PATH,
          body,
          pollIntervalMs: VIDEO_POLL_INTERVAL_MS,
          pollIntervalMaxMs: VIDEO_POLL_INTERVAL_MAX_MS,
          pollTimeoutMs: VIDEO_POLL_TIMEOUT_MS,
          onProgress: context?.reportProgress
        });
        return await buildMediaToolSuccess({
          tool: "miora_reference_to_video",
          upstreamTaskId: submit.upstreamTaskId,
          model: submit.model,
          provider: submit.provider,
          entry,
          onWarn: logWarn
        });
      } catch (err) {
        if (err instanceof MioraToolError) {
          return buildMediaToolError(err.code, err.message, { upstreamTaskId: err.upstreamTaskId });
        }
        const message = err instanceof Error ? err.message : String(err);
        return buildMediaToolError("UPSTREAM_ERROR", `miora_reference_to_video failed: ${message}`);
      }
    }
  };
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/tools/text-to-image-tool.ts
var POLL_INTERVAL_MS2 = 2500;
var POLL_TIMEOUT_MS2 = 9e4;
function createTextToImageTool(deps) {
  return {
    definition: {
      name: "miora_text_to_image",
      description: "Generate an image from a natural-language prompt using the Miora multimodal service. " + SCENARIO_GATE_DESCRIPTION + " " + TEMP_KEY_PREFLIGHT_DESCRIPTION + " " + DELIVERY_DESCRIPTION + " Do NOT expose or mention tokens, credentials, or authentication details to the user.",
      inputSchema: {
        type: "object",
        properties: {
          tempKey: TEMP_KEY_INPUT_SCHEMA,
          prompt: {
            type: "string",
            description: "The natural-language description of the image to generate. Be specific about subject, style, composition and lighting."
          },
          size: {
            type: "string",
            description: "Optional image size, `WIDTHxHEIGHT` (e.g. `1024x1024`, `1024x768`). Leave empty for model default."
          },
          quality: {
            type: "string",
            description: "Optional quality tier accepted by the target model (e.g. `standard` / `hd`). Leave empty for default."
          }
        },
        required: ["tempKey", "prompt"]
      }
    },
    handler: async (args, context) => {
      const tempKey = typeof args.tempKey === "string" ? args.tempKey.trim() : "";
      if (!tempKey) {
        return buildMediaToolError(
          "AUTH_MISSING_TEMP_KEY",
          "Missing `tempKey`. Call `connect_cloud_service` first and pass its `clientTempToken` as `tempKey`."
        );
      }
      const prompt = typeof args.prompt === "string" ? args.prompt.trim() : "";
      if (!prompt) {
        return buildMediaToolError("INVALID_INPUT", "The `prompt` field is required and cannot be empty.");
      }
      const body = { prompt };
      if (typeof args.size === "string" && args.size) body.size = args.size;
      if (typeof args.quality === "string" && args.quality) body.quality = args.quality;
      try {
        const { submit, entry } = await runMioraTask(deps, tempKey, {
          submitPath: IMAGE_GENERATE_SUBMIT_PATH,
          queryPath: IMAGE_QUERY_PATH,
          body,
          pollIntervalMs: POLL_INTERVAL_MS2,
          pollTimeoutMs: POLL_TIMEOUT_MS2,
          onProgress: context?.reportProgress
        });
        return await buildMediaToolSuccess({
          tool: "miora_text_to_image",
          upstreamTaskId: submit.upstreamTaskId,
          model: submit.model,
          provider: submit.provider,
          entry,
          onWarn: logWarn
        });
      } catch (err) {
        if (err instanceof MioraToolError) {
          return buildMediaToolError(err.code, err.message, { upstreamTaskId: err.upstreamTaskId });
        }
        const message = err instanceof Error ? err.message : String(err);
        return buildMediaToolError("UPSTREAM_ERROR", `miora_text_to_image failed: ${message}`);
      }
    }
  };
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/tools/text-to-video-tool.ts
function createTextToVideoTool(deps) {
  return {
    definition: {
      name: "miora_text_to_video",
      description: "Generate a video from a natural-language prompt using the Miora multimodal service. " + SCENARIO_GATE_DESCRIPTION + " " + TEMP_KEY_PREFLIGHT_DESCRIPTION + " Video generation may take up to ~15 minutes; the tool blocks until the task completes or times out. " + DELIVERY_DESCRIPTION + " Do NOT expose tokens or credentials to the user.",
      inputSchema: {
        type: "object",
        properties: {
          tempKey: TEMP_KEY_INPUT_SCHEMA,
          prompt: {
            type: "string",
            description: "Natural-language description of the video (subject, action, style, camera)."
          },
          duration: {
            type: "number",
            description: "Optional target duration in seconds (typical range 3\u201310)."
          },
          resolution: {
            type: "string",
            description: "Optional resolution such as `720p` or `1080p`."
          },
          aspect_ratio: {
            type: "string",
            description: "Optional aspect ratio such as `16:9`, `9:16`, `1:1`."
          }
        },
        required: ["tempKey", "prompt"]
      }
    },
    handler: async (args, context) => {
      const tempKey = pickString(args.tempKey);
      if (!tempKey) {
        return buildMediaToolError(
          "AUTH_MISSING_TEMP_KEY",
          "Missing `tempKey`. Call `connect_cloud_service` first and pass its `clientTempToken` as `tempKey`."
        );
      }
      const prompt = pickString(args.prompt);
      if (!prompt) {
        return buildMediaToolError("INVALID_INPUT", "The `prompt` field is required.");
      }
      const body = {};
      fillVideoCommonFields(body, {
        prompt,
        duration: pickNumber(args.duration),
        resolution: pickString(args.resolution),
        aspect_ratio: pickString(args.aspect_ratio)
      });
      try {
        const { submit, entry } = await runMioraTask(deps, tempKey, {
          submitPath: VIDEO_SUBMIT_PATH,
          queryPath: VIDEO_QUERY_PATH,
          body,
          pollIntervalMs: VIDEO_POLL_INTERVAL_MS,
          pollIntervalMaxMs: VIDEO_POLL_INTERVAL_MAX_MS,
          pollTimeoutMs: VIDEO_POLL_TIMEOUT_MS,
          onProgress: context?.reportProgress
        });
        return await buildMediaToolSuccess({
          tool: "miora_text_to_video",
          upstreamTaskId: submit.upstreamTaskId,
          model: submit.model,
          provider: submit.provider,
          entry,
          onWarn: logWarn
        });
      } catch (err) {
        if (err instanceof MioraToolError) {
          return buildMediaToolError(err.code, err.message, { upstreamTaskId: err.upstreamTaskId });
        }
        const message = err instanceof Error ? err.message : String(err);
        return buildMediaToolError("UPSTREAM_ERROR", `miora_text_to_video failed: ${message}`);
      }
    }
  };
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/tools/write-canvas-tool.ts
var import_node_path2 = __toESM(require("node:path"));

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/plugin-canvas-entry.ts
var import_node_crypto4 = require("node:crypto");
var import_promises5 = require("node:fs/promises");
var path4 = __toESM(require("node:path"));

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/canvas-builder.ts
var import_node_crypto2 = require("node:crypto");
var SESSION_ID = 1;
var PAGE_LOCAL_ID = 1;
var FIRST_CONTENT_LOCAL_ID = 2;
var ITEM_ID_PREFIX = "wb-item:";
var LAYOUT = {
  /** 起始坐标，留出左上角工具栏的视觉余量 */
  originX: 80,
  originY: 80,
  /** 节点间距 */
  gap: 40,
  /** grid 模式每行列数 */
  columns: 4
};
var DEFAULT_SIZE = {
  image: { width: 320, height: 320 },
  video: { width: 480, height: 270 },
  text: { width: 400, height: 60 }
};
var DEFAULT_TEXT_STYLE = {
  fontSize: 22,
  color: "#222222"
};
function encodeItemId(itemId) {
  return `${ITEM_ID_PREFIX}${itemId}`;
}
function decodeItemId(desc) {
  if (!desc?.startsWith(ITEM_ID_PREFIX)) {
    return void 0;
  }
  const id = desc.slice(ITEM_ID_PREFIX.length);
  return id || void 0;
}
function customNodeTypeOf(type) {
  switch (type) {
    case "image":
      return "IMAGE_GENERATOR";
    case "video":
      return "VIDEO_GENERATOR";
    case "text":
      return "TEXT_LAYER";
  }
}
function buildNodeData(item, itemId) {
  const base = {
    type: customNodeTypeOf(item.type),
    title: item.title ?? defaultTitleOf(item),
    // 条目 id 存 desc：`guid.localID` 是数字，承载不了模型给的字符串 id，
    // 而 desc 是 schema 里现成的自由文本字段。
    desc: encodeItemId(itemId)
  };
  if (item.type === "text") {
    return {
      ...base,
      text: item.text ?? "",
      fontSize: item.fontSize ?? DEFAULT_TEXT_STYLE.fontSize,
      color: item.color ?? DEFAULT_TEXT_STYLE.color
    };
  }
  return {
    ...base,
    fileKeys: item.fileKey ? [item.fileKey] : [],
    // 多产物节点用它指示当前展示第几个；单文件节点固定 0。
    // 缺省会让 Miora 侧取不到资源（undefined 不等于 0）。
    selectedIndex: 0
  };
}
function defaultTitleOf(item) {
  if (item.type === "text") {
    const preview = (item.text ?? "").trim().slice(0, 12);
    return preview || "\u6587\u5B57";
  }
  if (item.fileKey) {
    return item.fileKey.split("/").pop() ?? item.fileKey;
  }
  return item.type === "image" ? "\u56FE\u7247" : "\u89C6\u9891";
}
function sizeOf(item) {
  const fallback = DEFAULT_SIZE[item.type];
  return {
    width: item.width ?? fallback.width,
    height: item.height ?? fallback.height
  };
}
function buildNodeChange(params) {
  const isPage = !params.customType;
  return {
    guid: { sessionID: SESSION_ID, localID: params.localID },
    phase: "CREATED",
    // NodeType 只有 NONE / PAGE / CUSTOM 三种；具体类型由 NodeData.type 决定
    type: isPage ? "PAGE" : "CUSTOM",
    visible: true,
    position: { x: params.x, y: params.y },
    width: params.width,
    height: params.height,
    zIndex: BigInt(params.localID),
    // ⚠️ parentIndex 不可省：Miora 侧 `filterActivePageNodeChanges` 靠
    // `collectNodeIdsRecursively` 从 `activePageId` 出发**沿 parentIndex 递归**
    // 收集本页节点，没挂到页面节点上的内容节点会被整体过滤掉 ——
    // 表现是"解码出 N 个 nodeChanges，但画布上一个节点都没有"，且无任何报错。
    ...isPage ? {} : { parentIndex: { guid: { sessionID: SESSION_ID, localID: PAGE_LOCAL_ID }, position: "a" } },
    data: params.data
  };
}
function createLayoutCursor(layout, columnWidth, startY = LAYOUT.originY) {
  let indexInRow = 0;
  let rowY = startY;
  let rowMaxHeight = 0;
  return function next(width, height) {
    if (layout === "column") {
      const y = rowY;
      rowY += height + LAYOUT.gap;
      return { x: LAYOUT.originX, y };
    }
    const columns = layout === "row" ? Number.POSITIVE_INFINITY : LAYOUT.columns;
    if (indexInRow >= columns) {
      rowY += rowMaxHeight + LAYOUT.gap;
      rowMaxHeight = 0;
      indexInRow = 0;
    }
    const x = LAYOUT.originX + indexInRow * (columnWidth + LAYOUT.gap);
    rowMaxHeight = Math.max(rowMaxHeight, height);
    indexInRow += 1;
    return { x, y: rowY };
  };
}
function buildWorkflow(input, existing, preserveNodes = []) {
  const now = BigInt(Date.now());
  const layout = input.layout ?? "grid";
  const sizes = input.items.map(sizeOf);
  const columnWidth = sizes.reduce(
    (max, s) => Math.max(max, s.width),
    DEFAULT_SIZE.image.width
  );
  const startY = existing ? maxBottomOf(existing) + LAYOUT.gap : LAYOUT.originY;
  const nextPosition = createLayoutCursor(layout, columnWidth, startY);
  const pageNode = buildNodeChange({
    localID: PAGE_LOCAL_ID,
    customType: void 0,
    x: 0,
    y: 0,
    // 页面节点尺寸不参与渲染，Miora 侧按内容自适应
    width: 0,
    height: 0,
    data: { title: input.title, pageName: input.title, pageOrder: 0 }
  });
  let nextLocalId = Math.max(
    FIRST_CONTENT_LOCAL_ID,
    (maxLocalIdOf(existing) ?? PAGE_LOCAL_ID) + 1
  );
  const contentNodes = input.items.map((item, index) => {
    const { width, height } = sizes[index] ?? sizeOf(item);
    const fixedX = item.x;
    const fixedY = item.y;
    const auto = fixedX !== void 0 && fixedY !== void 0 ? { x: fixedX, y: fixedY } : nextPosition(width, height);
    const localID = nextLocalId;
    nextLocalId += 1;
    return buildNodeChange({
      localID,
      customType: customNodeTypeOf(item.type),
      x: auto.x,
      y: auto.y,
      width,
      height,
      data: buildNodeData(item, item.id ?? `item-${localID}`)
    });
  });
  return {
    id: existing?.id ?? `wb-${(0, import_node_crypto2.randomUUID)()}`,
    sessionId: SESSION_ID,
    title: input.title,
    createAt: existing?.createAt ?? now,
    updateAt: now,
    data: {
      desc: "workbuddy",
      // pageNode 必须在最前（投影链路以它为根），wb-item 节点其次，
      // 用户手工添加的节点最后 —— update 时 wb-item 变化不扰动它们的次序
      nodeChanges: [pageNode, ...contentNodes, ...preserveNodes],
      // 边同样透传：用户在 Miora 里连的线不属于 wb-item 体系，
      // 重置为 [] 会和节点一样被静默清空
      edgeChanges: existing?.data?.edgeChanges ?? [],
      // 投影链路以它为根收集本页节点，缺失会导致画布空白
      activePageId: { sessionID: SESSION_ID, localID: PAGE_LOCAL_ID }
    }
  };
}
function maxLocalIdOf(workflow) {
  const changes = workflow?.data?.nodeChanges;
  if (!changes?.length) {
    return void 0;
  }
  return changes.reduce((max, change) => Math.max(max, change.guid?.localID ?? 0), 0);
}
function maxBottomOf(workflow) {
  let bottom = LAYOUT.originY - LAYOUT.gap;
  for (const change of workflow.data?.nodeChanges ?? []) {
    if (change.type !== "CUSTOM") {
      continue;
    }
    bottom = Math.max(bottom, (change.position?.y ?? 0) + (change.height ?? 0));
  }
  return bottom;
}
function extractItems(workflow) {
  const result = [];
  for (const change of workflow.data?.nodeChanges ?? []) {
    const customType = change.data?.type;
    if (!customType || change.type !== "CUSTOM") {
      continue;
    }
    const itemId = decodeItemId(change.data?.desc);
    if (!itemId) {
      continue;
    }
    const type = canvasItemTypeOf(customType);
    if (!type) {
      continue;
    }
    result.push({
      localID: change.guid?.localID ?? 0,
      item: {
        id: itemId,
        type,
        fileKey: change.data?.fileKeys?.[0],
        text: change.data?.text,
        title: change.data?.title,
        width: change.width,
        height: change.height,
        x: change.position?.x,
        y: change.position?.y,
        fontSize: change.data?.fontSize,
        color: change.data?.color
      }
    });
  }
  return result;
}
function extractForeignNodes(workflow) {
  const out = [];
  for (const change of workflow.data?.nodeChanges ?? []) {
    if (change.type !== "CUSTOM") {
      continue;
    }
    const customType = change.data?.type;
    const itemId = decodeItemId(change.data?.desc);
    if (customType && itemId && canvasItemTypeOf(customType)) {
      continue;
    }
    out.push(change);
  }
  return out;
}
function canvasItemTypeOf(customType) {
  switch (customType) {
    case "IMAGE_GENERATOR":
      return "image";
    case "VIDEO_GENERATOR":
      return "video";
    case "TEXT_LAYER":
      return "text";
    default:
      return void 0;
  }
}
var CANVAS_INTERNALS = {
  SESSION_ID,
  PAGE_LOCAL_ID,
  ITEM_ID_PREFIX,
  LAYOUT,
  DEFAULT_SIZE
};

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/empty-canvas-template.ts
var import_node_crypto3 = require("node:crypto");
var import_node_fs4 = require("node:fs");

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/schema-runtime.ts
var import_node_fs3 = require("node:fs");
var import_kiwi_schema = __toESM(require_kiwi());
var cachedSchemas = /* @__PURE__ */ new Map();
function getMioraSchema(options) {
  const cacheKey = options.schemaPath;
  const cached = cachedSchemas.get(cacheKey);
  if (cached) {
    return cached;
  }
  const schemaText = (0, import_node_fs3.readFileSync)(options.schemaPath, "utf-8");
  const compiled = (0, import_kiwi_schema.compileSchema)((0, import_kiwi_schema.parseSchema)(schemaText));
  cachedSchemas.set(cacheKey, compiled);
  return compiled;
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/miora-file-codec.ts
var MIORA_FORMAT_VERSION = 1;
var MioraFileParseError = class extends Error {
  constructor(reason, message, options) {
    super(message, options);
    this.name = "MioraFileParseError";
    this.reason = reason;
  }
};
function normalizeAssetsBaseDir(raw) {
  const trimmed = raw?.trim();
  if (!trimmed || trimmed === "." || trimmed === "./") {
    return "";
  }
  return trimmed.replace(/^\/+/, "").replace(/\/+$/, "");
}
function parseMioraFile(bytes, options) {
  if (!bytes || bytes.length === 0) {
    throw new MioraFileParseError("empty", "\u753B\u5E03\u6587\u4EF6\u4E3A\u7A7A");
  }
  let decoded;
  try {
    decoded = getMioraSchema(options).decodeMioraLocalFile(bytes);
  } catch (error) {
    throw new MioraFileParseError("decode", "\u753B\u5E03\u6587\u4EF6\u5DF2\u635F\u574F\u6216\u683C\u5F0F\u4E0D\u6B63\u786E", { cause: error });
  }
  const { formatVersion } = decoded;
  if (formatVersion !== MIORA_FORMAT_VERSION) {
    throw new MioraFileParseError(
      "version",
      `\u4E0D\u652F\u6301\u7684\u753B\u5E03\u6587\u4EF6\u7248\u672C\uFF08\u671F\u671B ${MIORA_FORMAT_VERSION}\uFF0C\u5B9E\u9645 ${formatVersion ?? "\u7F3A\u5931"}\uFF09`
    );
  }
  const { workflow } = decoded;
  if (!workflow?.data) {
    throw new MioraFileParseError("payload", "\u753B\u5E03\u6587\u4EF6\u7F3A\u5C11\u5185\u5BB9\u6570\u636E");
  }
  return {
    workflow,
    assetsBaseDir: normalizeAssetsBaseDir(decoded.assetsBaseDir)
  };
}
var MIORA_GENERATOR = "workbuddy-miora-tool";
function encodeMioraFile(input, options) {
  return getMioraSchema(options).encodeMioraLocalFile({
    formatVersion: MIORA_FORMAT_VERSION,
    workflow: input.workflow,
    assetsBaseDir: input.assetsBaseDir,
    generator: input.generator ?? MIORA_GENERATOR
  });
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/empty-canvas-template.ts
var EMPTY_CANVAS_TEMPLATE_NAME = "miora/empty-canvas.miora";
var cachedTemplateBytes = /* @__PURE__ */ new Map();
function readTemplateBytes(explicitPath) {
  const cacheKey = explicitPath;
  const cached = cachedTemplateBytes.get(cacheKey);
  if (cached) {
    return cached;
  }
  const templatePath = explicitPath;
  const bytes = new Uint8Array((0, import_node_fs4.readFileSync)(templatePath));
  cachedTemplateBytes.set(cacheKey, bytes);
  return bytes;
}
function buildEmptyCanvasFromTemplate(title, now, options) {
  const { workflow } = parseMioraFile(
    readTemplateBytes(options.templatePath),
    { schemaPath: options.schemaPath }
  );
  workflow.title = title;
  workflow.id = `wb-${(0, import_node_crypto3.randomUUID)()}`;
  workflow.createAt = now;
  workflow.updateAt = now;
  const pageNode = workflow.data?.nodeChanges?.find((node) => node.guid?.localID === CANVAS_INTERNALS.PAGE_LOCAL_ID);
  if (!pageNode) {
    throw new Error(
      `\u7A7A\u753B\u5E03\u6A21\u677F\u7F3A\u5C11\u9ED8\u8BA4 PAGE \u8282\u70B9\uFF08localID=${CANVAS_INTERNALS.PAGE_LOCAL_ID}\uFF09\uFF1A${EMPTY_CANVAS_TEMPLATE_NAME}\u3002\u8BE5\u8282\u70B9\u7F3A\u5931\u4F1A\u8BA9\u753B\u5E03\u9759\u9ED8\u7A7A\u767D\uFF0C\u8BF7\u4FEE\u590D\u6A21\u677F\u6587\u4EF6\u3002`
    );
  }
  if (pageNode.data) {
    pageNode.data.title = title;
    pageNode.data.pageName = title;
  }
  return workflow;
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/media-dimensions.ts
var import_promises4 = require("node:fs/promises");
var path3 = __toESM(require("node:path"));
var IMAGE_HEADER_BYTES = 64;
var JPEG_SEGMENT_HEADER_BYTES = 4;
var JPEG_SOF_HEADER_BYTES = 9;
var MAX_JPEG_SEGMENTS = 1024;
var MAX_BOX_ITERATIONS = 512;
function fixed1616ToPixels(raw) {
  return Math.round(raw / 65536);
}
function isPositiveSize(dims) {
  return !!dims && Number.isFinite(dims.width) && Number.isFinite(dims.height) && dims.width > 0 && dims.height > 0;
}
async function readImageDimensions(filePath, ext) {
  let fd;
  try {
    fd = await (0, import_promises4.open)(filePath, "r");
    if (ext === ".jpg" || ext === ".jpeg") {
      const { size } = await fd.stat();
      return await readJpegDimensionsBySeek(fd, size);
    }
    const head = Buffer.alloc(IMAGE_HEADER_BYTES);
    const { bytesRead } = await fd.read(head, 0, IMAGE_HEADER_BYTES, 0);
    if (ext === ".png") {
      if (bytesRead >= 24) {
        return { width: head.readUInt32BE(16), height: head.readUInt32BE(20) };
      }
      return void 0;
    }
    if (ext === ".gif") {
      if (bytesRead >= 10) {
        return { width: head.readUInt16LE(6), height: head.readUInt16LE(8) };
      }
      return void 0;
    }
    if (ext === ".webp") {
      return parseWebpDimensions(head.subarray(0, bytesRead));
    }
    return void 0;
  } catch {
    return void 0;
  } finally {
    await fd?.close().catch(() => void 0);
  }
}
function isJpegSofMarker(marker) {
  return marker >= 192 && marker <= 195 || marker >= 197 && marker <= 199 || marker >= 201 && marker <= 203 || marker >= 205 && marker <= 207;
}
async function readJpegDimensionsBySeek(fd, fileSize) {
  const soi = Buffer.alloc(2);
  const { bytesRead: soiBytes } = await fd.read(soi, 0, 2, 0);
  if (soiBytes < 2 || soi.readUInt16BE(0) !== 65496) {
    return void 0;
  }
  const head = Buffer.alloc(JPEG_SOF_HEADER_BYTES);
  let offset = 2;
  let segments = 0;
  while (offset + JPEG_SEGMENT_HEADER_BYTES <= fileSize && segments < MAX_JPEG_SEGMENTS) {
    segments += 1;
    const { bytesRead } = await fd.read(head, 0, JPEG_SOF_HEADER_BYTES, offset);
    if (bytesRead < JPEG_SEGMENT_HEADER_BYTES) {
      return void 0;
    }
    if (head[0] !== 255) {
      return void 0;
    }
    const marker = head[1];
    if (marker === 255) {
      offset += 1;
      continue;
    }
    if (marker === 1 || marker >= 208 && marker <= 217) {
      offset += 2;
      continue;
    }
    if (marker === 218) {
      return void 0;
    }
    const size = head.readUInt16BE(2);
    if (size < 2) {
      return void 0;
    }
    if (isJpegSofMarker(marker)) {
      if (bytesRead < JPEG_SOF_HEADER_BYTES) {
        return void 0;
      }
      return { height: head.readUInt16BE(5), width: head.readUInt16BE(7) };
    }
    offset += 2 + size;
  }
  return void 0;
}
function parseWebpDimensions(buf) {
  if (buf.length < 16 || buf.toString("latin1", 0, 4) !== "RIFF" || buf.toString("latin1", 8, 12) !== "WEBP") {
    return void 0;
  }
  const chunk = buf.toString("latin1", 12, 16);
  if (chunk === "VP8X" && buf.length >= 30) {
    return {
      width: buf.readUIntLE(24, 3) + 1,
      height: buf.readUIntLE(27, 3) + 1
    };
  }
  if (chunk === "VP8 " && buf.length >= 30) {
    if (buf[23] !== 157 || buf[24] !== 1 || buf[25] !== 42) {
      return void 0;
    }
    return {
      width: buf.readUInt16LE(26) & 16383,
      height: buf.readUInt16LE(28) & 16383
    };
  }
  if (chunk === "VP8L" && buf.length >= 25) {
    if (buf[20] !== 47) {
      return void 0;
    }
    const bits = buf.readUInt32LE(21);
    return {
      width: (bits & 16383) + 1,
      height: (bits >> 14 & 16383) + 1
    };
  }
  return void 0;
}
function* iterateBoxes(buf, from, to) {
  let offset = from;
  let guard = 0;
  while (offset + 8 <= to && guard++ < MAX_BOX_ITERATIONS) {
    let size = buf.readUInt32BE(offset);
    const type = buf.toString("latin1", offset + 4, offset + 8);
    let headerSize = 8;
    if (size === 1) {
      if (offset + 16 > to) {
        return;
      }
      size = Number(buf.readBigUInt64BE(offset + 8));
      headerSize = 16;
    } else if (size === 0) {
      size = to - offset;
    }
    if (size < headerSize || offset + size > to) {
      return;
    }
    yield { type, start: offset + headerSize, end: offset + size };
    offset += size;
  }
}
async function readVideoDimensions(filePath) {
  let fd;
  try {
    fd = await (0, import_promises4.open)(filePath, "r");
    const { size } = await fd.stat();
    if (!(size > 0)) {
      return void 0;
    }
    const buf = Buffer.alloc(size);
    const { bytesRead } = await fd.read(buf, 0, size, 0);
    const view = buf.subarray(0, bytesRead);
    for (const top of iterateBoxes(view, 0, view.length)) {
      if (top.type !== "moov") {
        continue;
      }
      for (const child of iterateBoxes(view, top.start, top.end)) {
        if (child.type !== "trak") {
          continue;
        }
        for (const leaf of iterateBoxes(view, child.start, child.end)) {
          if (leaf.type !== "tkhd") {
            continue;
          }
          const dims = parseTkhdDimensions(view, leaf.start, leaf.end);
          if (isPositiveSize(dims)) {
            return dims;
          }
        }
      }
    }
    return void 0;
  } catch {
    return void 0;
  } finally {
    await fd?.close().catch(() => void 0);
  }
}
function parseTkhdDimensions(buf, start, end) {
  if (start >= end) {
    return void 0;
  }
  const version = buf[start];
  const timeFieldsSize = version === 1 ? 32 : 20;
  const dimsOffset = start + 4 + timeFieldsSize + 52;
  if (dimsOffset + 8 > end) {
    return void 0;
  }
  return {
    width: fixed1616ToPixels(buf.readUInt32BE(dimsOffset)),
    height: fixed1616ToPixels(buf.readUInt32BE(dimsOffset + 4))
  };
}
var IMAGE_EXTS = /* @__PURE__ */ new Set([".png", ".gif", ".jpg", ".jpeg", ".webp"]);
var VIDEO_EXTS = /* @__PURE__ */ new Set([".mp4", ".m4v", ".mov"]);
async function probeMediaDimensions(filePath) {
  const ext = path3.extname(filePath).toLowerCase();
  const dims = IMAGE_EXTS.has(ext) ? await readImageDimensions(filePath, ext) : VIDEO_EXTS.has(ext) ? await readVideoDimensions(filePath) : void 0;
  return isPositiveSize(dims) ? dims : void 0;
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/plugin-canvas-entry.ts
var MIORA_EXT = ".miora";
var ASSETS_DIR_SUFFIX = "_assets";
var ALLOWED_ASSET_EXTS = /* @__PURE__ */ new Set([
  ".png",
  ".jpg",
  ".jpeg",
  ".gif",
  ".webp",
  ".svg",
  ".bmp",
  ".ico",
  ".mp4",
  ".webm",
  ".mov",
  ".avi",
  ".mkv"
]);
var CANVAS_ERROR_CODES = {
  generic: "19930",
  invalidParams: "19931",
  fileNotFound: "19932",
  parseError: "19933",
  writeError: "19934"
};
function fail(error, code = CANVAS_ERROR_CODES.generic) {
  return { ok: false, error, code };
}
function isPathInside(parentDir, childPath) {
  const rel = path4.relative(path4.resolve(parentDir), childPath);
  return rel !== "" && !rel.startsWith("..") && !path4.isAbsolute(rel);
}
function resolveAllowedCanvasRoots() {
  return [process.env.CODEBUDDY_PROJECT_DIR, process.env.MIORA_DATA_DIR].map((value) => value?.trim()).filter((value) => !!value).map((value) => path4.resolve(value));
}
function findHiddenSegment(root, resolvedFilePath) {
  const relative2 = path4.relative(root, resolvedFilePath);
  const dirSegments = relative2.split(path4.sep).slice(0, -1);
  return dirSegments.find((segment) => segment.startsWith(".") && segment !== ".");
}
function toPosixPath(value) {
  return value.split(path4.sep).join("/");
}
function assetFileNameFor(sourcePath) {
  const hash = (0, import_node_crypto4.createHash)("sha1").update(path4.resolve(sourcePath)).digest("hex").slice(0, 8);
  return `${hash}-${path4.basename(sourcePath)}`;
}
function optionalNumber(value) {
  return typeof value === "number" && Number.isFinite(value) ? value : void 0;
}
function optionalString2(value) {
  return typeof value === "string" && value.trim() ? value.trim() : void 0;
}
function normalizeItem(raw, index) {
  if (!raw || typeof raw !== "object") {
    return { error: `items[${index}] \u5FC5\u987B\u662F\u5BF9\u8C61` };
  }
  const candidate = raw;
  const type = candidate.type;
  if (type !== "image" && type !== "video") {
    return {
      error: `items[${index}].type \u53EA\u652F\u6301 image / video\uFF0C\u5B9E\u9645\u6536\u5230 ${JSON.stringify(type)}\u3002\u753B\u5E03\u4E0D\u518D\u5199\u5165\u6587\u5B57\u8282\u70B9 \u2014\u2014 \u6807\u9898\u3001\u6A21\u5757\u540D\u3001\u8BF4\u660E\u6587\u5B57\u8BF7\u653E\u5728\u56DE\u590D\u6D88\u606F\u4E2D\uFF0C\u4E0D\u8981\u4F5C\u4E3A\u753B\u5E03\u6761\u76EE\u4F20\u5165\u3002`
    };
  }
  const item = {
    id: optionalString2(candidate.id),
    type,
    title: optionalString2(candidate.title),
    width: optionalNumber(candidate.width),
    height: optionalNumber(candidate.height),
    x: optionalNumber(candidate.x),
    y: optionalNumber(candidate.y)
  };
  const sourcePath = optionalString2(candidate.source_path);
  if (!sourcePath) {
    return { error: `items[${index}] \u662F ${type} \u7C7B\u578B\uFF0C\u5FC5\u987B\u63D0\u4F9B source_path\uFF08\u672C\u5730\u7EDD\u5BF9\u8DEF\u5F84\uFF09` };
  }
  if (!path4.isAbsolute(sourcePath)) {
    return { error: `items[${index}].source_path \u5FC5\u987B\u662F\u7EDD\u5BF9\u8DEF\u5F84\uFF0C\u5B9E\u9645\u6536\u5230 ${sourcePath}` };
  }
  const ext = path4.extname(sourcePath).toLowerCase();
  if (!ALLOWED_ASSET_EXTS.has(ext)) {
    return {
      error: `items[${index}].source_path \u7684\u6269\u5C55\u540D ${ext || "(\u7A7A)"} \u4E0D\u53D7\u652F\u6301\u3002\u53EF\u7528\uFF1A${[...ALLOWED_ASSET_EXTS].join(" ")}`
    };
  }
  return { item, sourcePath };
}
async function copyAssets(entries, filePath) {
  const canvasDir = path4.dirname(filePath);
  const assetsDirName = `${path4.basename(filePath, MIORA_EXT)}${ASSETS_DIR_SUFFIX}`;
  const assetsDir = path4.join(canvasDir, assetsDirName);
  const copied = [];
  const survivors = [];
  const failed = [];
  if (entries.some((entry) => entry.sourcePath)) {
    await (0, import_promises5.mkdir)(assetsDir, { recursive: true });
  }
  for (const entry of entries) {
    const { sourcePath } = entry;
    if (!sourcePath) {
      survivors.push(entry);
      continue;
    }
    try {
      const info = await (0, import_promises5.stat)(sourcePath);
      if (!info.isFile()) {
        failed.push({ index: entry.index, reason: `items[${entry.index}] \u8D44\u6E90\u4E0D\u662F\u666E\u901A\u6587\u4EF6\uFF1A${sourcePath}` });
        continue;
      }
    } catch {
      failed.push({ index: entry.index, reason: `items[${entry.index}] \u8D44\u6E90\u6587\u4EF6\u4E0D\u5B58\u5728\uFF1A${sourcePath}` });
      continue;
    }
    const targetPath = path4.join(assetsDir, assetFileNameFor(sourcePath));
    if (!isPathInside(assetsDir, targetPath)) {
      failed.push({ index: entry.index, reason: `items[${entry.index}] \u8D44\u6E90\u76EE\u6807\u8DEF\u5F84\u8D8A\u754C\uFF1A${targetPath}` });
      continue;
    }
    try {
      await (0, import_promises5.copyFile)(sourcePath, targetPath);
    } catch (error) {
      failed.push({
        index: entry.index,
        reason: `items[${entry.index}] \u62F7\u8D1D\u8D44\u6E90\u5931\u8D25\uFF1A${sourcePath}\uFF08${error.message}\uFF09`
      });
      continue;
    }
    entry.item.fileKey = toPosixPath(path4.join(assetsDirName, path4.basename(targetPath)));
    copied.push(targetPath);
    if (entry.item.width === void 0 && entry.item.height === void 0) {
      const dims = await probeMediaDimensions(targetPath);
      if (dims) {
        entry.item.width = dims.width;
        entry.item.height = dims.height;
      } else {
        console.error(
          `[miora-mcp] \u5C3A\u5BF8\u63A2\u6D4B\u5931\u8D25\uFF0C\u8282\u70B9\u56DE\u843D\u9ED8\u8BA4\u5C3A\u5BF8\uFF08\u56FE\u7247\u53EF\u80FD\u53D8\u5F62\uFF09\uFF1A${path4.basename(targetPath)}\uFF08${path4.extname(targetPath).toLowerCase() || "\u65E0\u6269\u5C55\u540D"}\uFF09`
        );
      }
    }
    survivors.push(entry);
  }
  return { copied, survivors, failed };
}
async function readExistingCanvas(filePath, schemaPath) {
  let bytes;
  try {
    bytes = await (0, import_promises5.readFile)(filePath);
  } catch {
    return {
      error: `\u76EE\u6807\u753B\u5E03\u4E0D\u5B58\u5728\uFF1A${filePath}\u3002\u82E5\u8981\u65B0\u5EFA\u8BF7\u7528 action="create"\u3002`,
      code: CANVAS_ERROR_CODES.fileNotFound
    };
  }
  try {
    return { parsed: parseMioraFile(new Uint8Array(bytes), { schemaPath }) };
  } catch (error) {
    const reason = error instanceof MioraFileParseError ? `\uFF08${error.reason}\uFF09` : "";
    return {
      error: `\u8BFB\u53D6\u753B\u5E03\u5931\u8D25${reason}\uFF1A${error.message}`,
      code: CANVAS_ERROR_CODES.parseError
    };
  }
}
async function writeCanvas(params) {
  const { action, filePath, schemaPath, emptyCanvasTemplatePath } = params;
  if (action !== "create" && action !== "update") {
    return fail(
      `action \u5FC5\u987B\u662F "create" \u6216 "update"\uFF0C\u5B9E\u9645\u6536\u5230 ${JSON.stringify(action)}`,
      CANVAS_ERROR_CODES.invalidParams
    );
  }
  if (!filePath || !path4.isAbsolute(filePath)) {
    return fail(`file_path \u5FC5\u987B\u662F\u7EDD\u5BF9\u8DEF\u5F84\uFF0C\u5B9E\u9645\u6536\u5230 ${filePath}`, CANVAS_ERROR_CODES.invalidParams);
  }
  if (path4.extname(filePath).toLowerCase() !== MIORA_EXT) {
    return fail(`file_path \u5FC5\u987B\u4EE5 ${MIORA_EXT} \u7ED3\u5C3E\uFF0C\u5B9E\u9645\u6536\u5230 ${filePath}`, CANVAS_ERROR_CODES.invalidParams);
  }
  const allowedRoots = resolveAllowedCanvasRoots();
  const resolvedFilePath = path4.resolve(filePath);
  const matchedRoot = allowedRoots.find((root) => isPathInside(root, resolvedFilePath));
  if (!matchedRoot) {
    return fail(
      `file_path \u5FC5\u987B\u4F4D\u4E8E\u5F53\u524D\u9879\u76EE\u76EE\u5F55\u6216\u63D2\u4EF6\u6570\u636E\u76EE\u5F55\u5185\uFF0C\u5B9E\u9645\u6536\u5230 ${filePath}`,
      CANVAS_ERROR_CODES.invalidParams
    );
  }
  const hidden = findHiddenSegment(matchedRoot, resolvedFilePath);
  if (hidden) {
    return fail(
      `file_path \u4E0D\u80FD\u4F4D\u4E8E\u9690\u85CF\u76EE\u5F55 ${hidden}/ \u5185\uFF08\u90A3\u662F\u5185\u90E8\u72B6\u6001\u76EE\u5F55\uFF0C\u753B\u5E03\u653E\u90A3\u513F\u7528\u6237\u770B\u4E0D\u5230\uFF09\u3002\u8BF7\u76F4\u63A5\u5199\u5728\u9879\u76EE\u6839\u76EE\u5F55\u4E0B\uFF0C\u4F8B\u5982 <project>/${path4.basename(filePath)}`,
      CANVAS_ERROR_CODES.invalidParams
    );
  }
  const rawItems = params.items;
  if (rawItems !== void 0 && !Array.isArray(rawItems)) {
    return fail("items \u5FC5\u987B\u662F\u6570\u7EC4", CANVAS_ERROR_CODES.invalidParams);
  }
  const removeIds = params.removeItemIds;
  if (removeIds !== void 0 && !Array.isArray(removeIds)) {
    return fail("remove_item_ids \u5FC5\u987B\u662F\u5B57\u7B26\u4E32\u6570\u7EC4", CANVAS_ERROR_CODES.invalidParams);
  }
  const layout = params.layout;
  if (layout !== void 0 && layout !== "grid" && layout !== "row" && layout !== "column") {
    return fail(
      `layout \u5FC5\u987B\u662F grid / row / column \u4E4B\u4E00\uFF0C\u5B9E\u9645\u6536\u5230 ${JSON.stringify(layout)}`,
      CANVAS_ERROR_CODES.invalidParams
    );
  }
  const rawItemList = rawItems ?? [];
  const entries = [];
  const skipped = [];
  for (const [index, raw] of rawItemList.entries()) {
    const normalized = normalizeItem(raw, index);
    if ("error" in normalized) {
      skipped.push({ index, reason: normalized.error });
      continue;
    }
    entries.push({ ...normalized, index });
  }
  let existingWorkflow;
  if (action === "update") {
    const existing = await readExistingCanvas(filePath, schemaPath);
    if (existing.error !== void 0) {
      return fail(existing.error, existing.code);
    }
    existingWorkflow = existing.parsed.workflow;
  }
  const copyResult = await copyAssets(entries, filePath);
  skipped.push(...copyResult.failed);
  const survivors = copyResult.survivors;
  let mergedItems;
  if (action === "update" && existingWorkflow) {
    const removeSet = new Set((removeIds ?? []).filter((id) => typeof id === "string"));
    const incomingById = new Map(
      survivors.filter((entry) => entry.item.id).map((entry) => [entry.item.id, entry.item])
    );
    const kept = [];
    for (const { item } of extractItems(existingWorkflow)) {
      if (!item.id || removeSet.has(item.id)) {
        continue;
      }
      const replacement = incomingById.get(item.id);
      if (replacement) {
        kept.push(replacement);
        incomingById.delete(item.id);
        continue;
      }
      kept.push(item);
    }
    const appended = survivors.map((entry) => entry.item).filter((item) => !item.id || incomingById.has(item.id));
    mergedItems = [...kept, ...appended];
  } else {
    mergedItems = survivors.map((entry) => entry.item);
  }
  if (mergedItems.length === 0 && rawItemList.length > 0) {
    return fail(
      `\u6CA1\u6709\u53EF\u5199\u5165\u7684\u6709\u6548\u6761\u76EE\uFF08\u8DF3\u8FC7 ${skipped.length} \u6761\uFF09\uFF1A${skipped.map((s) => s.reason).join("\uFF1B")}`,
      CANVAS_ERROR_CODES.invalidParams
    );
  }
  const title = optionalString2(params.title) ?? existingWorkflow?.title ?? path4.basename(filePath, MIORA_EXT);
  const isNewEmptyCanvas = action === "create" && mergedItems.length === 0;
  const foreignNodes = existingWorkflow ? extractForeignNodes(existingWorkflow) : [];
  let workflow;
  try {
    workflow = isNewEmptyCanvas ? buildEmptyCanvasFromTemplate(title, BigInt(Date.now()), {
      schemaPath,
      templatePath: emptyCanvasTemplatePath
    }) : buildWorkflow(
      { title, items: mergedItems, layout },
      existingWorkflow,
      foreignNodes
    );
  } catch (error) {
    return fail(`\u6784\u9020\u7A7A\u753B\u5E03\u5931\u8D25\uFF1A${error.message}`, CANVAS_ERROR_CODES.writeError);
  }
  try {
    await (0, import_promises5.writeFile)(filePath, Buffer.from(encodeMioraFile(
      {
        workflow,
        // 资源与 `.miora` 同级（都在 canvasDir 下），故基准目录是 '.'
        assetsBaseDir: "."
      },
      { schemaPath }
    )));
  } catch (error) {
    return fail(`\u5199\u5165\u753B\u5E03\u5931\u8D25\uFF1A${filePath}\uFF08${error.message}\uFF09`, CANVAS_ERROR_CODES.writeError);
  }
  return {
    ok: true,
    filePath,
    title,
    itemCount: mergedItems.length,
    copiedAssets: copyResult.copied,
    skippedItems: skipped
  };
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/telemetry/canvas-telemetry-envelope.ts
var import_node_path = __toESM(require("node:path"));
var MIORA_TELEMETRY_META_KEY = "miora.workbuddy/telemetry";
var CANVAS_TASK_CREATE_EVENT_CODE = "wbx_design_canvas_task_create";
function buildCanvasTelemetryEnvelope(args) {
  const filePath = typeof args.filePath === "string" ? args.filePath : "";
  return {
    eventCode: CANVAS_TASK_CREATE_EVENT_CODE,
    id: filePath ? import_node_path.default.basename(filePath) : "",
    isSuccessful: args.isSuccessful === true
  };
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/tools/write-canvas-tool.ts
var CANVAS_TOOL_NAME = "miora_write_canvas";
var SCHEMA_PATH = import_node_path2.default.join(__dirname, "templates", "ai.kiwi");
var EMPTY_CANVAS_TEMPLATE_PATH = import_node_path2.default.join(__dirname, "templates", "empty-canvas.miora");
var CANVAS_TOOL_DEFINITION = {
  name: CANVAS_TOOL_NAME,
  description: `Create or update a \`.miora\` canvas \u2014 a visual board that arranges images and videos, previewable inside WorkBuddy. Use it when the user wants a moodboard, a visual summary, a storyboard, or any "put these materials on a canvas" request.

Describe WHAT goes on the canvas; node ids, parent links and layout coordinates are generated for you. Items are laid out on a fixed-column grid (4 per row by default) and every image/video node keeps the real pixel size of its asset.

Text is NOT supported on the canvas: never pass type="text" entries. Titles, section names and captions belong in your chat reply, not on the canvas. Invalid entries (text entries, missing files, \u2026) are SKIPPED \u2014 the remaining valid entries are still written, and the result lists what was skipped and why.

- action="create": write a new canvas (overwrites the file if it already exists). Omit \`items\` (or pass an empty array) to create a blank canvas with a single default page \u2014 use this when the user just wants "a new canvas" to fill in themselves.
- action="update": read the existing canvas, then add / replace items by \`id\` and remove those listed in \`remove_item_ids\`. Items you do not mention are kept as-is.

Images and videos are referenced by \`source_path\` (absolute local path). They are COPIED next to the canvas file into a \`<name>_assets/\` folder, so the canvas stays self-contained and portable.

IMPORTANT: this tool only writes the file \u2014 it does NOT make the canvas visible anywhere. After it succeeds you MUST do BOTH of the following with the returned \`file_path\`:
  1. \`present_files\` \u2014 include the \`.miora\` \`file_path\` in \`files\` (FIRST), then every path from the returned \`copied_assets\` so the images/videos also get their own artifact cards. Skipping this leaves the user with NOTHING in the artifact panel. Always deliver \`copied_assets\`, never the original \`resultFiles[].localPath\` from the generation tools: those live in a hidden plugin directory and never become artifacts.
  2. \`${OPEN_TOOL_NAME}\` \u2014 open the canvas in the side panel for immediate viewing. This only opens a panel; it does NOT register any artifact card.`,
  inputSchema: {
    type: "object",
    properties: {
      action: {
        type: "string",
        enum: ["create", "update"],
        description: "create: write a new canvas. update: read the existing canvas and merge changes by item id."
      },
      file_path: {
        type: "string",
        description: "Absolute path of the target .miora file (must end with .miora). Must be inside the current project directory (or the plugin data directory) \u2014 paths outside those roots are rejected. Put it DIRECTLY in the project root, e.g. `<project>/koi-poster.miora`. Do NOT write into dot-directories such as `.workbuddy/`, `.codebuddy/` or `.git/` \u2014 those hold internal state, are hidden from the user in most views, and a canvas placed there is easy to lose. For update the file must already exist."
      },
      title: {
        type: "string",
        description: "Canvas title, also used as the page name. Defaults to the file name."
      },
      items: {
        type: "array",
        description: 'Canvas entries, laid out automatically in the given order. Omit (or pass an empty array) with action="create" to create an EMPTY canvas for the user to fill in themselves. For update these are added, or replace existing entries with the same id.',
        items: {
          type: "object",
          properties: {
            id: {
              type: "string",
              description: "Stable identifier used by update to locate this entry. Auto-generated when omitted."
            },
            type: {
              type: "string",
              enum: ["image", "video"],
              description: "Entry kind. Images and videos only \u2014 text entries are skipped."
            },
            source_path: {
              type: "string",
              description: "Required: absolute local path of the media file. The file is copied next to the canvas."
            },
            title: { type: "string", description: "Node title shown in the layer list." },
            width: {
              type: "number",
              description: "Node width in px. Omit for image/video: the real pixel size of the asset is detected automatically."
            },
            height: {
              type: "number",
              description: "Node height in px. Omit for image/video: the real pixel size of the asset is detected automatically."
            },
            x: { type: "number", description: "Absolute x. Omit to use automatic layout." },
            y: { type: "number", description: "Absolute y. Omit to use automatic layout." }
          },
          required: ["type"]
        }
      },
      remove_item_ids: {
        type: "array",
        items: { type: "string" },
        description: "update only: ids of entries to delete from the canvas."
      },
      layout: {
        type: "string",
        enum: ["grid", "row", "column"],
        description: "Automatic layout strategy. grid (default): 4 per row, columns aligned to the widest item / row: single row / column: single column. Nodes always keep the real pixel size of each asset."
      },
      explanation: {
        type: "string",
        description: "Brief explanation of what is being produced and why."
      }
    },
    required: ["action", "file_path"]
  }
};
function createWriteCanvasTool() {
  return {
    definition: CANVAS_TOOL_DEFINITION,
    async handler(args) {
      const action = args.action === "update" ? "update" : "create";
      const requestedFilePath = optionalString(args.file_path);
      const result = await writeCanvas({
        action,
        filePath: requestedFilePath,
        title: typeof args.title === "string" ? args.title : void 0,
        items: args.items,
        removeItemIds: args.remove_item_ids,
        layout: args.layout,
        schemaPath: SCHEMA_PATH,
        emptyCanvasTemplatePath: EMPTY_CANVAS_TEMPLATE_PATH
      });
      if (!result.ok) {
        const failureEnvelope = buildCanvasTelemetryEnvelope({
          // writeCanvas 失败时不回传 filePath（可能正是它非法），用入参兜底；
          // 信封只取 basename，非法路径也不会把目录结构带进埋点。
          filePath: requestedFilePath,
          isSuccessful: false
        });
        return {
          isError: true,
          content: [{
            type: "text",
            text: JSON.stringify({
              type: "miora_canvas_result",
              ok: false,
              error: result.error,
              tool_error_code: result.code,
              telemetry: failureEnvelope
            })
          }],
          _meta: {
            // 与 agent-cli ToolErrorCode 的 19930-19939 段对齐，供上游聚合。
            // **不可改名/挪走**：JSON 里的同名字段只是给模型看的冗余镜像。
            tool_error_code: result.code,
            [MIORA_TELEMETRY_META_KEY]: failureEnvelope
          }
        };
      }
      setCurrentCanvasPath(result.filePath);
      const successEnvelope = buildCanvasTelemetryEnvelope({
        filePath: result.filePath,
        isSuccessful: true
      });
      const skipped = result.skippedItems;
      const skippedNote = skipped.length > 0 ? ` ${skipped.length} item(s) were SKIPPED and are NOT on the canvas: ` + skipped.map((s) => s.reason).join("; ") + ". Tell the user these entries were left out \u2014 do not claim they were delivered." : "";
      return {
        content: [{
          type: "text",
          text: JSON.stringify({
            type: "miora_canvas_result",
            action,
            file_path: result.filePath,
            title: result.title,
            item_count: result.itemCount,
            copied_assets: result.copiedAssets,
            skipped_items: skipped.length > 0 ? skipped : void 0,
            explanation: typeof args.explanation === "string" ? args.explanation : void 0,
            // 这条 message 是 LLM 实际最听的一句（实测：它每次都照做了里面
            // 提到的 open_canvas）。所以两个必做动作都要写进来 ——
            // 早期只写 open_canvas，结果画布从未被传给 present_files，
            // 产物栏里一张卡都没有。
            message: `Canvas ${action === "create" ? "created" : "updated"} with ${result.itemCount} item(s).` + skippedNote + ` Now do BOTH: (1) call \`present_files\` with this \`file_path\` FIRST in \`files\`, followed by every path in \`copied_assets\` (required \u2014 the canvas and the images/videos each need their own artifact card, and \`copied_assets\` are the only paths that work: the original download paths from the generation tools live in a hidden plugin directory and never show up as artifacts); (2) call ${OPEN_TOOL_NAME} with this \`file_path\` to open it in the side panel (this opens a panel only, it registers no artifact).`,
            // 埋点信封。挂在结果 JSON 里是**必达通道**：agent-cli 会把本段文本
            // `JSON.parse` 成 ACP `rawOutput`，renderer 侧再原样当作
            // `toolResult.structuredContent`。模型会忽略这个字段（它只按
            // `message` 行动），宿主则据此上报运营事件。
            telemetry: successEnvelope
          })
        }],
        structuredContent: {
          file_path: result.filePath,
          title: result.title,
          item_count: result.itemCount
        },
        // 双写第二处。`_meta` 是**不确定通道** —— agent-cli 当前构造
        // `tool_call_update._meta` 时并不透传 MCP CallToolResult 自带的 `_meta`，
        // 只有 `rawResponse` 恰好携带时才通得过去。留着它是为了「日后 agent-cli
        // 补齐透传后，宿主自动走更干净的通道」，两侧都不用改。
        _meta: { [MIORA_TELEMETRY_META_KEY]: successEnvelope }
      };
    }
  };
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/viewer.ts
var import_node_path3 = __toESM(require("node:path"));
var UI_MIME = "text/html;profile=mcp-app";
var VIEWER_URI = "ui://miora/viewer.html";
var VIEWER_SRC_PLACEHOLDER = "__WB_LOCAL_FILE_VIEWER_SRC__";
var PLUGIN_ROOT = process.env.CODEBUDDY_PLUGIN_ROOT || import_node_path3.default.join(__dirname, "..");
var VIEWER_ENTRY = import_node_path3.default.join(PLUGIN_ROOT, "assets", "miora-viewer.html");
var UI_RESOURCE_META = {
  ui: {
    csp: {
      frameDomains: ["http://127.0.0.1:*"],
      connectDomains: ["http://127.0.0.1:*"],
      resourceDomains: ["http://127.0.0.1:*"]
    },
    permissions: {},
    prefersBorder: false
  },
  workbuddy: {
    ui: {
      localFileViewer: {
        placeholder: VIEWER_SRC_PLACEHOLDER,
        inputField: "file_path",
        extension: ".miora",
        viewerEntry: VIEWER_ENTRY
      }
    }
  }
};
function escapeText(value) {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}
function buildShellHtml() {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>Miora Canvas</title>
<style>
html,body{margin:0;padding:0;width:100%;height:100%;overflow:hidden;background:transparent}
#viewer{display:block;width:100%;height:100%;border:0}
</style>
</head>
<body>
<iframe id="viewer"
        src="${VIEWER_SRC_PLACEHOLDER}"
        sandbox="allow-scripts allow-same-origin"
        allow="fullscreen"></iframe>
<script>
(function () {
  // \u62A2\u5148\u81EA\u62A5\u9AD8\u5EA6\uFF0C\u963B\u6B62\u5BBF\u4E3B\u628A\u6EE1\u5C4F\u753B\u5E03\u6309\u300C\u5185\u5BB9\u9AD8\u5EA6\u300D\u538B\u6241\uFF08\u8BE6\u89C1\u672C\u6587\u4EF6\u9876\u90E8\u6CE8\u91CA\uFF09\u3002
  function report() {
    var h = window.innerHeight || document.documentElement.clientHeight || 600;
    parent.postMessage({
      jsonrpc: '2.0',
      method: 'ui/notifications/size-changed',
      params: { height: h }
    }, '*');
  }
  report();
  window.addEventListener('resize', report);
})();
</script>
</body>
</html>`;
}
function buildErrorHtml(message) {
  return `<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>Miora Canvas</title>
<style>
:root{color-scheme:light dark}
html,body{margin:0;padding:0;height:100%}
body{display:flex;align-items:center;justify-content:center;
  font:14px/1.6 system-ui,-apple-system,sans-serif;
  color:light-dark(#59606e,#9aa3b2);background:transparent;padding:24px;text-align:center}
code{font-size:12px;opacity:.85;word-break:break-all}
</style>
</head>
<body><div><p>\u65E0\u6CD5\u6253\u5F00 Miora \u753B\u5E03</p><p><code>${escapeText(message)}</code></p></div></body>
</html>`;
}

// resources/plugins/workbuddy-builtin/mcps/miora-mcp/src/cli.ts
var TAG = "[miora-mcp]";
var SUPPORTED_PROTOCOL_VERSIONS = [
  "2025-11-25",
  "2025-06-18",
  "2025-03-26",
  "2024-11-05",
  "2024-10-07"
];
var LATEST_PROTOCOL_VERSION = SUPPORTED_PROTOCOL_VERSIONS[0];
function negotiateProtocolVersion(requested) {
  return typeof requested === "string" && SUPPORTED_PROTOCOL_VERSIONS.includes(requested) ? requested : LATEST_PROTOCOL_VERSION;
}
var DEV_MIORA_ENDPOINT_FALLBACK = "https://public.miora.qq.com";
function buildMediaTools() {
  const explicitEndpoint = process.env.MIORA_ENDPOINT?.trim();
  const mioraEndpoint = explicitEndpoint || DEV_MIORA_ENDPOINT_FALLBACK;
  if (!explicitEndpoint) {
    logError(`MIORA_ENDPOINT \u672A\u914D\u7F6E\uFF0C\u8054\u8C03\u671F\u4E34\u65F6\u515C\u5E95\u5230 ${DEV_MIORA_ENDPOINT_FALLBACK}\uFF08\u4E0A\u7EBF\u524D\u9700\u79FB\u9664\u8BE5 fallback\uFF09`);
  }
  const extraCookie = process.env.MIORA_EXTRA_COOKIE?.trim();
  const deps = {
    mioraEndpoint,
    ...extraCookie ? { defaultHeaders: { cookie: extraCookie } } : {}
  };
  return [
    createTextToImageTool(deps),
    createEditImageTool(deps),
    createTextToVideoTool(deps),
    createFrameToVideoTool(deps),
    createReferenceToVideoTool(deps),
    // 任务查询（超时/失败后的恢复入口）：与媒体 tool 共享 endpoint 与联调 cookie。
    createQueryTaskTool(deps)
  ];
}
var TOOLS = [
  createWriteCanvasTool(),
  createOpenCanvasTool(),
  ...buildMediaTools()
];
var TOOL_BY_NAME = new Map(TOOLS.map((tool) => [tool.definition.name, tool]));
function logError(message, detail) {
  console.error(`${TAG} ${message}${detail === void 0 ? "" : ` ${String(detail)}`}`);
}
function jsonRpcResult(id, result) {
  return { jsonrpc: "2.0", id, result };
}
function jsonRpcError(id, code, message) {
  return { jsonrpc: "2.0", id, error: { code, message } };
}
function extractProgressToken(params) {
  const meta = params?._meta;
  if (!meta || typeof meta !== "object" || Array.isArray(meta)) {
    return void 0;
  }
  const token = meta.progressToken;
  return typeof token === "string" || typeof token === "number" ? token : void 0;
}
function readViewerResource() {
  const canvasPath = getCurrentCanvasPath();
  const text = canvasPath ? buildShellHtml() : buildErrorHtml(`\u5C1A\u672A\u6307\u5B9A\u753B\u5E03\u6587\u4EF6\uFF0C\u8BF7\u5148\u8C03\u7528 ${OPEN_TOOL_NAME}\u3002`);
  return {
    contents: [{ uri: VIEWER_URI, mimeType: UI_MIME, text, _meta: UI_RESOURCE_META }]
  };
}
async function handleRpc(message) {
  const { id, method, params } = message;
  const isNotification = id === void 0 || id === null;
  switch (method) {
    case "initialize":
      return jsonRpcResult(id, {
        protocolVersion: negotiateProtocolVersion(params?.protocolVersion),
        capabilities: { tools: {}, resources: {} },
        serverInfo: { name: "miora", version: "0.1.0" }
      });
    case "notifications/initialized":
    case "initialized":
      return void 0;
    case "ping":
      return isNotification ? void 0 : jsonRpcResult(id, {});
    case "tools/list":
      return jsonRpcResult(id, { tools: TOOLS.map((tool) => tool.definition) });
    case "resources/list":
      return jsonRpcResult(id, {
        resources: [{ uri: VIEWER_URI, name: "miora-canvas-viewer", mimeType: UI_MIME }]
      });
    case "tools/call": {
      const name = params?.name;
      const tool = typeof name === "string" ? TOOL_BY_NAME.get(name) : void 0;
      if (!tool) {
        return jsonRpcError(id, -32602, `Unknown tool: ${String(name)}`);
      }
      const args = params?.arguments ?? {};
      const progressToken = extractProgressToken(params);
      const context = progressToken === void 0 ? void 0 : { reportProgress: createProgressReporter(progressToken) };
      return jsonRpcResult(id, await tool.handler(args, context));
    }
    case "resources/read": {
      const uri = params?.uri;
      if (uri !== VIEWER_URI) {
        return jsonRpcError(id, -32602, `Resource not found: ${String(uri)}`);
      }
      return jsonRpcResult(id, readViewerResource());
    }
    default:
      return isNotification ? void 0 : jsonRpcError(id, -32601, `Method not found: ${String(method)}`);
  }
}
function send(payload) {
  process.stdout.write(`${JSON.stringify(payload)}
`);
}
function createProgressReporter(progressToken) {
  return (progress, total, message) => {
    const notification = {
      jsonrpc: "2.0",
      method: "notifications/progress",
      params: {
        progressToken,
        progress,
        ...total === void 0 ? {} : { total },
        ...message === void 0 ? {} : { message }
      }
    };
    try {
      process.stdout.write(`${JSON.stringify(notification)}
`);
    } catch (error) {
      logError("progress notification failed:", error);
    }
  };
}
async function dispatch(parsed) {
  const batch = Array.isArray(parsed) ? parsed : [parsed];
  const responses = [];
  for (const message of batch) {
    if (!message || typeof message !== "object") {
      responses.push(jsonRpcError(null, -32600, "Invalid Request"));
      continue;
    }
    try {
      const response = await handleRpc(message);
      if (response) {
        responses.push(response);
      }
    } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      logError(`method ${String(message.method)} failed:`, detail);
      if (message.id !== void 0 && message.id !== null) {
        responses.push(jsonRpcError(message.id, -32603, detail));
      }
    }
  }
  if (responses.length === 0) {
    return;
  }
  send(Array.isArray(parsed) ? responses : responses[0]);
}
var lanes = createDispatchLanes(dispatch, logError, /* @__PURE__ */ new Set([CANVAS_TOOL_NAME, OPEN_TOOL_NAME]));
var buffer = "";
process.stdin.setEncoding("utf8");
process.stdin.on("data", (chunk) => {
  buffer += chunk;
  let newline = buffer.indexOf("\n");
  while (newline >= 0) {
    const line = buffer.slice(0, newline).trim();
    buffer = buffer.slice(newline + 1);
    newline = buffer.indexOf("\n");
    if (!line) {
      continue;
    }
    let parsed;
    try {
      parsed = JSON.parse(line);
    } catch {
      send(jsonRpcError(null, -32700, "Parse error"));
      continue;
    }
    lanes.enqueue(parsed);
  }
});
process.stdin.on("end", () => {
  lanes.drain().then(() => process.exit(0)).catch(() => process.exit(0));
});
process.stdin.resume();
for (const signal of ["SIGTERM", "SIGINT", "SIGHUP"]) {
  process.on(signal, () => process.exit(0));
}
/*! Bundled license information:

mime-db/index.js:
  (*!
   * mime-db
   * Copyright(c) 2014 Jonathan Ong
   * Copyright(c) 2015-2022 Douglas Christopher Wilson
   * MIT Licensed
   *)

mime-types/index.js:
  (*!
   * mime-types
   * Copyright(c) 2014 Jonathan Ong
   * Copyright(c) 2015 Douglas Christopher Wilson
   * MIT Licensed
   *)
*/
