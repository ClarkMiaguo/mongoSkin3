/*!
 * mongoskin - collection.js
 *
 * Copyright(c) 2011 - 2012 kissjs.org
 * Copyright(c) 2012 fengmk2 <fengmk2@gmail.com>
 * MIT Licensed
 */

"use strict";

/**
 * Module dependencies.
 */
var __slice = Array.prototype.slice;
var Collection = require('mongodb').Collection;
var Cursor = require('mongodb').Cursor;
var SkinCursor = require('./cursor').SkinCursor;
var helper = require('./helper');
var utils = require('./utils');

/**
 * Constructor
 */
var SkinCollection = exports.SkinCollection = utils.makeSkinClass(Collection);

/**
 * bind extend functions to collection
 *
 * e.g.
 *
 * db.bind('article').bind({
 *   getPostByAuthor: function(id, callback) {
 *      this.findOne({author_id: id}, callback);
 *   }
 * });
 *
 */
SkinCollection.prototype.bind = function(extendObject) {
  for(var key in extendObject) {
    if(typeof extendObject[key] == 'function') {
      this[key] = extendObject[key].bind(this);
    } else {
      this[key] = extendObject[key];
    }
  }
}

SkinCollection.prototype._open = function(callback) {
  var collection_args = this._collection_args.concat([callback]);
  this._skin_db.open(function(err, db) {
      if(err) return callback(err);
      db.collection.apply(db, collection_args);
  });
}

/*
 * find is a special method, because it could return a SkinCursor instance
 */
SkinCollection.prototype._find = SkinCollection.prototype.find;

/**
 * same args as find, but use Array as callback result but not use Cursor
 *
 * findItems(args, function (err, items) {});
 *
 * same as
 *
 * find(args).toArray(function (err, items) {});
 *
 * or using `mongodb.collection.find()`
 *
 * find(args, function (err, cursor) {
 *   cursor.toArray(function (err, items) {
 *   });
 * });
 *
 * @param {Object} [query]
 * @param {Object} [options]
 * @param {Function(err, docs)} callback
 * @return {SkinCollection} this
 * @api public
 */
SkinCollection.prototype.findItems = function (query, options, callback) {
  var args = __slice.call(arguments);
  var fn = args[args.length - 1];
  args[args.length - 1] = function (err, cursor) {
    if (err) {
      return fn(err);
    }
    cursor.toArray(fn);
  };
  this.find.apply(this, args);
  return this;
};

/**
 * find and cursor.each(fn).
 *
 * @param {Object} [query]
 * @param {Object} [options]
 * @param {Function(err, item)} eachCallback
 * @return {SkinCollection} this
 * @api public
 */
SkinCollection.prototype.findEach = function (query, options, eachCallback) {
  var args = __slice.call(arguments);
  var fn = args[args.length - 1];
  args[args.length - 1] = function (err, cursor) {
    if (err) {
      return fn(err);
    }
    cursor.each(fn);
  };
  this.find.apply(this, args);
  return this;
};

/**
 * Operate by object.`_id`
 *
 * @param {String} methodName
 * @param {String|ObjectID|Number} id
 * @param {Arguments|Array} args
 * @return {SkinCollection} this
 * @api private
 */
SkinCollection.prototype._operateById = function (methodName, id, args) {
  args = __slice.call(args);
  args[0] = {_id: helper.toObjectID(id)};
  this[methodName].apply(this, args);
  return this;
};

/**
 * Find one object by _id.
 *
 * @param {String|ObjectID|Number} id, doc primary key `_id`
 * @param {Function(err, doc)} callback
 * @return {SkinCollection} this
 * @api public
 */
SkinCollection.prototype.findById = function (id, callback) {
  return this._operateById('findOne', id, arguments);
};

/**
 * Update doc by _id.
 * @param {String|ObjectID|Number} id, doc primary key `_id`
 * @param {Object} doc
 * @param {Function(err)} callback
 * @return {SkinCollection} this
 * @api public
 */
SkinCollection.prototype.updateById = function (id, doc, callback) {
  var oldCb = callback;
  var _this = this;
  if (callback) {
    callback = function(error, res) {
      oldCb.call(_this, error, !!res ? res.result : null);
    };
  }
  return this._operateById('update', id, [id, doc, callback]);
};

/**
 * Remove doc by _id.
 * @param {String|ObjectID|Number} id, doc primary key `_id`
 * @param {Function(err)} callback
 * @return {SkinCollection} this
 * @api public
 */
SkinCollection.prototype.removeById = function (id, callback) {
  var oldCb = callback;
  var _this = this;
  if (callback) {
    callback = function(error, res) {
      oldCb.call(_this, error, !!res ? res.result.n : null);
    };
  }
  return this._operateById('remove', id, [id, callback]);
};

/**
 * Creates a cursor for a query that can be used to iterate over results from MongoDB.
 *
 * @param {Object} query
 * @param {Object} options
 * @param {Function(err, docs)} callback
 * @return {SkinCursor|SkinCollection} if last argument is not a function, then returns a SkinCursor,
 *   otherise return this
 * @api public
 */
SkinCollection.prototype.find = function (query, options, callback) {
  var args = createFindCmd(__slice.call(arguments));
  if(this.isOpen()) {
    return this._native.find.apply(this._native, args);
  }
  if (args.length > 0 && typeof args[args.length - 1] === 'function') {
    this._find.apply(this, args);
    return this;
  } else {
    var cursor = new SkinCursor();
    cursor._skin_collection = this;
    cursor._find_args = args;
    return cursor;
  }
};

const testForFields = {
  limit: 1, sort: 1, fields:1, skip: 1, hint: 1, explain: 1, snapshot: 1, timeout: 1, tailable: 1, tailableRetryInterval: 1
  , numberOfRetries: 1, awaitdata: 1, awaitData: 1, exhaust: 1, batchSize: 1, returnKey: 1, maxScan: 1, min: 1, max: 1, showDiskLoc: 1
  , comment: 1, raw: 1, readPreference: 1, partial: 1, read: 1, dbName: 1, oplogReplay: 1, connection: 1, maxTimeMS: 1, transforms: 1
  , collation: 1, noCursorTimeout: 1
}

function _createFindCmd(args) {
  if (args.length <= 1) {
    return args;
  } else if (args.length >= 3) {
    const options = Object.assign({}, {projection: args[1]}, args[2]);
    return [args[0], options]
  }
  // args.length === 2
  for (const key in args[1]) {
    if (testForFields[key]) {
      return args;
    }
  }
  return [args[0], {projection: args[1]}]
}

function createFindCmd(args) {
  if (!Array.isArray(args) || args.length == 0) {
    return args;
  }

  let callback;
  if (typeof args[args.length - 1] === 'function') {
    callback = args.pop();
  }
  const newArgs = _createFindCmd(args);
  if (callback) {
    newArgs.push(callback);
  }
  return newArgs;
}

SkinCollection.prototype._aggregate = SkinCollection.prototype.aggregate;

SkinCollection.prototype.aggregate = function(...args) {
    if (!Array.isArray(args) || typeof args[args.length - 1] !== 'function') {
        return this._aggregate.apply(this, args);
    }
    const callback = args.pop();
    args.push((err, cursor) => {
      if (err) {
          return callback(err);
      }
      if (cursor instanceof Cursor) {
          return cursor.toArray(callback);
      }
      return callback(null, cursor);
    });
    return this._aggregate.apply(this, args);
}

SkinCollection.prototype._findOne = SkinCollection.prototype.findOne;

SkinCollection.prototype.findOne = function(...args) {
  var args = createFindCmd(__slice.call(arguments));
  return this._findOne.apply(this, args)
}

SkinCollection.prototype._insertOne = SkinCollection.prototype.insertOne;

SkinCollection.prototype.insertOne = function(doc, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = {};
  };
  options.checkKeys = false;
  return this._insertOne.call(this, doc, options, callback)
}