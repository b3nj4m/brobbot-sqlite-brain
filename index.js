var Q = require('q');
var sqlite = require('sqlite3');
var brobbot = require('brobbot');
var Brain = brobbot.Brain;
var User = brobbot.User;
var _ = require('lodash');
var msgpack = require('msgpack');

function SqliteBrain(robot, useMsgpack) {
  Brain.call(this, robot);

  this.robot = robot;

  this.useMsgpack = useMsgpack === undefined ? true : useMsgpack;

  this.prefix = process.env.BROBBOT_SQLITE_DATA_PREFIX || 'data';
  this.prefixRegex = new RegExp("^" + this.prefix + ":");

  this.dbName = process.env.BROBBOT_SQLITE_DB_NAME || 'brobbot';
  this.tableName = process.env.BROBBOT_SQLITE_TABLE_NAME || 'brobbot';

  try {
    this.client = new sqlite.Database(this.dbName);
    this.robot.logger.info("Successfully connected to pg");
  }
  catch (err) {
    this.robot.logger.error("Failed to connect to pg: " + err);
  }

  this.ready = this.initTable();
}

SqliteBrain.prototype = Object.create(Brain);
SqliteBrain.prototype.constructor = SqliteBrain;

SqliteBrain.prototype.initTable = function() {
  var query = "CREATE TABLE IF NOT EXISTS " + this.tableName + " (key, subkey default NULL, isset default 0, value, UNIQUE (key, subkey) ON CONFLICT ABORT);";
  query += "CREATE INDEX IF NOT EXISTS brobbot-index-1 ON " + this.tableName + " (key, subkey, isset);";
  return Q.ninvoke(this.client, 'run', query);
};

SqliteBrain.prototype.transaction = function(fn) {
  return this.currentTransaction = this.currentTransaction.then(this.runTransaction.bind(this, fn));
};

SqliteBrain.prototype.runTransaction = function(fn) {
  var self = this;

  return this.query("BEGIN").then(fn).then(function(result) {
    return self.query("COMMIT").then(function() {
      return result;
    });
  }).fail(function(err) {
    return self.query("COMMIT").then(function() {
      throw err;
    });
  });
};

SqliteBrain.prototype.query = function(query, params) {
  var self = this;

  return this.ready.then(function() {
    return Q.ninvoke(self.client, 'all', query, params).then(function(results) {
      return results === undefined ? [] : results;
    }).fail(function(err) {
      self.robot.logger.error('SQLite error:', err.stack);
      return null;
    });
  });
};

SqliteBrain.prototype.updateValue = function(key, value, isSet) {
  var self = this;

  if (isSet === undefined) {
    isSet = false;
  }

  return this.keyExists(key).then(function(exists) {
    value = self.serialize(value);
    if (exists) {
      return self.query("UPDATE " + self.tableName + " SET value = ? WHERE key = ?", [value, key]);
    }
    else {
      return self.query("INSERT INTO " + self.tableName + " (key, value, isset) VALUES (?, ?, ?)", [key, value, isSet]);
    }
  });
};

SqliteBrain.prototype.updateSubValue = function(key, subkey, value) {
  var self = this;

  value = this.serialize(value);

  return this.subkeyExists(key, subkey).then(function(exists) {
    if (exists) {
      return self.query("UPDATE " + self.tableName + " SET value = ? WHERE key = ? AND subkey = ?", [value, key, subkey]);
    }
    else {
      return self.query("INSERT INTO " + self.tableName + " (key, value, subkey) VALUES (?, ?, ?)", [key, value, subkey]);
    }
  });
};

SqliteBrain.prototype.getValues = function(key, subkey) {
  var self = this;

  var params = [key];
  var subkeyPart = "";

  if (subkey !== undefined) {
    subkeyPart = "AND subkey = ?";
    params.push(subkey);
  }

  return this.query("SELECT value FROM " + this.tableName + " WHERE key = ? " + subkeyPart, params).then(function(results) {
    return _.map(results, function(result) {
      return self.deserialize(result.value);
    });
  });
};

SqliteBrain.prototype.reset = function() {
  return this.query("DELETE FROM " + this.tableName).then(function() {
    return Q();
  });
};

SqliteBrain.prototype.llen = function(key) {
  var self = this;

  return this.lgetall(key).then(function(values) {
    return values ? values.length : null;
  });
};

SqliteBrain.prototype.lset = function(key, index, value) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      values = values || [];
      values[index] = value;
      return self.updateValue(self.key(key), values, false);
    });
  });
};

SqliteBrain.prototype.lfindindex = function(values, findValue) {
  var self = this;

  return _.findIndex(values, function(value) {
    return value === findValue;
  });
};

SqliteBrain.prototype.linsert = function(key, placement, pivot, value) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      values = values || [];

      var idx = self.lfindindex(values, pivot);

      if (idx === -1) {
        return -1;
      }

      if (placement === 'AFTER') {
        idx = idx + 1;
      }

      values.splice(idx, 0, value);

      return self.updateValue(self.key(key), values, false);
    });
  });
};

SqliteBrain.prototype.lpush = function(key, value) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      values = values || [];
      values.unshift(value);
      return self.updateValue(self.key(key), values, false);
    });
  });
};

SqliteBrain.prototype.rpush = function(key, value) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      values = values || [];
      values.push(value);
      return self.updateValue(self.key(key), values, false);
    });
  });
};

SqliteBrain.prototype.lpop = function(key) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      if (values) {
        var value = values.shift();
        return self.updateValue(self.key(key), values, false).then(function() {
          return value;
        });
      }
      else {
        return null;
      }
    });
  });
};

SqliteBrain.prototype.rpop = function(key) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      if (values) {
        var value = values.pop();
        return self.updateValue(self.key(key), values, false).then(function() {
          return value;
        });
      }
      else {
        return null;
      }
    });
  });
};

SqliteBrain.prototype.lindex = function(key, index) {
  var self = this;

  return this.lgetall(key).then(function(values) {
    return values ? values[index] : null;
  });
};

SqliteBrain.prototype.lgetall = function(key) {
  return this.getValues(this.key(key)).then(function(results) {
    return results && results.length > 0 ? results[0] : null;
  });
};

SqliteBrain.prototype.lrange = function(key, start, end) {
  return this.lgetall(key).then(function(values) {
    return values ? values.slice(start, end + 1) : null;
  });
};

SqliteBrain.prototype.lrem = function(key, value) {
  var self = this;

  return this.transaction(function() {
    return self.lgetall(key).then(function(values) {
      if (values) {
        var newValues = _.without(values, value);

        return self.updateValue(self.key(key), newValues, false).then(function() {
          return values.length - newValues.length;
        });
      }
      return 0;
    });
  });
};

SqliteBrain.prototype.sadd = function(key, value) {
  var self = this;

  return this.transaction(function() {
    return self.sismember(key, value).then(function(isMemeber) {
      if (isMemeber) {
        return -1;
      }
      return self.lgetall(key).then(function(values) {
        values = values || [];
        values.push(value);
        return self.updateValue(self.key(key), values, true);
      });
    });
  });
};

SqliteBrain.prototype.sismember = function(key, value) {
  return this.smembers(key, value).then(function(values) {
    return values ? _.contains(values, value) : false;
  });
};

SqliteBrain.prototype.srem = function(key, value) {
  return this.lrem(key, value);
};

SqliteBrain.prototype.scard = function(key) {
  return this.llen(key);
};

SqliteBrain.prototype.spop = function(key) {
  return this.rpop(key);
};

SqliteBrain.prototype.srandmember = function(key) {
  var self = this;

  return this.smembers(key).then(function(values) {
    return values && values.length > 0 ? values[_.random(values.length - 1)] : null;
  });
};

SqliteBrain.prototype.smembers = function(key) {
  return this.lgetall(key);
};

SqliteBrain.prototype.keys = function(searchKey) {
  var self = this;

  if (searchKey === undefined) {
    searchKey = '';
  }

  searchKey = this.key(searchKey);

  return this.query("SELECT DISTINCT key FROM " + this.tableName + " WHERE key LIKE ?", [searchKey + "%"]).then(function(results) {
    return _.map(results, function(result) {
      return self.unkey(result.key);
    });
  });
};

SqliteBrain.prototype.type = function(key) {
  return this.query("SELECT (CASE WHEN isset THEN 'set' WHEN value LIKE '[%' THEN 'list' WHEN subkey IS NOT NULL THEN 'hash' ELSE 'object' END) AS type FROM " + this.tableName + " WHERE key = ? LIMIT 1", [this.key(key)]).then(function(results) {
    return results.length > 0 ? results[0].type : null;
  });
};

SqliteBrain.prototype.types = function(keys) {
  return Q.all(_.map(keys, this.type.bind(this)));
};

SqliteBrain.prototype.unkey = function(key) {
  return key.replace(this.prefixRegex, '');
};

SqliteBrain.prototype.key = function(key) {
  return this.prefix + ":" + key;
};

SqliteBrain.prototype.usersKey = function() {
  return "users";
};

SqliteBrain.prototype.subkeyExists = function(table, key) {
  return this.query("SELECT 1 FROM " + this.tableName + " WHERE key = ? AND subkey = ? LIMIT 1", [table, key]).then(function(results) {
    return results.length > 0;
  });
};

SqliteBrain.prototype.keyExists = function(key) {
  return this.query("SELECT 1 FROM " + this.tableName + " WHERE key = ? LIMIT 1", [key]).then(function(results) {
    return results.length > 0;
  });
};

SqliteBrain.prototype.exists = function(key) {
  return this.keyExists(this.key(key));
};

SqliteBrain.prototype.get = function(key) {
  return this.getValues(this.key(key)).then(function(results) {
    return results.length > 0 ? results[0] : null;
  });
};

SqliteBrain.prototype.set = function(key, value) {
  return this.updateValue(this.key(key), value);
};

SqliteBrain.prototype.remove = function(key) {
  return this.query("DELETE FROM " + this.tableName + " WHERE key = ?", [this.key(key)]);
};

SqliteBrain.prototype.incrby = function(key, num) {
  var self = this;

  return this.transaction(function() {
    return self.get(key).then(function(val) {
      key = self.key(key);

      if (val !== null) {
        num = val + num;
      }
      return self.updateValue(key, num).then(_.constant(num));
    });
  });
};

SqliteBrain.prototype.hkeys = function(table) {
  return this.query("SELECT subkey FROM " + this.tableName + " WHERE key = ?", [this.key(table)]).then(function(results) {
    return _.map(results, function(result) {
      return result.subkey;
    });
  });
};

SqliteBrain.prototype.hvals = function(table) {
  return this.getValues(this.key(table));
};

SqliteBrain.prototype.hlen = function(table) {
  return this.query("SELECT COUNT(*) AS count FROM " + this.tableName + " WHERE key = ? GROUP BY key", [this.key(table)]).then(function(results) {
    return results.length > 0 ? parseInt(results[0].count) : null;
  });
};

SqliteBrain.prototype.hset = function(table, key, value) {
  return this.updateSubValue(this.key(table), key, value);
};

SqliteBrain.prototype.hget = function(table, key) {
  return this.getValues(this.key(table), key).then(function(results) {
    return results.length > 0 ? results[0] : null;
  });
};

SqliteBrain.prototype.hdel = function(table, key) {
  return this.query("DELETE FROM " + this.tableName + " WHERE key = ? AND subkey = ?", [this.key(table), key]);
};

SqliteBrain.prototype.hgetall = function(table) {
  var self = this;

  return this.query("SELECT subkey, value FROM " + this.tableName + " WHERE key = ?", [this.key(table)]).then(function(results) {
    var map = new Map();

    _.each(results, function(result) {
      return map.set(result.subkey, self.deserialize(result.value));
    });

    return map;
  });
};

SqliteBrain.prototype.hincrby = function(table, key, num) {
  var self = this;

  return this.transaction(function() {
    return self.hget(table, key).then(function(val) {
      table = self.key(table);

      if (val !== null) {
        num = val + num;
      }
      return this.updateSubValue(table, key, num).then(_.constant(num));
    });
  });
};

SqliteBrain.prototype.close = function() {
  return this.client.close();
};

SqliteBrain.prototype.serialize = function(value) {
  if (this.useMsgpack) {
    if (_.isObject(value)) {
      return msgpack.pack(value);
    }
    return value.toString();
  }

  return JSON.stringify(value);
};

SqliteBrain.prototype.deserialize = function(value) {
  if (value === undefined || value === null) {
    return null;
  }

  if (this.useMsgpack) {
    var result;

    try {
      result = msgpack.unpack(value);
    }
    catch (err) {
      console.error('SQLite error deserializing data:', err.stack);

      result = value.toString();
    }

    return result;
  }
  return JSON.parse(value.toString());
};

SqliteBrain.prototype.serializeUser = function(user) {
  return this.serialize(user);
};

SqliteBrain.prototype.deserializeUser = function(obj) {
  if (obj) {
    obj = this.deserialize(obj);
    if (obj && obj.id) {
      return new User(obj.id, obj);
    }
  }
  return null;
};

SqliteBrain.prototype.users = function() {
  var self = this;

  return this.getValues(this.usersKey()).then(function(results) {
    return _.map(results, function(result) {
      return self.deserializeUser(result.value);
    });
  });
};

SqliteBrain.prototype.addUser = function(user) {
  return this.updateSubValue(this.usersKey(), user.id, user);
};

SqliteBrain.prototype.userForId = function(id, options) {
  var self = this;

  return this.getValues(this.usersKey(), id).then(function(results) {
    var user = results[0];

    if (user) {
      user = self.deserializeUser(user);
    }

    if (!user || (options && options.room && (user.room !== options.room))) {
      return self.addUser(new User(id, options));
    }

    return user;
  });
};

SqliteBrain.prototype.userForName = function(name) {
  var self = this;

  name = name && name.toLowerCase() || '';

  return this.users().then(function(users) {
    return _.find(users, function(user) {
      return user.name && user.name.toLowerCase() === name;
    }) || null;
  });
};

SqliteBrain.prototype.usersForRawFuzzyName = function(fuzzyName) {
  var self = this;

  fuzzyName = fuzzyName && fuzzyName.toLowerCase() || '';

  return this.users().then(function(users) {
    return _.filter(users, function(user) {
      return user && user.name.toLowerCase().substr(0, fuzzyName.length) === fuzzyName;
    });
  });
};

SqliteBrain.prototype.usersForFuzzyName = function(fuzzyName) {
  fuzzyName = fuzzyName && fuzzyName.toLowerCase() || '';

  return this.usersForRawFuzzyName(fuzzyName).then(function(matchedUsers) {
    var exactMatch = _.find(matchedUsers, function(user) {
      return user.name.toLowerCase() === fuzzyName;
    });
    return exactMatch && [exactMatch] || matchedUsers;
  });
};

module.exports = SqliteBrain;
