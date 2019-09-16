var Minio = require('minio')
var mime = require('mime')

var MinioStorage = function (properties) {
  this.bucket = properties.bucket
  this.minioClient = new Minio.Client({
    endPoint: '127.0.0.1',
    port: 9000,
    useSSL: false,
    accessKey: properties.accessKey,
    secretKey: properties.secretKey
  })
}

MinioStorage.prototype.listKeys = function (options, cb) {
  if (typeof options === 'function') {
    cb = options
    options = {}
  }
  var result = {
    done: true,
    keys: []
  }
  var stream = this.minioClient.listObjectsV2(this.bucket, '', true)
  stream.on('data', function(obj) {
    result.keys.push(obj.name)
  })
  stream.on('error', function(err) { return cb(err) })
  stream.on('end', function() {
    return cb(null, result)
  })
}

MinioStorage.prototype.saveFile = function (file, filename, cb) {
  var metaData = {
    'Content-Type': mime.lookup(filename),
  }  
  return this.minioClient.putObject(this.bucket, filename, file, cb)
}

MinioStorage.prototype.getFile = function (filename, cb) {
  return this.minioClient.getObject(this.bucket, filename, function(err, stream) {
    if (err) {
      return cb(err)
    }
    var bufs = []
    stream.on('data', function(d) { bufs.push(d) })
    stream.on('end', function(){
      var data  = {
        Body: Buffer.concat(bufs)
      }
      return cb(null, data)
    })
    stream.on('error', function(err) {
      return cb(err)
    })
  })
}

module.exports = MinioStorage