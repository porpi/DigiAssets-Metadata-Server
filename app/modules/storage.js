var S3Storage = require('./s3-storage')
var MinioStorage = require('./minio-storage')

var Storage = function (properties) {
  if (properties.storage.type === 's3') {
    var accessKeyId = properties.AWS.accessKeyId
    var secretAccessKey = properties.AWS.secretAccessKey
    var bucket = properties.AWS.S3bucket

    if (!accessKeyId || !secretAccessKey || !bucket) {
      throw new Error('Missing parameters for AWS S3 storage')
    }

    this.s3Client = new S3Storage({
      accessKeyId: accessKeyId,
      secretAccessKey: secretAccessKey,
      bucket: bucket
    })
  } else if (properties.storage.type === 'digitalocean') {
    var accessKeyId = properties.DigitalOcean.accessKeyId
    var secretAccessKey = properties.DigitalOcean.secretAccessKey
    var bucket = properties.DigitalOcean.spaceName
    var endpoint = properties.DigitalOcean.spacesEndPoint

    if (!accessKeyId || !secretAccessKey || !bucket || !endpoint) {
      throw new Error('Missing parameters for DigitalOcean Spaces storage')
    }

    this.s3Client = new S3Storage({
      accessKeyId: accessKeyId,
      secretAccessKey: secretAccessKey,
      bucket: bucket,
      endpoint: endpoint
    })
  } else if (properties.storage.type === 'minio') {
    var accessKey = properties.MINIO.accessKey
    var secretKey = properties.MINIO.secretKey
    var bucket = properties.MINIO.miniobucket
    var endPoint = properties.MINIO.endPoint
    var minioPort = properties.MINIO.minioPort
    var minioSSL = properties.MINIO.minioSSL

    if (!accessKey || !secretKey || !bucket || !endPoint || !minioSSL || !minioPort) {
      throw new Error('Missing parameters for MINIO storage')
    }

    this.minioClient = new MinioStorage({
      accessKey: accessKey,
      secretKey: secretKey,
      bucket: bucket,
      endPoint: endPoint,
      minioPort: Number(minioPort),
      minioSSL: minioSSL == 'false' ? false : true
    })
  }
}

Storage.prototype.listKeys = function (options, cb) {
  if (typeof options === 'function') {
    cb = options
    options = {}
  }
  if (this.s3Client) {
    return this.s3Client.listKeys(options, cb)
  } else if (this.minioClient) {
    return this.minioClient.listKeys(options, cb)
  }
  throw new Error('Storage type not configured')
}

Storage.prototype.saveFile = function (file, filename, cb) {
  if (this.s3Client) {
    return this.s3Client.saveFile(file, filename, cb)
  } else if (this.minioClient) {
    return this.minioClient.saveFile(file, filename, cb)
  }
  throw new Error('Storage type not configured')
}

Storage.prototype.getFile = function (filename, cb) {
  if (this.s3Client) {
    return this.s3Client.getFile(filename, cb)
  } else if (this.minioClient) {
    return this.minioClient.getFile(filename, cb)
  }
  throw new Error('Storage type not configured')
}

module.exports = Storage
