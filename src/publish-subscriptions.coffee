_ = require 'lodash'
async = require 'async'
uuid = require 'uuid'
DeviceManager = require 'meshblu-core-manager-device'
http = require 'http'

class DeliverSubscriptions
  constructor: (options={},dependencies={}) ->
    {cache,datastore,pepper,@uuidAliasResolver,@jobManager} = options
    @deviceManager ?= new DeviceManager {datastore, @uuidAliasResolver}

  _createJob: ({messageType, toUuid, message, fromUuid, auth}, callback) =>
    request =
      data: message
      metadata:
        auth: auth
        toUuid: toUuid
        fromUuid: fromUuid
        jobType: 'DeliverReceivedMessage'
        messageType: messageType
        responseId: uuid.v4()

    @jobManager.createRequest 'request', request, callback

  _doCallback: (request, code, callback) =>
    response =
      metadata:
        responseId: request.metadata.responseId
        code: code
        status: http.STATUS_CODES[code]
    callback null, response

  do: (request, callback) =>
    {auth, toUuid, fromUuid, messageType} = request.metadata
    fromUuid ?= auth.uuid
    message = JSON.parse request.rawData

    @_send {auth,toUuid, fromUuid, messageType, message}, (error) =>
      return callback error if error?
      return @_doCallback request, 204, callback

  _send: ({auth,toUuid,fromUuid,messageType,message}, callback=->) =>
    return callback null unless messageType == 'broadcast'

    @deviceManager.findOne {uuid: fromUuid}, (error, device) =>
      return callback error if error?
      subscriptions = device?.meshblu?.messageForward
      subscriptions ?= []
      async.eachSeries subscriptions, async.apply(@_publishSubscription, {auth,toUuid,fromUuid,messageType,message}), callback

  _publishSubscription: ({auth, toUuid, fromUuid, messageType, message}, subscriberUuid, callback) =>
    message = JSON.parse JSON.stringify(message)

    message.forwardedFor ?= []

    @uuidAliasResolver.resolve fromUuid, (error, resolvedFromUuid) =>
      # use the real uuid of the device
      message.forwardedFor.push resolvedFromUuid
      message.devices = [subscriberUuid]
      message.fromUuid = resolvedFromUuid

      @_createJob {toUuid: subscriberUuid, fromUuid: resolvedFromUuid, auth, messageType: 'received', message}, callback

module.exports = DeliverSubscriptions
