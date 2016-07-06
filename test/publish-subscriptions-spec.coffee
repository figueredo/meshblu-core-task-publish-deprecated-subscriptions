_ = require 'lodash'
uuid = require 'uuid'
redis = require 'fakeredis'
mongojs = require 'mongojs'
Datastore = require 'meshblu-core-datastore'
Cache = require 'meshblu-core-cache'
JobManager = require 'meshblu-core-job-manager'
DeliverSubscriptions = require '../'

describe 'DeliverSubscriptions', ->
  beforeEach (done) ->
    database = mongojs 'subscription-test', ['devices']
    @datastore = new Datastore
      database: database
      collection: 'devices'

    database.devices.remove done

  beforeEach ->
    @redisKey = uuid.v1()
    @redisPubSubKey = uuid.v1()
    @pepper = 'im-a-pepper'
    @uuidAliasResolver = resolve: (uuid, callback) => callback(null, uuid)

    @jobManager = new JobManager
      client: _.bindAll redis.createClient @redisKey
      timeoutSeconds: 1
      jobLogSampleRate: 1
      
    options = {
      pepper: 'totally-a-secret'
      @cache
      @datastore
      @jobManager
      @uuidAliasResolver
      @pepper
    }

    @sut = new DeliverSubscriptions options

  describe '->do', ->
    context 'when device has messageForward exist', ->
      beforeEach (done) ->
        record =
          uuid: 'emitter-uuid'
          meshblu:
            messageForward: [
              'subscriber-uuid'
            ]

        @datastore.insert record, done

      beforeEach (done) ->
        request =
          metadata:
            auth:
              uuid: 'emitter-uuid'
              token: 'abc123'
            responseId: 'its-electric'
            toUuid: 'emitter-uuid'
            fromUuid: 'emitter-uuid'
            messageType: 'broadcast'
          rawData: '{"devices":"*"}'

        @sut.do request, (error, @response) => done error

      it 'should return a 204', ->
        expectedResponse =
          metadata:
            responseId: 'its-electric'
            code: 204
            status: 'No Content'

        expect(@response).to.deep.equal expectedResponse

      describe 'JobManager gets DeliverReceivedMessage job', (done) ->
        beforeEach (done) ->
          @jobManager.getRequest ['request'], (error, @request) =>
            done error

        it 'should be a received messageType', ->
          auth =
            uuid: 'emitter-uuid'
            token: 'abc123'

          {rawData, metadata} = @request
          expect(metadata.auth).to.deep.equal auth
          expect(metadata.jobType).to.equal 'DeliverReceivedMessage'
          expect(metadata.messageType).to.equal 'received'
          expect(metadata.toUuid).to.equal 'subscriber-uuid'
          expect(metadata.fromUuid).to.equal 'emitter-uuid'
          expect(rawData).to.equal JSON.stringify
            devices:['subscriber-uuid']
            forwardedFor:['emitter-uuid']
            fromUuid:'emitter-uuid'

    context 'messageType: sent', ->
      beforeEach (done) ->
        record =
          uuid: 'emitter-uuid'
          meshblu:
            messageForward: [
              'subscriber-uuid'
            ]

        @datastore.insert record, done

      beforeEach (done) ->
        request =
          metadata:
            auth:
              uuid: 'emitter-uuid'
              token: 'abc123'
            responseId: 'its-electric'
            fromUuid: 'emitter-uuid'
            messageType: 'sent'
          rawData: '{"devices":"*"}'

        @sut.do request, (error, @response) => done error

      it 'should return a 204', ->
        expectedResponse =
          metadata:
            responseId: 'its-electric'
            code: 204
            status: 'No Content'

        expect(@response).to.deep.equal expectedResponse
