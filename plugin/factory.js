import PushPull from '../lib/types/push-pull.js'
import PubSub from '../lib/types/pub-sub.js'
import sendHandler from '../lib/send-handler.js'

async function factory (pkgName) {
  const me = this

  return class Masohi extends this.lib.BajoPlugin {
    constructor () {
      super(pkgName, me.app)
      this.alias = 'masohi'
      this.dependencies = ['bajo-queue']
      this.config = {
        connections: [],
        pipelines: [],
        localPubSub: {
          host: '127.0.0.1',
          port: 7782
        }
      }
      this.types = ['pushPull', 'pubSub']
      this.sourceMsg = {}
    }

    init = async () => {
      const { buildCollections, importPkg } = this.app.bajo

      const connSanitizer = async ({ item }) => {
        const { pick, has } = this.lib._
        if (!this.types.includes(item.type)) throw this.error('invalidConnType%s', item.type)
        item.options = item.options ?? {}
        if (!has(item.options, 'host')) throw this.error('isRequired%s', 'options.host')
        if (!has(item.options, 'port')) throw this.error('isRequired%s', 'options.port')
        return pick(item, ['type', 'name', 'options'])
      }

      const pipeSanitizer = async ({ item }) => {
        const { has, isString, map } = this.lib._
        const { breakNsPath } = this.app.bajo
        for (const key of ['source', 'handlers']) {
          if (!has(item, key)) throw this.error('isRequired%s', key)
        }
        breakNsPath(item.source)
        item.sourceType = item.sourceType ?? 'connection'
        if (!['connection', 'hook'].includes(item.sourceType)) throw this.error('invalid%s%s', this.print.write('sourceType'), item.sourceType)
        if (isString(item.handlers)) item.handlers = [item.handlers]
        item.handlers = map(item.handlers, h => {
          if (isString(h)) h = { handler: h }
          return h
        })
        return item
      }

      this.config.connections.unshift({
        name: 'default',
        type: 'pubSub',
        options: {
          host: this.config.localPubSub.host,
          port: this.config.localPubSub.port,
          publisherAutoStart: true,
          subscriberAutoStart: true,
          publisherOpts: {
            sendTimeout: 0
          }
        }
      })
      this.zmq = await importPkg('bajoQueue:zeromq')
      this.connections = await buildCollections({
        ns: this.name,
        useDefaultName: false,
        handler: connSanitizer,
        container: 'connections'
      })
      this.pipelines = await buildCollections({
        ns: this.name,
        container: 'pipelines',
        handler: pipeSanitizer
      })
    }

    start = async () => {
      const { eachPlugins, breakNsPath } = this.app.bajo
      const { get, filter, map, isPlainObject, has } = this.lib._
      const { outmatchNs } = this.lib

      for (const conn of this.connections) {
        conn.options = conn.options ?? {}
        switch (conn.type) {
          case 'pushPull': conn.instance = new PushPull(this, conn); break
          case 'pubSub': conn.instance = new PubSub(this, conn); break
        }
        if (conn.instance) await conn.instance.init()
        this.log.debug('instanceCreatedOnConn%s%s', conn.name, this.print.write(conn.type))
      }

      const conn = this.getConn('default')
      if (!conn) return
      conn.instance.subscriber.subscribe('')
      this._catchAllHandler(conn)
      // get all pipeline capable connections
      const connPipes = []
      await eachPlugins(async ({ ns }) => {
        const conns = map(filter(get(this, `app.${ns}.connections`, []), c => {
          return c.masohiPipeline
        }), c => `${ns}.${c.name}`)
        connPipes.push(...conns)
      })
      for (const ns of connPipes) {
        const mod = { ns, path: 'data', src: this.name, level: 1000 }
        mod.handler = async function (params) {
          const pipes = filter(this.pipelines, p => {
            return p.sourceType === 'connection' && outmatchNs(params.source, p.source)
          })
          for (const p of pipes) {
            await this.pushToPipeline(p.name, params)
          }
        }
        this.app.bajo.hooks.push(mod)
      }
      // tap hook
      for (const p of this.pipelines) {
        if (p.sourceType !== 'hook') continue
        const { fullNs, path } = breakNsPath(p.source)
        const mod = { ns: fullNs, path, src: this.name, level: 1000 }
        mod.handler = async function (params) {
          let newParams = {}
          if (isPlainObject(params) && has(params, 'payload') && has(params, 'source') &&
            has(params.payload, 'type') && has(params.payload, 'data')) newParams = params
          else newParams.payload = { type: typeof params, data: params }
          newParams.source = p.source
          await this.pushToPipeline(p.name, newParams)
        }
        this.app.bajo.hooks.push(mod)
      }
    }

    _catchAllHandler = async (conn) => {
      const { runHook } = this.app.bajo
      const { camelCase } = this.lib._
      for await (const [topic, msg] of conn.instance.subscriber) {
        try {
          const [alias, ...args] = topic.toString().split(':')
          const data = JSON.parse(msg.toString())
          await runHook(`${this.name}.${alias}:${camelCase(args.join(':'))}`, data)
        } catch (err) {
          this.log.error('error%s', err.message)
        }
      }
    }

    // send message
    send = async (input) => {
      const { push } = this.app.bajoQueue
      const payload = { type: 'object', data: input }
      const pushed = await push({ worker: 'masohi:workerSend', payload })
      if (!pushed) await sendHandler.call(this, input)
    }

    workerSend = async ({ payload, source } = {}) => {
      await sendHandler.call(this, payload.data)
    }

    getConn = name => {
      const { find } = this.lib._
      const conn = find(this.connections, { name })
      if (!conn) {
        this.log.error('notFound%s%s', this.print.write('Connection'), name)
        return
      }
      return conn
    }

    transform = async ({ transformer, payload, plugin }) => {
      const { isFunction, get } = this.lib._
      const { outmatch } = this.lib
      const { callHandler, isSet } = this.app.bajo
      if (transformer.topic && isSet(payload.topic)) {
        const match = outmatch(transformer.topic)
        if (!match(payload.topic)) return true
      }
      const decoder = get(transformer, 'decoder')
      if (!decoder) return false
      try {
        if (isFunction(decoder)) payload.data = await decoder(payload.data)
        else {
          const [hns, hmethod, ...args] = decoder.split(':')
          args.push(payload.data)
          const result = await callHandler(plugin, `${hns}:${hmethod}`, ...args)
          payload.data = result
          payload.type = 'object'
        }
        return true
      } catch (err) {
        return false
      }
    }

    publish = async ({ hook, payload, source, conn = 'default' }) => {
      if (!hook || !payload) return
      const { getPlugin, breakNsPath } = this.app.bajo
      const { ns } = breakNsPath(source)
      const connection = this.getConn(conn)
      if (!connection) throw this.error('notFound%s%s', this.print.write('Connection'), conn)
      const plugin = getPlugin(ns, true)
      if (!plugin) throw this.error('pluginWithNameAliasNotLoaded%s', ns)
      const params = { payload, source, conn }
      await connection.instance.publisher.send([`${plugin.alias}:${hook}`, JSON.stringify(params)])
    }

    subscribe = async ({ topic, options = {} }) => {
      const { conn = 'default' } = options
      const connection = this.getConn(conn)
      if (!connection) return
      connection.instance.subscriber.subscribe(topic)
    }

    preventRepeatedMsg = ({ payload, source } = {}) => {
      const { isEqual } = this.lib._
      this.sourceMsg[source] = this.sourceMsg[source] ?? {}
      if (isEqual(this.sourceMsg[source].msg, payload.data)) throw this.error('repeatedMsg', { payload })
      this.sourceMsg[source].msg = payload.data
    }

    isExpiredMsg = (source, ttl) => {
      const { has } = this.lib._
      if (ttl <= 0) return false
      const now = Date.now()
      this.sourceMsg[source] = this.sourceMsg[source] ?? {}
      if (!has(this.sourceMsg[source], 'ts')) this.sourceMsg[source].ts = now
      if ((this.sourceMsg[source].ts + ttl) < now) {
        this.sourceMsg[source].ts = now
        return false
      }
      return true
    }

    pushToPipeline = async (name, options = {}) => {
      const { find } = this.lib._
      const { callHandler } = this.app.bajo
      const { push } = this.app.bajoQueue
      const { source } = options
      const pipe = find(this.pipelines, { name })
      try {
        // handlers/transformers
        for (const item of pipe.handlers) {
          if (item.queue) {
            options.worker = item.handler
            await push(options)
          } else {
            await callHandler(item.handler, options)
          }
        }
      } catch (err) {
        this.log.error('error%s%s%s', this.print.write('pipeline%s', pipe.name),
          this.print.write('source%s', source), err.message)
      }
    }

    mergeStationData = async ({ payload, source }) => {
      const { data } = payload
      const { find, merge } = this.lib._
      const { getMemdbStorage } = this.app.dobo
      if (!getMemdbStorage) return
      const { importPkg } = this.app.bajo
      const { fixFloat } = this.app.masohiCodec
      const geolib = await importPkg('bajoSpatial:geolib')
      const [connection] = source.split(':')
      const stations = getMemdbStorage('MasohiStation')
      const station = find(stations, { connection })
      if (!station) return
      const item = {
        stationId: station.id,
        feed: station.feedType
      }
      if (station.lat && station.lng && data.lat && data.lng) {
        item.stationDistance = fixFloat(geolib.getDistance(
          { longitude: station.lng, latitude: station.lat },
          { longitude: data.lng, latitude: data.lat }
        ) / 1000, null, 2)
        item.stationBearing = fixFloat(geolib.getRhumbLineBearing(
          { longitude: station.lng, latitude: station.lat },
          { longitude: data.lng, latitude: data.lat }
        ), null, 2)
      }
      payload.data = merge({}, item, station.dataMerge ?? {}, data)
    }
  }
}

export default factory
