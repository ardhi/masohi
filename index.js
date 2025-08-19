import PushPull from './lib/types/push-pull.js'
import PubSub from './lib/types/pub-sub.js'
import sendHandler from './lib/send-handler.js'

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
          port: 17782
        },
        waibu: {
          prefix: 'messaging',
          title: 'messaging'
        },
        waibuMpa: {
          logo: 'wifi',
          icon: 'wifi'
        },
        waibuAdmin: {
          modelDisabled: 'all'
        },
        saveStream: {
          ingest: true,
          ingestHistory: false
        },
        dumpPipelineError: false
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

      if (this.config.localPubSub) {
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
      }
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
      const { get, filter, map, isPlainObject, has, merge } = this.lib._
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
          if (this.config.saveStream.ingest) await this.saveStream(merge({}, params, { type: 'INGEST' }))
          if (this.config.saveStream.ingestHistory) await this.saveStreamHistory(merge({}, params, { type: 'INGEST' }))
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
          if (isPlainObject(params) && has(params, 'payload') && has(params, 'source')) newParams = params
          else newParams = { payload: params }
          newParams.source = p.source
          await this.pushToPipeline(p.name, newParams)
        }
        this.app.bajo.hooks.push(mod)
      }
      // pubsub
      const conn = this.getConn('default')
      if (!conn) return
      conn.instance.subscriber.subscribe('')
      this._catchAllHandler(conn)
    }

    _catchAllHandler = async (conn) => {
      const { runHook } = this.app.bajo
      const { camelCase } = this.lib._
      for await (const [topic, msg] of conn.instance.subscriber) {
        try {
          const [ns, ...args] = topic.toString().split(':')
          const data = JSON.parse(msg.toString())
          await runHook(`${this.name}.subscriber.${ns}:${camelCase(args.join(':'))}`, data)
        } catch (err) {
          this.log.error('error%s', err.message)
        }
      }
    }

    saveStreamHistory = async (body) => {
      if (!this.app.dobo) return
      const { recordCreate } = this.app.dobo
      await recordCreate('MasohiStreamHistory', body, { noResult: true, noValidation: true })
    }

    saveStream = async (body) => {
      if (!this.app.dobo) return
      const { recordUpsert } = this.app.dobo
      const query = { source: body.source }
      await recordUpsert('MasohiStream', body, { query, noResult: true, noValidation: true })
    }

    // send message
    send = async (params = {}) => {
      const { noQueue = false } = params
      if (noQueue) {
        await sendHandler.call(this, params)
        return
      }
      const { push } = this.app.bajoQueue
      params.worker = 'masohi:workerSend'
      await push(params)
    }

    workerSend = async (params) => {
      await sendHandler.call(this, params)
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

    publish = async (topicName, { payload, source, connection = 'default' }) => {
      if (!topicName || !payload) return
      const { getPlugin, breakNsPath } = this.app.bajo
      const { ns } = breakNsPath(source)
      const conn = this.getConn(connection)
      if (!conn) throw this.error('notFound%s%s', this.print.write('Connection'), connection)
      const plugin = getPlugin(ns, true)
      if (!plugin) throw this.error('pluginWithNameAliasNotLoaded%s', ns)
      const params = { payload, source, connection }
      await conn.instance.publisher.send([`${plugin.name}:${topicName}`, JSON.stringify(params)])
    }

    subscribe = async ({ topic, options = {} }) => {
      const { conn = 'default' } = options
      const connection = this.getConn(conn)
      if (!connection) return
      connection.instance.subscriber.subscribe(topic)
    }

    lodashTransform = (method = '', params = {}) => {
      for (const item of method.split('.')) {
        const fn = this.lib._[item]
        if (!fn) continue
        params.payload = fn(params.payload)
      }
    }

    preventRepeatedMsg = (params = {}) => {
      const { isEqual } = this.lib._
      const { payload, source } = params
      if (isEqual(this.sourceMsg[source], payload)) throw this.error('repeatedMsg')
      this.sourceMsg[source] = payload
    }

    pushToPipeline = async (name, options = {}) => {
      const { find, isString, camelCase } = this.lib._
      const { callHandler, runHook } = this.app.bajo
      const { push } = this.app.bajoQueue
      const { source } = options
      const pipe = find(this.pipelines, { name })
      try {
        // handlers/transformers
        for (const item of pipe.handlers) {
          if (isString(item.handler)) await runHook(`${this.name}.${camelCase(item.handler)}:beforePipe`, options)
          if (item.queue) {
            options.worker = item.handler
            await push(options)
          } else {
            await callHandler(item.handler, options)
            if (isString(item.handler)) await runHook(`${this.name}.${camelCase(item.handler)}:afterPipe`, options)
          }
        }
      } catch (err) {
        if (this.app.bajo.config.log.level === 'trace') {
          this.log.error('error%s%s%s', this.print.write('pipeline%s', pipe.name),
            this.print.write('source%s', source), err.message)
        }
        if (this.config.dumpPipelineError) console.error(err)
      }
    }
  }
}

export default factory
