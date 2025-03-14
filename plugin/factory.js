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
        localPubSub: {
          host: '127.0.0.1',
          port: 7782
        },
        transformers: []
      }
      this.types = ['pushPull', 'pubSub']
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

      const transSanitizer = async ({ item }) => {
        const { has } = this.lib._
        for (const key of ['conn', 'nsConn', 'ns']) {
          if (!has(item, key)) throw this.error('isRequired%s', key)
        }
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
      this.transformers = await buildCollections({
        ns: this.name,
        useDefaultName: false,
        dupChecks: ['conn', 'ns', 'nsConn'],
        container: 'transformers',
        handler: transSanitizer
      })
    }

    start = async () => {
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
    }

    _catchAllHandler = async (conn) => {
      const { runHook } = this.app.bajo
      for await (const [topic, msg] of conn.instance.subscriber) {
        try {
          const [alias, ...args] = topic.toString().split(':')
          const payload = JSON.parse(msg.toString())
          await runHook(`${this.name}.${alias}:${args.join(':')}`, payload)
        } catch (err) {
          this.log.error('error%s', err.message)
        }
      }
    }

    // send to queue
    send = async (payload, noQueue) => {
      const { push } = this.app.bajoQueue
      if (noQueue) {
        await sendHandler.call(this, payload)
        return
      }
      const pushed = await push('masohi:send', payload)
      if (!pushed) await sendHandler.call(this, payload)
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

    publish = async ({ topic, payload, options = {} }) => {
      if (!payload) return
      const { find, trim } = this.lib._
      const { getPlugin } = this.app.bajo
      const { ns, conn = 'default', nsConn } = options
      const connection = this.getConn(conn)
      if (!connection) return
      const plugin = getPlugin(ns, true)
      if (!plugin) return
      const transformer = find(this.transformers, { ns: plugin.name, conn, nsConn })
      payload.data = trim(payload.data)
      if (transformer && payload.data) {
        const result = await this.transform({ transformer, payload, plugin })
        if (!result) return
      }
      const params = { ns, conn, payload, nsConn }
      await connection.instance.publisher.send([`${plugin.alias}:${topic}`, JSON.stringify(params)])
    }

    subscribe = async ({ topic, options = {} }) => {
      const { conn = 'default' } = options
      const connection = this.getConn(conn)
      if (!connection) return
      connection.instance.subscriber.subscribe(topic)
    }
  }
}

export default factory
