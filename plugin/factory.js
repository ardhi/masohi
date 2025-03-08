import PushPull from '../lib/types/push-pull.js'
import sendHandler from '../lib/send-handler.js'

const types = ['pushPull', 'majordomo']

async function connSanitizer ({ item }) {
  const { pick, has } = this.app.bajo.lib._
  if (!types.includes(item.type)) throw this.error('invalidConnType%s', item.type)
  item.options = item.options ?? {}
  if (!has(item.options, 'host')) throw this.error('isRequired%s', 'options.host')
  if (!has(item.options, 'port')) throw this.error('isRequired%s', 'options.port')
  return pick(item, ['type', 'name', 'options'])
}

async function factory (pkgName) {
  const me = this

  return class Masohi extends this.lib.BajoPlugin {
    constructor () {
      super(pkgName, me.app)
      this.alias = 'masohi'
      this.dependencies = ['bajo-queue']
      this.config = {
        connections: []
      }
    }

    init = async () => {
      const { buildCollections, importPkg } = this.app.bajo
      this.zmq = await importPkg('bajoQueue:zeromq')
      this.connections = await buildCollections({
        ns: this.name,
        useDefaultName: false,
        handler: connSanitizer,
        container: 'connections'
      })
    }

    start = async () => {
      for (const conn of this.connections) {
        conn.options = conn.options ?? {}
        if (conn.type === 'pushPull') {
          conn.instance = new PushPull(this, conn)
          this.log.debug('instanceCreatedOnConn%s%s', conn.name, this.print.write(conn.type))
        }
      }
    }

    send = async (payload, noQueue) => {
      const { push } = this.app.bajoQueue
      if (noQueue) {
        await sendHandler.call(this, payload)
        return
      }
      const pushed = await push('masohi:send', payload)
      if (!pushed) await sendHandler.call(this, payload)
    }
  }
}

export default factory
