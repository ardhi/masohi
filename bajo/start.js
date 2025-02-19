import EventEmitter2 from 'eventemitter2'
import collectEvents from '../lib/collect-events.js'
import handler from '../lib/collect-broadcast-pools.js'

async function start () {
  const { pick } = this.app.bajo.lib._
  const { buildCollections } = this.app.bajo
  this.broadcastPools = await buildCollections({ ns: this.name, handler, container: 'broadcastPools', dupChecks: ['name'] })
  const opts = pick(this.getConfig(), ['maxListeners', 'verboseMemoryLeak', 'ignoreErrors'])
  opts.wildcard = true
  opts.delimiter = '.'
  this.instance = new EventEmitter2(opts)
  await collectEvents.call(this)
}

export default start
