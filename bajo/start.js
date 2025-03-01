import PushPull from '../lib/factory/push-pull.js'

async function start () {
  const { merge } = this.app.bajo.lib._
  for (const conn of this.connections) {
    if (conn.type === 'pushPull') {
      conn.instance = new PushPull(this, conn)
    }
  }
  if (!this.config.jobQueue) return
  const conn = { name: 'jobQueue', type: 'pushPull', options: this.config.jobQueue.connection }
  this.jobQueue = merge({}, conn, { instance: new PushPull(this, conn) })
  this.jobQueue.pusher = this.jobQueue.instance.createPusher()
}

export default start
