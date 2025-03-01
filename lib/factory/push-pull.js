import zmq from 'zeromq'

class PushPull {
  constructor (plugin, conn) {
    this.plugin = plugin
    this.conn = conn
    this.name = this.conn.name
  }

  createPusher () {
    const sock = new zmq.Push({ sendTimeout: 0 })
    const { host, port } = this.conn.options
    sock.bind(`tcp://${host}:${port}`)
    this.plugin.log.debug('pusherCreated%s%s%d', this.name, host, port)
    return sock
  }

  createPuller () {
    const sock = new zmq.Pull()
    const { host, port } = this.conn.options
    sock.connect(`tcp://${host}:${port}`)
    this.plugin.log.debug('pullerCreated%s%s%d', this.name, host, port)
    return sock
  }
}

export default PushPull
