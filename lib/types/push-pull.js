class PushPull {
  constructor (plugin, conn) {
    this.plugin = plugin
    this.conn = conn
    this.conn.options = this.conn.options = {}
    this.name = this.conn.name
    this.pusher = null
    this.puller = null
    if (this.conn.options.pusherAutoStart) this.startPusher()
    if (this.conn.options.pullerAutoStart) this.startPuller()
  }

  startPusher = () => {
    if (this.pusher) return this.pusher
    const { host, port, pusherOpts } = this.conn.options
    const sock = new this.plugin.zmq.Push(pusherOpts ?? {})
    sock.bind(`tcp://${host}:${port}`)
    this.pusher = sock
    this.plugin.log.debug('pusherStarted%s%s%d', this.name, host, port)
  }

  startPuller = () => {
    if (this.puller) return this.puller
    const { host, port, pullerOpts } = this.conn.options
    const sock = new this.plugin.zmq.Pull(pullerOpts ?? {})
    sock.connect(`tcp://${host}:${port}`)
    this.puller = sock
    this.plugin.log.debug('pullerStarted%s%s%d', this.name, host, port)
  }
}

export default PushPull
