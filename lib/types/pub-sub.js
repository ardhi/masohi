class PubSub {
  constructor (plugin, conn) {
    this.plugin = plugin
    this.conn = conn
    this.conn.options = this.conn.options ?? {}
    this.name = this.conn.name
    this.publisher = null
    this.subscriber = null
  }

  init = async () => {
    if (this.conn.options.publisherAutoStart) await this.startPublisher()
    if (this.conn.options.subscriberAutoStart) this.startSubscriber()
  }

  startPublisher = async () => {
    if (this.publisher) return this.publisher
    const { host, port, publisherOpts } = this.conn.options
    const sock = new this.plugin.zmq.Publisher(publisherOpts ?? {})
    await sock.bind(`tcp://${host}:${port}`)
    this.publisher = sock
    this.plugin.log.debug('publisherStarted%s%s%d', this.name, host, port)
  }

  startSubscriber = () => {
    if (this.subscriber) return this.subscriber
    const { host, port, subscriberOpts } = this.conn.options
    const sock = new this.plugin.zmq.Subscriber(subscriberOpts ?? {})
    sock.connect(`tcp://${host}:${port}`)
    this.subscriber = sock
    this.plugin.log.debug('subscriberStarted%s%s%d', this.name, host, port)
  }
}

export default PubSub
