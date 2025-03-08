import { Router } from 'zeromq'

class MajordomoBroker {
  constructor (plugin) {
    this.plugin = plugin
    this.socket = new Router({ sendHighWaterMark: 1, sendTimeout: 1 })
  }

  async start (conn) {
    this.conn = conn
    await this.socket.bind(`tcp://${conn.host}:${conn.port}`)
    this.plugin.log.debug('connIs%s%s', conn.name, 'openedL')
    this.plugin.log.info('boundTo%s%s%d', conn.name, conn.host, conn.port)

    for await (const [sender,, header, ...args] of this.socket) {
      switch (header) {
        case 'MDPC01': await this.handleClient(sender, ...args); break
        case 'MDPW01': await this.handleWorker(sender, ...args); break
        default: this.plugin.log.error('invalidHeader%s', header)
      }
    }
  }

  async stop () {
    if (!this.socket.closed) {
      this.socket.close()
      this.plugin.log.debug('connIs%s%s', this.conn.name, this.plugin.print.write('closedL'))
    }
  }
}

export default MajordomoBroker
