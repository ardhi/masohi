const handlers = ['masohi:send']

async function jobQueueWorker (conn) {
  const { runHook, callHandler } = this.app.bajo
  const puller = conn.instance.createPuller()

  for await (const [msg] of puller) {
    await runHook(`${this.name}:beforeJobQueueWorkerProcess`, msg)
    try {
      const { type, payload } = JSON.parse(msg.toString())
      if (!handlers.includes(type)) throw this.error('invalidWorkerHandler')
      await callHandler(this, type, payload, true)
    } catch (err) {
      console.error(err)
      this.log.error('jobQueueError%s', err.message)
    }
    await runHook(`${this.name}:afterJobQueueWorkerProcess`, msg)
  }
}

export default jobQueueWorker
