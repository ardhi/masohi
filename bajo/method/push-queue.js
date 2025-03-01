async function pushQueue (type, input) {
  const { isArray, isPlainObject } = this.app.bajo.lib._
  const payload = isArray(input) || isPlainObject(input) ? input : { value: input }
  if (this.jobQueue) {
    const data = JSON.stringify({ type, payload })
    try {
      await this.jobQueue.pusher.send(data)
      return true
    } catch (err) {
      this.log.error('queueError%s', err.message)
    }
  }
  return false
}

export default pushQueue
