import sendHandler from '../../lib/send-handler.js'

async function send (payload, noQueue) {
  if (noQueue) {
    await sendHandler.call(this, payload)
    return
  }
  const pushed = await this.pushQueue('masohi:send', payload)
  if (!pushed) await sendHandler.call(this, payload)
}

export default send
