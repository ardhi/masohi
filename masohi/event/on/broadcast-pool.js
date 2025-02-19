const broadcastPool = {
  level: 1,
  handler: async function onBroadcastPool ({ msg, from, to, subject }) {
    const { callHandler } = this.app.bajo
    const { get, isFunction, filter, isEmpty } = this.app.bajo.lib._
    const pools = filter(this.broadcastPools, p => {
      return p.from.includes(from)
    })
    for (const p of pools) {
      if (p.handler) {
        await callHandler(p.handler, { from, to, subject, msg })
        continue
      }
      let ok = true
      if (p.filter) ok = await callHandler(p.filter, { from, to, subject, msg })
      if (!ok) continue
      let item = msg
      if (p.transformer) item = await callHandler(p.transformer, { from, to, subject, msg })
      if (!p.to) return
      for (let t of p.to) {
        const addr = this.addressSplit(t)
        const key = `${addr.plugin}.send`
        const handler = get(this.app, key)
        if (!isFunction(handler)) continue
        if (isEmpty(addr.subject)) t = `${subject}:${t}`
        await handler({ msg: item, from, to: t })
      }
    }
  }
}

export default broadcastPool
