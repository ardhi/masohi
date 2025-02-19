async function collectBroadcastPoolsHandler ({ item }) {
  const { isString } = this.app.bajo.lib._
  for (const f of ['from', 'to']) {
    if (!item[f]) continue
    if (isString(item[f])) item[f] = [item[f]]
    for (const a of item[f]) {
      this.addressVerify(a, { skipConnectionCheck: item.skipConnectionCheck })
    }
  }
  if (!item.from || item.from.length === 0) throw this.error('A pool must have a \'from\' address')
}

export default collectBroadcastPoolsHandler
