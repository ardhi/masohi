function verify (address, { skipConnectionCheck = false } = {}) {
  const { isSet } = this.app.bajo
  const { isEmpty, find } = this.app.bajo.lib._
  const { subject, connection, plugin } = this.addressSplit(address)
  if (!isSet(subject) || isEmpty(connection) || isEmpty(plugin)) throw this.error('Invalid address \'%s\'', address)
  if (!this.app[plugin]) throw this.error('Unknown plugin \'%s\' or plugin isn\'t loaded yet', plugin)
  if (!skipConnectionCheck) if (!find(this.app[plugin].connections, { name: connection })) throw this.error('Unknown connection \'%s@%s\'', connection, plugin)
  return true
}

export default verify
