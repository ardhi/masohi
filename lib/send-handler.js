async function send (params = {}) {
  const { conn = 'masohiMail' } = params
  const { getPlugin } = this.app.bajo
  // conn format: connName@masohiMail
  let [connName, pluginName] = conn.split('@')
  if (!pluginName) {
    pluginName = connName
    connName = 'default'
  }
  const plugin = getPlugin(pluginName)
  params.conn = connName
  const resp = await plugin.send(params)
  return resp
}

export default send
