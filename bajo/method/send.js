async function send ({ to, from, subject, message, conn = 'masohiMail', options = {} }) {
  const { getPlugin, importModule } = this.app.bajo
  // conn format: connName@masohiMail
  let [connName, pluginName] = conn.split('@')
  if (!pluginName) {
    pluginName = connName
    connName = 'default'
  }
  const plugin = getPlugin(pluginName)
  const handler = await importModule(`${plugin.dir.pkg}/lib/send.js`)
  const resp = await handler.call(plugin, { to, from, subject, message, conn: connName, options })
  return resp
}

export default send
