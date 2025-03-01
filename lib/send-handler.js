async function send ({ to, cc, bcc, from, subject, message, conn = 'masohiMail', options = {} }) {
  const { getPlugin, importModule } = this.app.bajo
  // conn format: connName@masohiMail
  let [connName, pluginName] = conn.split('@')
  if (!pluginName) {
    pluginName = connName
    connName = 'default'
  }
  const plugin = getPlugin(pluginName)
  const handler = await importModule(`${plugin.dir.pkg}/lib/send-handler.js`)
  const resp = await handler.call(plugin, { to, cc, bcc, from, subject, message, conn: connName, options })
  return resp
}

export default send
