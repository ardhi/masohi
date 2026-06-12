async function send (params = {}) {
  const { conn = '' } = params
  // conn format: connName:masohiMail
  const [pluginName, connName = 'default'] = conn.split(':')
  try {
    const plugin = this.app.getPlugin(pluginName)
    params.conn = connName
    await plugin.send(params)
  } catch (err) {
    this.error('error%s', err.message)
    if (this.app.bajo.config.log.level === 'trace') console.error(err)
  }
}

export default send
