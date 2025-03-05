const types = ['pushPull', 'majordomo']

async function handler ({ item }) {
  const { pick, has } = this.app.bajo.lib._
  if (!types.includes(item.type)) throw this.error('invalidConnType%s', item.type)
  item.options = item.options ?? {}
  if (!has(item.options, 'host')) throw this.error('isRequired%s', 'options.host')
  if (!has(item.options, 'port')) throw this.error('isRequired%s', 'options.port')
  return pick(item, ['type', 'name', 'options'])
}

async function init () {
  const { buildCollections } = this.app.bajo
  this.connections = await buildCollections({ ns: this.name, useDefaultName: false, handler, container: 'connections' })
}

export default init
