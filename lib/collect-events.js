const methods = ['on', 'off', 'once']

async function persistence (mod, args) {
  const { callHandler } = this.app.bajo
  const { collExists, recordCreate } = this.app.bajoDb
  const { isPlainObject } = this.app.bajo.lib._
  const exists = await collExists(mod.persist.schema)
  if (!exists) return
  let result
  if (mod.persist.transformer) {
    result = await callHandler(mod.persist.transformer, ...args)
    if (!result) return
  }
  if (!isPlainObject(result)) return
  try {
    await recordCreate(mod.persist.schema, result)
  } catch (err) {}
}

async function collectEvents () {
  const me = this
  const { eachPlugins, runHook, importModule, breakNsPathFromFile } = me.app.bajo
  const { merge, filter, groupBy, orderBy, isString } = me.app.bajo.lib._
  me.events = me.events ?? []
  await runHook(`${this.name}:beforeCollectEvents`)
  // collects
  me.log.trace('Collecting %s...', me.print.write('events'))
  await eachPlugins(async function ({ ns, dir, file }) {
    const parts = breakNsPathFromFile({ baseNs: ns, dir, file, suffix: '/event/', getType: true })
    if (!methods.includes(parts.type)) return undefined
    const mod = await importModule(file, { asHandler: true })
    if (!mod) return undefined
    if (mod.persist) {
      if (isString(mod.persist)) mod.persist = { schema: mod.persist }
      mod.persist.msgIndex = mod.persist.msgIndex ?? 0
    }
    mod.level = mod.level ?? 100
    merge(mod, { method: parts.type, ns: parts.fullNs, path: parts.path, src: ns })
    me.events.push(mod)
  }, { glob: `event/{${methods.join(',')}}/**/*.js`, prefix: this.name })
  // apply events
  await eachPlugins(async function ({ ns }) {
    for (const m of methods) {
      const events = filter(me.events, { ns, method: m })
      if (events.length === 0) return undefined
      const items = groupBy(events, 'path')
      for (const i in items) {
        const mods = orderBy(items[i], ['level'])
        me.log.trace('- [%s] %s:%s (%d)', m.toUpperCase(), ns, i, mods.length)
        me.instance[m](`${ns}.${i}`, async (...args) => {
          for (const mod of mods) {
            if (mod.handler) await mod.handler.call(this.app[mod.src], ...args)
            // persistence
            if (!me.bajoDb || !mod.persist) continue
            await persistence.call(me, mod, args)
          }
        })
      }
    }
  })
  me.log.debug('%s collected: %d', me.print.write('events'), me.events.length)
  await runHook(`${this.name}:afterCollectEvents`)
}

export default collectEvents
