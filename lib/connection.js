async function connectionFactory () {
  const { Tools } = this.app.baseClass
  const { omit } = this.app.lib._

  /**
   * Connection class
   *
   * @class
   */
  class MasohiConnection extends Tools {
    constructor (plugin, options = {}) {
      super(plugin)
      /**
       * Connection name
       *
       * @type {string}
       */
      this.name = options.name

      /**
       * Connected instance
       *
       * @type {Object}
       */
      this.instance = null

      /**
       * Options object from connection defined on ```bajoQueue.config.connections```
       *
       * @type {Object}
       */
      this.options = omit(options, ['name'])
      this.options.connName = options.name
    }

    dispose () {
      const { fs } = this.app.lib
      super.dispose()
      if (this.options.filename) {
        try {
          fs.unlinkSync(this.options.filename)
        } catch (err) {}
      }
    }
  }

  this.app.baseClass.MasohiConnection = MasohiConnection
  return MasohiConnection
}

export default connectionFactory
