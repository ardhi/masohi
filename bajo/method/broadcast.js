function broadcast ({ msg, from, to, subject }) {
  this.instance.emit('masohi.broadcastPool', { msg, from, to, subject })
}

export default broadcast
