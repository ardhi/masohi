function emit (event, ...params) {
  this.instance.emit(event, ...params)
}

export default emit
