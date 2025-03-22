async function beforeRecordCreate (body, options) {
  const { encrypt } = this.app.bajoExtra
  body.id = await encrypt(body.code, { subType: 'url' })
}

export default beforeRecordCreate
