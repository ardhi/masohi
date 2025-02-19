const split = (address) => {
  if (!address.includes(':')) address = ':' + address
  const [subject = '', cinfo = ''] = address.split(':')
  const [connection = '', plugin = ''] = cinfo.split('@')
  return { subject, connection, plugin }
}

export default split
