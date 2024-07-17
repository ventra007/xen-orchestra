export type ServerData = {
  host: string
  label: string
  username: string
  allowUnauthorized: boolean
  enabled: boolean
  readonly: boolean
  id: string
  status: string
  poolId: string
  href: string
}

export type Server = {
  type: 'server'
  server: ServerData[]
}
