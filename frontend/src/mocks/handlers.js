import { http, HttpResponse } from 'msw'

export const handlers = [
  http.get('/api/v1/tables', () => {
    return HttpResponse.json({
      tables: ['default.events', 'default.logging'],
    })
  }),
]
