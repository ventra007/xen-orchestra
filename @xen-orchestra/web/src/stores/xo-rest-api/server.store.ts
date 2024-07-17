import type { Server } from '@/types/server.type'
import { createXoStoreConfig } from '@/utils/create-xo-store-config.util'
import { createSubscribableStoreContext } from '@core/utils/create-subscribable-store-context.util'
// import { sortByNameLabel } from '@core/utils/sort-by-name-label.util'
import { useFetch } from '@vueuse/core'
import { defineStore } from 'pinia'
import { ref } from 'vue'

export const useServerStore = defineStore('server', () => {
  const config = createXoStoreConfig('server', {
    // sortBy: sortByNameLabel,
  })

  const { data: serversEndpoints, execute: fetchServersEndpoints } = useFetch(`/rest/v0/servers`, {
    immediate: false,
    beforeFetch({ options }) {
      options.credentials = 'include'
      return { options }
    },
  })
    .get()
    .json()

  const fetchedServersEndpoints = ref<Server[]>([])

  const fetchData = async () => {
    await fetchServersEndpoints()

    if (Array.isArray(serversEndpoints.value)) {
      const results = await Promise.all(
        serversEndpoints.value.map(async url => {
          const { data } = await useFetch(url, { immediate: false }).get().json()
          return data.value
        })
      )

      fetchedServersEndpoints.value = results
    }
  }

  const context = createSubscribableStoreContext(config, {})

  return {
    ...context,
    fetchedServersEndpoints,
    fetchData,
  }
})
