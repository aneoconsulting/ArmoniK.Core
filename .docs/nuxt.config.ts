const baseURL = process.env.NODE_ENV === 'production' ? '/ArmoniK.Core/' : '/'

export default defineNuxtConfig({
  app: {
    baseURL,
    head: {
      link: [
        {
          rel: 'icon',
          type: 'image/ico',
          href: `${baseURL}favicon.ico`
        }
      ]
    }
  },

  extends: '@aneoconsultingfr/armonik-docs-theme',

  runtimeConfig: {
    public: {
      siteName: 'ArmoniK Core',
      siteDescription: 'The heart of ArmoniK'
    }
  },

  robots: { 
    robotsTxt: false
  },
})
