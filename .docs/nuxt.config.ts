export default defineNuxtConfig({
  app: {
    baseURL: process.env.NODE_ENV === "production" ? "/ArmoniK.Core/" : "",
  },

  extends: "@aneoconsultingfr/armonik-docs-theme",
});
