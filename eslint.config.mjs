import js from "@eslint/js";
import globals from "globals";

export default [
  {
    ignores: [
      "node_modules/**",
      "archive/**",
      "analysis/**",
      "data/**",
      "src/core/**",
      "src/pipeline/**",
      "src/server/**",
      "src/scripts/*",
      "!src/scripts/dataset.js",
      "public/*",
      "!public/app.js",
    ],
  },
  {
    files: [
      "server.js",
      "src/data/**/*.js",
      "src/explorer/**/*.js",
      "test/**/*.js",
    ],
    ...js.configs.recommended,
    languageOptions: { globals: globals.node },
  },
  {
    files: ["src/scripts/dataset.js"],
    ...js.configs.recommended,
    languageOptions: { globals: globals.node },
  },
  {
    files: ["public/app.js"],
    ...js.configs.recommended,
    languageOptions: { globals: globals.browser, sourceType: "module" },
  },
];
