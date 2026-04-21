import type { Config } from 'tailwindcss';

const config: Config = {
  content: ['./src/**/*.{js,ts,jsx,tsx,mdx}'],
  theme: {
    extend: {
      colors: {
        powerbi: {
          yellow: '#F2C811',
          dark: '#1E1E2E',
          card: '#2A2A3E',
          border: '#3A3A5E',
        },
      },
    },
  },
  plugins: [],
};
export default config;
