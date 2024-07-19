/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      backgroundColor: {
        "app": "var(--app-bg)",
        "row-primary": "var(--app-bg)",
        "row-secondary": "#242424",
        "app-dark": "var(--app-bg-dark)",
        "row-primary-dark": "var(--app-bg)",
        "row-secondary-dark": "var(--row-bg-secondary-dark)",
      },
      textColor: {
        primary: "#181818",
        "primary-dark": "#FBFBFB",
        secondary: "rgba(0, 0, 0, 0.5)",
        "secondary-dark": "#DEDEDE",
        logo: "#242424",
        "logo-dark": "#F6F6F6",
      }
    },
  },
  plugins: [],
};

