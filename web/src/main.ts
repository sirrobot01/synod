import "./app.css";
import { mount } from "svelte";
import App from "./App.svelte";

const target = document.getElementById("app");
if (!target) throw new Error("the application root is missing");

export default mount(App, { target });
