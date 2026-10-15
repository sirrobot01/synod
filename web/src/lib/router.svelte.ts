// Browser routing over the history API.
//
// An operation ID must survive a reload, so a route lives in
// the path and never in memory alone. The controller returns the application for every
// path outside /v1, so a deep link and a reload both reach the same screen.

export type ScreenName =
  | "overview"
  | "apps"
  | "devices"
  | "storage"
  | "activity"
  | "operation"
  | "settings"
  | "unknown";

export interface Route {
  screen: ScreenName;
  /** The resource that the path names, when it names one. */
  id: string;
}

/** match reads one path into a screen and its resource. It never reads the network. */
export function match(pathname: string): Route {
  let parts: string[];
  try { parts = pathname.split("/").filter(Boolean).map(decodeURIComponent); }
  catch { return { screen: "unknown", id: "" }; }
  if (parts.length > 2) return { screen: "unknown", id: "" };
  const [first, second] = parts;
  if (first === undefined) return { screen: "overview", id: "" };
  switch (first) {
    // Earlier builds used /snapshots/{app} for the copies of an app. The app page holds them now.
    case "apps": case "snapshots":
      return { screen: "apps", id: second ?? "" };
    case "devices": case "nodes":
      return { screen: "devices", id: second ?? "" };
    case "storage": case "destinations":
      return { screen: second ? "unknown" : "storage", id: "" };
    case "activity": case "operations":
      if (second === undefined) return { screen: "activity", id: "" };
      return { screen: "operation", id: second };
    case "settings":
      if (second === undefined) return { screen: "settings", id: "" };
      return settingsTabs.includes(second) ? { screen: "settings", id: second } : { screen: "unknown", id: "" };
    default:
      return { screen: "unknown", id: "" };
  }
}

/** The tabs of Settings. The first one opens at /settings. */
export const settingsTabs = ["general", "notifications", "accounts", "api-tokens", "recovery-kit"];

/** path builds the address of one screen, so no screen writes a literal path. A query narrows the screen. */
export function path(screen: ScreenName, id = "", query = ""): string {
  return address(screen, id) + (query ? `?${query}` : "");
}

function address(screen: ScreenName, id: string): string {
  const encoded = encodeURIComponent(id);
  switch (screen) {
    case "overview":
      return "/";
    case "apps":
      return id === "" ? "/apps" : `/apps/${encoded}`;
    case "devices":
      return id === "" ? "/devices" : `/devices/${encoded}`;
    case "storage":
      return "/storage";
    case "activity":
      return "/activity";
    case "operation":
      return `/activity/${encoded}`;
    case "settings":
      return id === "" || id === "general" ? "/settings" : `/settings/${encoded}`;
    default:
      return "/";
  }
}

// The shell owns one router. A link anywhere moves through it.
let active: Router | null = null;

/** go opens one screen through the shell's router, or loads the address without one. */
export function go(screen: ScreenName, id = "", query = "") {
  if (active) active.go(screen, id, query);
  else window.location.assign(path(screen, id, query));
}

/** swap changes the address in place, without a history entry. */
export function swap(screen: ScreenName, id = "") {
  if (active) active.replace(screen, id);
  else window.history.replaceState({}, "", path(screen, id));
}

/** Router holds the current route and keeps it in the address bar. */
export class Router {
  current = $state<Route>({ screen: "overview", id: "" });

  constructor() {
    this.current = match(window.location.pathname);
    window.addEventListener("popstate", this.onPopState);
    active = this;
  }

  private onPopState = () => { this.current = match(window.location.pathname); };

  destroy() {
    window.removeEventListener("popstate", this.onPopState);
    if (active === this) active = null;
  }

  /** go moves to one screen and records the move, so the back button works. */
  go(screen: ScreenName, id = "", query = "") {
    const target = path(screen, id, query);
    if (target !== window.location.pathname + window.location.search) window.history.pushState({}, "", target);
    this.current = match(window.location.pathname);
  }

  /** replace changes the address without a new history entry, as a tab change does. */
  replace(screen: ScreenName, id = "") {
    window.history.replaceState({}, "", path(screen, id));
    this.current = match(window.location.pathname);
  }
}

/**
 * follow routes one link click in the browser, and leaves the rest to the browser.
 *
 * A modified click or a middle click means open elsewhere, and a click that
 * something already handled is not ours, so only a plain primary click is taken.
 * Every link on every screen goes through this, so no screen can quietly break
 * open in a new tab by calling preventDefault on its own.
 */
export function follow(event: MouseEvent, go: () => void) {
  if (event.defaultPrevented || event.button !== 0) return;
  if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
  event.preventDefault();
  go();
}
