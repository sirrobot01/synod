// Browser routing over the history API.
//
// An operation ID must survive a reload, so a route lives in
// the path and never in memory alone. The controller returns the application for every
// path outside /v1, so a deep link and a reload both reach the same screen.

export type ScreenName =
  | "fleet"
  | "apps"
  | "destinations"
  | "snapshots"
  | "nodes"
  | "operations"
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
  if (first === undefined) return { screen: "fleet", id: "" };
  switch (first) {
    case "apps": case "snapshots":
      return { screen: first, id: second ?? "" };
    case "destinations":
      return { screen: second ? "unknown" : "destinations", id: "" };
    case "devices":
    case "nodes":
      return { screen: "nodes", id: second ?? "" };
    case "activity":
    case "operations":
      if (second === undefined) return { screen: "operations", id: "" };
      return { screen: "operation", id: second };
    case "settings":
      return { screen: second ? "unknown" : "settings", id: "" };
    default:
      return { screen: "unknown", id: "" };
  }
}

/** path builds the address of one screen, so no screen writes a literal path. */
export function path(screen: ScreenName, id = ""): string {
  const encoded = encodeURIComponent(id);
  switch (screen) {
    case "fleet":
      return "/";
    case "apps": case "snapshots":
      return id === "" ? `/${screen}` : `/${screen}/${encoded}`;
    case "destinations": return "/destinations";
    case "nodes":
      return id === "" ? "/nodes" : `/nodes/${encoded}`;
    case "operations":
      return "/operations";
    case "operation":
      return `/operations/${encoded}`;
    case "settings":
      return "/settings";
    default:
      return "/";
  }
}

/** Router holds the current route and keeps it in the address bar. */
export class Router {
  current = $state<Route>({ screen: "fleet", id: "" });

  constructor() {
    this.current = match(window.location.pathname);
    window.addEventListener("popstate", this.onPopState);
  }

  private onPopState = () => { this.current = match(window.location.pathname); };

  destroy() { window.removeEventListener("popstate", this.onPopState); }

  /** go moves to one screen and records the move, so the back button works. */
  go(screen: ScreenName, id = "") {
    const target = path(screen, id);
    if (target !== window.location.pathname) window.history.pushState({}, "", target);
    this.current = match(target);
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
