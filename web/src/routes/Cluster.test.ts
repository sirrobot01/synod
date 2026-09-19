import { cleanup, render, screen } from "@testing-library/svelte";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import Cluster from "./Cluster.svelte";

vi.mock("../lib/api", async () => {
  const real = await vi.importActual<typeof import("../lib/api")>("../lib/api");
  return {
    ...real,
    api: {
      cluster: () =>
        Promise.resolve({
          id: "cl_31d0aa47",
          spec: { name: "pg-core", engine: "postgres" },
          desired_revision: 14,
          desired_lifecycle: "running",
          observed_revision: 14,
          observed_lifecycle: "running",
          updated_at: new Date().toISOString(),
        }),
    },
  };
});

function open() {
  render(Cluster, { id: "cl_31d0aa47", onOpenTopology: () => {} });
}

async function settle() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

/** press clicks a control by name and lets the screen redraw. */
async function press(name: RegExp | string) {
  (screen.getByRole("button", { name }) as HTMLButtonElement).click();
  await settle();
}

/** confirm types the cluster name, which is what arms the destructive control. */
async function confirm(name: string) {
  const field = screen.getByLabelText(/Type the cluster name/) as HTMLInputElement;
  field.value = name;
  field.dispatchEvent(new Event("input", { bubbles: true }));
  await settle();
}

beforeEach(() => {
  cleanup();
});

afterEach(cleanup);

describe("a destructive plan", () => {
  it("states the exact target, the revision, the downtime, and the data disposition", async () => {
    open();
    await settle();
    await press("Delete…");
    // Specification 17 requires all four before a destructive action is offered.
    expect(screen.getByText("clusters.delete.v1")).toBeTruthy();
    expect(screen.getByText("pg-core · cl_31d0aa47")).toBeTruthy();
    expect(screen.getByText("14")).toBeTruthy();
    expect(screen.getByText("permanent — cluster stops serving")).toBeTruthy();
    expect(screen.getByText(/volumes RETAINED as retained storage/)).toBeTruthy();
  });

  it("binds the confirmation to a plan token, and refuses until the name is typed", async () => {
    open();
    await settle();
    await press("Delete…");
    const button = () => screen.getByRole("button", { name: /Delete pg-core/ }) as HTMLButtonElement;
    // An unbound confirmation could approve a plan the operator never read.
    expect(button().disabled).toBe(true);
    expect(screen.getByText("plt_a41c")).toBeTruthy();
    await confirm("wrong-name");
    expect(button().disabled).toBe(true);
    await confirm("pg-core");
    expect(button().disabled).toBe(false);
  });

  it("refreshes the plan on a revision conflict instead of approving the old one", async () => {
    open();
    await settle();
    await press("Delete…");
    await confirm("pg-core");
    await press(/Delete pg-core/);

    // The server moved the cluster on. The previous token is rejected, not
    // silently accepted, and the refreshed plan states what changed.
    expect(screen.getByText(/revision_conflict · HTTP 412/)).toBeTruthy();
    expect(screen.getByText("15 (was 14 — refreshed)")).toBeTruthy();
    expect(screen.getByText("2 instances (was 3)")).toBeTruthy();
    expect(screen.getByText(/changed: members 3 → 2/)).toBeTruthy();
    // A new token, so the rejected one can never be replayed.
    expect(screen.getByText("plt_b9f1 (refreshed)")).toBeTruthy();
    expect(screen.queryByText("plt_a41c")).toBe(null);
  });

  it("names a point of no return on an upgrade, and promises no rollback past it", async () => {
    open();
    await settle();
    await press("Upgrade…");
    expect(screen.getByText(/point of no return/)).toBeTruthy();
    expect(screen.getByText("No rollback after step 3")).toBeTruthy();
    expect(screen.getByText(/cannot be returned to 15\.8/)).toBeTruthy();
  });

  it("closes without acting, so a dialog is never a commitment", async () => {
    open();
    await settle();
    await press("Delete…");
    await press("Cancel");
    expect(screen.queryByRole("dialog")).toBe(null);
  });
});

describe("the cluster screen", () => {
  it("draws desired and observed apart, each with its own revision", async () => {
    open();
    await settle();
    // A single collapsed badge would have to pick a winner between the two.
    expect(screen.getByText("desired")).toBeTruthy();
    expect(screen.getByText("observed")).toBeTruthy();
    expect(screen.getByText("converged")).toBeTruthy();
  });

  it("says the verdict measures a write time, because the API serves no observation time", async () => {
    open();
    await settle();
    expect(screen.getByText(/no observation time or reason on a cluster/)).toBeTruthy();
  });
});
