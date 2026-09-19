import { cleanup, render, screen, within } from "@testing-library/svelte";
import { afterEach, describe, expect, it } from "vitest";
import Backups from "./Backups.svelte";

// The screen reads no API. What is under test is that a backup's existence and
// its verification are reported as two separate facts, and that the restore
// control never claims an ability this build does not have.

afterEach(cleanup);

describe("backups", () => {
  it("reports verification apart from the artifact", () => {
    const { container } = render(Backups);
    const table = container.querySelector(".grid.backups");
    if (!table) throw new Error("no artifact table");
    // A backup that exists is not a backup that restores, so the two are separate
    // columns. Two artifacts are unverified, and both say so in the row.
    expect(within(table as HTMLElement).getByText("restore-verified 6h ago")).toBeTruthy();
    expect(within(table as HTMLElement).getAllByText("never verified")).toHaveLength(2);
    // Unverified says so, rather than showing a neutral dash that reads as fine.
    expect(within(table as HTMLElement).queryByText("—")).toBe(null);
  });

  it("reads an unobservable artifact as unknown, not as lost", () => {
    render(Backups);
    expect(screen.getByText("unknown — target unreachable")).toBeTruthy();
    expect(screen.getByText("? last 44 GiB")).toBeTruthy();
  });

  it("keeps BackupCurrent unknown rather than true when nobody can confirm it", () => {
    render(Backups);
    // The last backup succeeded. Whether it is still there cannot be observed.
    expect(screen.getByText("target_unreachable", { exact: false })).toBeTruthy();
    expect(screen.getByText(/is unknown, not true/)).toBeTruthy();
  });

  it("draws the recovery range with its gap, because recovery is refused inside one", () => {
    const { container } = render(Backups);
    expect(container.querySelectorAll(".span.gap")).toHaveLength(1);
    expect(screen.getByText(/WAL missing Sep 09/)).toBeTruthy();
    expect(screen.getByText(/not offered inside a gap/)).toBeTruthy();
  });

  it("offers no restore this build cannot perform", () => {
    render(Backups);
    // Specification 17 requires restore. The API serves no backup surface, so the
    // control is present and refused rather than absent or pretending to work.
    const restore = screen.getByRole("button", { name: /restore/i }) as HTMLButtonElement;
    expect(restore.disabled).toBe(true);
    expect(screen.getByText(/no backup surface/)).toBeTruthy();
  });
});
