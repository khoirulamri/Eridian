import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "@testing-library/react";
import type { AccountUsage, ClaudeDirInfo, Settings } from "../lib/types";

// First test in the suite to stub the Tauri invoke layer: SettingsPanel is the
// one panel that both reads and WRITES through it, and the watched-directory
// list has to persist immediately, so a plain props render can't cover it.
vi.mock("../lib/api", () => ({
  api: {
    dbInfo: vi.fn(),
    getSettings: vi.fn(),
    setSettings: vi.fn(),
    claudeDirsStatus: vi.fn(),
    archiveUsage: vi.fn(),
    inspectClaudeDir: vi.fn(),
    rebuildDb: vi.fn(),
    marketRefresh: vi.fn(),
  },
  onIngestProgress: vi.fn(() => Promise.resolve(() => {})),
}));

import { api } from "../lib/api";
import { SettingsPanel } from "./SettingsPanel";

const mocked = api as unknown as Record<string, ReturnType<typeof vi.fn>>;

const dir = (o: Partial<ClaudeDirInfo>): ClaudeDirInfo => ({
  path: "~/.claude",
  label: null,
  projectCount: 0,
  exists: true,
  kind: "default",
  ...o,
});

const settings = (o: Partial<Settings> = {}): Settings => ({
  backfillFileLimit: 2000,
  maxSessionsPerAccount: 300,
  catalogFetchEnabled: false,
  claudeDirs: [],
  maxArchiveMb: null,
  ...o,
});

beforeEach(() => {
  vi.clearAllMocks();
  mocked.dbInfo.mockResolvedValue({
    path: "/app/eridian.db",
    sizeBytes: 1024,
    sessions: 3,
    events: 40,
  });
  mocked.getSettings.mockResolvedValue(settings());
  mocked.claudeDirsStatus.mockResolvedValue([dir({ projectCount: 9 })]);
  mocked.archiveUsage.mockResolvedValue([]);
});

const usage = (o: Partial<AccountUsage>): AccountUsage => ({
  agent: "claude-code",
  account: null,
  sessions: 1,
  events: 10,
  bytes: 1024,
  ...o,
});

describe("SettingsPanel — retention", () => {
  it("shows the per-account archive breakdown, biggest first", async () => {
    mocked.archiveUsage.mockResolvedValue([
      usage({ account: "work", sessions: 182, bytes: 150 * 1024 * 1024 }),
      usage({ account: null, sessions: 2, bytes: 1024 * 1024 }),
    ]);
    const { container } = render(<SettingsPanel />);
    await waitFor(() =>
      expect(container.querySelectorAll(".usage-row").length).toBe(2)
    );
    const rows = container.querySelectorAll(".usage-row");
    expect(rows[0].textContent).toContain("work");
    expect(rows[0].textContent).toContain("182 sessions");
    // The default root has no label, so it renders as a plain "default" tag.
    expect(rows[1].textContent).toContain("default");
    // Bar width is the share of the largest account.
    expect(
      (rows[0].querySelector(".usage-bar") as HTMLElement).style.getPropertyValue("--w")
    ).toBe("100%");
  });

  it("round-trips the size budget between GB in the UI and MB on the wire", async () => {
    mocked.getSettings.mockResolvedValue(settings({ maxArchiveMb: 2048 }));
    mocked.setSettings.mockResolvedValue(settings({ maxArchiveMb: 512 }));
    const { container } = render(<SettingsPanel />);

    // Selected by placeholder rather than position — stable if fields move.
    const gb = () =>
      container.querySelector<HTMLInputElement>('input[placeholder="no limit"]')!;
    await waitFor(() => expect(gb().value).toBe("2"));

    // Decimals must survive: 0.5 GB → 512 MB, not 0.
    fireEvent.change(gb(), { target: { value: "0.5" } });
    fireEvent.click(screen.getByRole("button", { name: "Save settings" }));
    await waitFor(() => expect(mocked.setSettings).toHaveBeenCalled());
    expect(mocked.setSettings.mock.calls[0][0].maxArchiveMb).toBe(512);
  });

  it("sends null, not 0, when the budget is cleared", async () => {
    mocked.getSettings.mockResolvedValue(settings({ maxArchiveMb: 2048 }));
    mocked.setSettings.mockResolvedValue(settings());
    const { container } = render(<SettingsPanel />);
    // Selected by placeholder rather than position — stable if fields move.
    const gb = () =>
      container.querySelector<HTMLInputElement>('input[placeholder="no limit"]')!;
    await waitFor(() => expect(gb().value).toBe("2"));

    fireEvent.change(gb(), { target: { value: "" } });
    fireEvent.click(screen.getByRole("button", { name: "Save settings" }));
    await waitFor(() => expect(mocked.setSettings).toHaveBeenCalled());
    expect(mocked.setSettings.mock.calls[0][0].maxArchiveMb).toBeNull();
  });

  it("carries retention settings through a directory save", async () => {
    // Regression: the panel builds a whole Settings object in three places, so
    // adding a directory must not wipe the retention fields.
    mocked.getSettings.mockResolvedValue(
      settings({ maxArchiveMb: 2048, maxSessionsPerAccount: 300 })
    );
    mocked.claudeDirsStatus.mockResolvedValue([
      dir({ projectCount: 9 }),
      dir({ path: "~/.claude-work", label: "work", projectCount: 5, kind: "detected" }),
    ]);
    mocked.inspectClaudeDir.mockResolvedValue(dir({ path: "~/.claude-work" }));
    mocked.setSettings.mockResolvedValue(settings({ claudeDirs: ["~/.claude-work"] }));

    const { container } = render(<SettingsPanel />);
    await waitFor(() =>
      expect(container.querySelector(".dir-detected")).not.toBeNull()
    );
    fireEvent.click(screen.getByRole("button", { name: "Add" }));
    await waitFor(() => expect(mocked.setSettings).toHaveBeenCalled());

    const sent = mocked.setSettings.mock.calls[0][0];
    expect(sent.maxArchiveMb).toBe(2048);
    expect(sent.maxSessionsPerAccount).toBe(300);
    expect(sent.claudeDirs).toEqual(["~/.claude-work"]);
  });
});

describe("SettingsPanel — watched directories", () => {
  it("lists the default root as always-on and renders real punctuation", async () => {
    const { container } = render(<SettingsPanel />);
    await waitFor(() =>
      expect(container.querySelectorAll(".dir-row").length).toBe(1)
    );
    const row = container.querySelector(".dir-row")!;
    expect(row.textContent).toContain("~/.claude");
    expect(row.textContent).toContain("9 projects");
    // The default root can't be removed.
    expect(row.querySelector(".dir-remove")).toBeNull();
    expect(row.textContent).toContain("always on");
    // Guard against escape sequences leaking into JSX text as literal "—".
    expect(container.textContent).not.toMatch(/\\u[0-9a-f]{4}/i);
  });

  it("offers a detected sibling directory and persists it on Add", async () => {
    mocked.claudeDirsStatus.mockResolvedValue([
      dir({ projectCount: 9 }),
      dir({ path: "~/.claude-work", label: "work", projectCount: 5, kind: "detected" }),
    ]);
    mocked.inspectClaudeDir.mockResolvedValue(
      dir({ path: "~/.claude-work", label: "work", kind: "detected" })
    );
    mocked.setSettings.mockResolvedValue(settings({ claudeDirs: ["~/.claude-work"] }));

    const { container } = render(<SettingsPanel />);
    await waitFor(() =>
      expect(container.querySelector(".dir-detected")).not.toBeNull()
    );
    fireEvent.click(screen.getByRole("button", { name: "Add" }));

    await waitFor(() => expect(mocked.setSettings).toHaveBeenCalled());
    // The whole settings object round-trips — adding a directory must not wipe
    // the other fields (they share one settings.json).
    expect(mocked.setSettings.mock.calls[0][0]).toEqual(
      settings({ claudeDirs: ["~/.claude-work"] })
    );
  });

  it("won't write settings before the existing ones have loaded", async () => {
    // Persisting writes the whole settings.json; firing early would save a blank
    // backfill limit over the user's configured value.
    let resolveSettings!: (s: Settings) => void;
    mocked.getSettings.mockReturnValue(
      new Promise<Settings>((r) => {
        resolveSettings = r;
      })
    );
    mocked.claudeDirsStatus.mockResolvedValue([
      dir({ projectCount: 9 }),
      dir({ path: "~/.claude-work", label: "work", projectCount: 5, kind: "detected" }),
    ]);

    const { container } = render(<SettingsPanel />);
    await waitFor(() =>
      expect(container.querySelector(".dir-detected")).not.toBeNull()
    );
    const add = screen.getByRole("button", { name: "Add" }) as HTMLButtonElement;
    expect(add.disabled).toBe(true);

    resolveSettings(settings({ backfillFileLimit: 42 }));
    await waitFor(() => expect(add.disabled).toBe(false));
  });

  it("shows an inline error and saves nothing when the path is rejected", async () => {
    mocked.inspectClaudeDir.mockRejectedValue("~/.nope has no projects/ folder.");
    const { container } = render(<SettingsPanel />);
    await waitFor(() => expect(mocked.getSettings).toHaveBeenCalled());

    fireEvent.change(container.querySelector(".dir-add input")!, {
      target: { value: "~/.nope" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Add directory" }));

    await waitFor(() =>
      expect(container.querySelector(".dir-error")?.textContent).toContain(
        "no projects/ folder"
      )
    );
    expect(mocked.setSettings).not.toHaveBeenCalled();
  });

  it("confirms before it stops watching a configured directory", async () => {
    mocked.getSettings.mockResolvedValue(settings({ claudeDirs: ["~/.claude-work"] }));
    mocked.claudeDirsStatus.mockResolvedValue([
      dir({ projectCount: 9 }),
      dir({ path: "~/.claude-work", label: "work", projectCount: 5, kind: "configured" }),
    ]);
    mocked.setSettings.mockResolvedValue(settings({ claudeDirs: [] }));

    const { container } = render(<SettingsPanel />);
    await waitFor(() =>
      expect(container.querySelectorAll(".dir-row").length).toBe(2)
    );
    fireEvent.click(container.querySelector(".dir-remove")!);

    // Nothing is persisted until the modal is confirmed.
    expect(mocked.setSettings).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole("button", { name: "Stop watching" }));

    await waitFor(() => expect(mocked.setSettings).toHaveBeenCalled());
    expect(mocked.setSettings.mock.calls[0][0].claudeDirs).toEqual([]);
  });
});
