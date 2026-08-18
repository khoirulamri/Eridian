import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor, fireEvent } from "@testing-library/react";
import type { ClaudeDirInfo, Settings } from "../lib/types";

// First test in the suite to stub the Tauri invoke layer: SettingsPanel is the
// one panel that both reads and WRITES through it, and the watched-directory
// list has to persist immediately, so a plain props render can't cover it.
vi.mock("../lib/api", () => ({
  api: {
    dbInfo: vi.fn(),
    getSettings: vi.fn(),
    setSettings: vi.fn(),
    claudeDirsStatus: vi.fn(),
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
  maxSessionsPerAgent: 1000,
  catalogFetchEnabled: false,
  claudeDirs: [],
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
