# Security posture

Soele launches real agent processes on your machine and gives them a web
remote control. Treat it accordingly.

## The blunt facts

- The daemon has no login of its own. Anyone who can reach :8800 can open
  sessions as the service user. Keep it LAN-only, or behind TLS + auth, or on
  a VPN. Never port-forward it raw.
- Sessions are real processes with the service user's whole reach. Claude
  Code's permission prompts and trust dialog are UX guardrails, not a jail: a
  shell command can wander anywhere the user can.
- Anything the model can read, a prompt injection can try to exfiltrate. Scope
  what sessions can see to what you would paste into a chat.

## Scoping tiers, weakest to strongest

1. **Scoped user.** Run the daemon as a user that owns only your projects
   directory. Cheap, coarse, better than root.
2. **Golden folder.** Keep a template directory (CLAUDE.md, settings, skills)
   and stamp a copy per project; new chats get that copy as cwd. Turn on
   Claude Code's sandbox mode, which enforces filesystem and network scope at
   the OS level (bubblewrap/Seatbelt). This is the recommended default and
   needs no virtualization.
3. **Worker containers.** Proxmox LXC clones from a golden template, one per
   project, sessions inside via `pct exec`. Real isolation; a wrecked
   container is a delete-and-reclone. See the companion [golden-container](https://github.com/Floral-Overcast/golden-container)
   project.

## Credentials

- The OAuth token lives in `/etc/hub.env` (chmod 600), never in the repo or
  the unit file.
- Watch OAuth grant limits: Anthropic caps grants per account and new logins
  evict the oldest. Prefer one `claude setup-token` shared into containers
  over logging in per box.

## Reporting

Security issues: open a GitHub issue with the `security` label (or use
GitHub's private reporting on this repo).
