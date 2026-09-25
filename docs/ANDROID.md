# Building the Android app

There is no prebuilt APK, on purpose: the app is a Trusted Web Activity (a
fullscreen wrapper around the PWA), and a TWA is bound to one origin and one
signing key at build time. Mine points at my house; yours has to point at
yours. The build is about 15 minutes the first time.

If you don't need a launcher icon without browser chrome, skip all of this:
the PWA installs from the browser menu ("Add to home screen") on any phone,
and that's the whole iPhone story anyway.

## Prerequisites

- Your Soele reachable at an **HTTPS origin with a certificate Chrome
  trusts** (self-signed won't verify). Two workable shapes:
  - a real domain with a Let's Encrypt cert on a reverse proxy in front of
    :8800, reachable only on your LAN/VPN
  - the split-horizon trick: a public DNS A record for your domain pointing
    at a private VPN IP (e.g. a Tailscale address). Public name, private
    address; the phone resolves publicly and connects over the VPN. Useful
    because Chrome on Android ignores VPN-only DNS names.
- Node 18+, JDK **17** (Bubblewrap rejects newer JDKs), Android SDK
  cmdline-tools. Bubblewrap can download the SDK for you on first run.
- The phone needs Chrome installed (TWAs render through it).

## Build

```sh
npm i -g @bubblewrap/cli
mkdir soele-twa && cd soele-twa
bubblewrap init --manifest https://your.origin/manifest.json
```

Answer the prompts: application id (pick a package name you'll keep, e.g.
`com.example.soele`), launcher name, icons (it reads them from the manifest).
Let it generate a keystore.

**Back the keystore up now.** The signing key is forever: lose it and you can
never update the installed app, only uninstall and reinstall.

```sh
bubblewrap build
```

Non-interactive builds (CI, scripts): `bubblewrap update --skipVersionUpgrade
&& bubblewrap build`, both with stdin closed (`< /dev/null`) and
`BUBBLEWRAP_KEYSTORE_PASSWORD` / `BUBBLEWRAP_KEY_PASSWORD` in the
environment. The interactive versionName prompt hangs scripted builds
otherwise.

Output: `app-release-signed.apk`. Sideload it (the phone must be able to
reach your origin at install time).

## Tell Soele about your signature

Android verifies the app owns the origin by fetching
`/.well-known/assetlinks.json` at first launch. The daemon serves it when
these are set in `/etc/hub.env`:

```
HUB_TWA_PACKAGE=com.example.soele
HUB_TWA_FINGERPRINT=AA:BB:...   # apksigner verify --print-certs app-release-signed.apk
```

Restart the service, launch the app: no URL bar, no browser chrome. If you
still see a URL bar, the fingerprint doesn't match the APK's signer, the
envs aren't set, or the origin isn't the one baked into the APK.

## Updating

Edit `twa-manifest.json`, bump `appVersionCode`, rebuild with the same
keystore, sideload over the old install. A changed launcher name or icon is
also a rebuild; the wrapper only reads the web manifest at build time.
